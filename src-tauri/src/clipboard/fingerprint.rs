//! 系统剪贴板的跨设备稳定语义指纹。
//!
//! 数据库 `content_hash` 负责历史去重；这里的指纹只用于判断同步内容是否已经是当前
//! 系统剪贴板内容，从而避免远程桌面与 EcoPaste 双向同步形成反馈循环。

use std::{
    collections::VecDeque,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use image::{imageops::FilterType, ImageReader, Rgba, RgbaImage};

use crate::db::models::{ClipboardItem, ClipboardKind, ClipboardSubKind};

const TEXT_DOMAIN: &[u8] = b"ecopaste-clipboard-text-v1\0";
const IMAGE_DOMAIN: &[u8] = b"ecopaste-clipboard-image-v1\0";
const FILES_DOMAIN: &[u8] = b"ecopaste-clipboard-files-v1\0";
const PERCEPTUAL_HASH_COLUMNS: u32 = 16;
const PERCEPTUAL_HASH_ROWS: u32 = 16;
const PERCEPTUAL_SAMPLE_COUNT: usize =
    ((PERCEPTUAL_HASH_COLUMNS + 1) * PERCEPTUAL_HASH_ROWS) as usize;
const PERCEPTUAL_HASH_DISTANCE_LIMIT: u32 = 32;
const PERCEPTUAL_LUMA_MAE_LIMIT: u32 = 2;
const RECENT_SYNCED_IMAGE_LIMIT: usize = 8;
const RECENT_SYNCED_IMAGE_TTL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClipboardFingerprint {
    exact: String,
    perceptual_image: Option<PerceptualImageFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PerceptualImageFingerprint {
    width: u32,
    height: u32,
    difference_hash: [u64; 4],
    reduced_luma: [u8; PERCEPTUAL_SAMPLE_COUNT],
}

impl ClipboardFingerprint {
    /// 文本以纯文本表示为跨应用语义；HTML/RTF 被远程工具降级后仍视为同一内容。
    pub fn from_text_item(item: &ClipboardItem) -> Option<Self> {
        if item.kind != ClipboardKind::Text {
            return None;
        }
        let text = if matches!(
            item.sub_kind,
            Some(ClipboardSubKind::Html | ClipboardSubKind::Rtf)
        ) {
            item.search_text.as_deref().unwrap_or(&item.content)
        } else {
            &item.content
        };

        Some(Self::exact(hash_parts(TEXT_DOMAIN, [text.as_bytes()])))
    }

    /// 图片使用 RGBA 语义指纹；最多缓存 64 个字节摘要到指纹的映射，不保留原图或像素。
    /// 编码字节变化时重新解码，PNG/TIFF/DIB 重新编码仍按像素判等。
    pub fn from_image_bytes(bytes: &[u8]) -> Option<Self> {
        static CACHE: LazyLock<Mutex<VecDeque<(blake3::Hash, ClipboardFingerprint)>>> =
            LazyLock::new(|| Mutex::new(VecDeque::new()));
        let key = blake3::hash(bytes);
        {
            let mut cache = CACHE.lock().expect("image fingerprint cache poisoned");
            if let Some(index) = cache.iter().position(|(digest, _)| *digest == key) {
                let entry = cache.remove(index)?;
                let fingerprint = entry.1.clone();
                cache.push_back(entry);
                return Some(fingerprint);
            }
        }
        let image = ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .ok()?
            .decode()
            .ok()?
            .to_rgba8();
        let width = image.width().to_le_bytes();
        let height = image.height().to_le_bytes();

        let fingerprint = Self {
            exact: hash_parts(
                IMAGE_DOMAIN,
                [width.as_slice(), height.as_slice(), image.as_raw()],
            ),
            perceptual_image: Some(PerceptualImageFingerprint::from_rgba(&image)),
        };
        let mut cache = CACHE.lock().expect("image fingerprint cache poisoned");
        if !cache.iter().any(|(digest, _)| *digest == key) {
            if cache.len() == 64 {
                cache.pop_front();
            }
            cache.push_back((key, fingerprint.clone()));
        }
        Some(fingerprint)
    }

    /// 文件卡片忽略绝对路径和选择顺序，保留逻辑名称、类型及内容指纹。
    pub fn from_file_entries(entries: &[FileEntryFingerprint]) -> Option<Self> {
        if entries.is_empty() {
            return None;
        }
        let mut entry_hashes = entries
            .iter()
            .map(FileEntryFingerprint::digest)
            .collect::<Vec<_>>();
        entry_hashes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let mut hasher = blake3::Hasher::new();
        hasher.update(FILES_DOMAIN);
        for digest in entry_hashes {
            hasher.update(digest.as_bytes());
        }

        Some(Self::exact(hasher.finalize().to_hex().to_string()))
    }

    /// 写回保护继续使用与监听端一致的字符串形式。
    pub fn as_str(&self) -> &str {
        &self.exact
    }

    fn exact(value: String) -> Self {
        Self {
            exact: value,
            perceptual_image: None,
        }
    }
}

impl PerceptualImageFingerprint {
    /// 将完整 RGBA 图缩成 17×16，再比较相邻灰度像素形成 256 位差异哈希。
    fn from_rgba(image: &RgbaImage) -> Self {
        let reduced = image::imageops::resize(
            image,
            PERCEPTUAL_HASH_COLUMNS + 1,
            PERCEPTUAL_HASH_ROWS,
            FilterType::Triangle,
        );
        let mut difference_hash = [0_u64; 4];
        let mut reduced_luma = [0_u8; PERCEPTUAL_SAMPLE_COUNT];
        for (index, pixel) in reduced.pixels().enumerate() {
            reduced_luma[index] = composited_luma(pixel);
        }
        for row in 0..PERCEPTUAL_HASH_ROWS {
            for column in 0..PERCEPTUAL_HASH_COLUMNS {
                let sample_index = (row * (PERCEPTUAL_HASH_COLUMNS + 1) + column) as usize;
                let left = reduced_luma[sample_index];
                let right = reduced_luma[sample_index + 1];
                let index = (row * PERCEPTUAL_HASH_COLUMNS + column) as usize;
                if left > right {
                    difference_hash[index / u64::BITS as usize] |=
                        1_u64 << (index % u64::BITS as usize);
                }
            }
        }

        Self {
            width: image.width(),
            height: image.height(),
            difference_hash,
            reduced_luma,
        }
    }

    fn is_similar_to(&self, other: &Self) -> bool {
        if self.width != other.width || self.height != other.height {
            return false;
        }
        let luma_distance = self
            .reduced_luma
            .iter()
            .zip(other.reduced_luma)
            .map(|(left, right)| u32::from(left.abs_diff(right)))
            .sum::<u32>();
        if luma_distance > PERCEPTUAL_LUMA_MAE_LIMIT * PERCEPTUAL_SAMPLE_COUNT as u32 {
            return false;
        }
        self.difference_hash
            .iter()
            .zip(other.difference_hash)
            .map(|(left, right)| (left ^ right).count_ones())
            .sum::<u32>()
            <= PERCEPTUAL_HASH_DISTANCE_LIMIT
    }
}

fn composited_luma(pixel: &Rgba<u8>) -> u8 {
    let [red, green, blue, alpha] = pixel.0;
    let alpha = u32::from(alpha);
    let inverse_alpha = u32::from(u8::MAX) - alpha;
    let composite =
        |channel: u8| (u32::from(channel) * alpha + u32::from(u8::MAX) * inverse_alpha + 127) / 255;
    let red = composite(red);
    let green = composite(green);
    let blue = composite(blue);

    ((299 * red + 587 * green + 114 * blue + 500) / 1_000) as u8
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntryFingerprint {
    pub name: String,
    pub is_directory: bool,
    pub content_hash: String,
}

impl FileEntryFingerprint {
    fn digest(&self) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"ecopaste-clipboard-file-entry-v1\0");
        hasher.update(&[u8::from(self.is_directory)]);
        update_length_prefixed(&mut hasher, self.name.as_bytes());
        update_length_prefixed(&mut hasher, self.content_hash.as_bytes());
        hasher.finalize()
    }
}

#[derive(Default)]
pub struct ClipboardFingerprintState {
    inner: Mutex<FingerprintState>,
}

#[derive(Default)]
struct FingerprintState {
    generation: u64,
    current: Option<ClipboardFingerprint>,
    write_pending: bool,
    recent_synced_images: VecDeque<RecentSyncedImage>,
}

struct RecentSyncedImage {
    fingerprint: ClipboardFingerprint,
    recorded_at: Instant,
    write_generation: Option<u64>,
}

#[derive(Clone)]
pub struct ClipboardObservation {
    generation: u64,
    previous: Option<ClipboardFingerprint>,
    restore_previous: bool,
    outbound_image: Option<ClipboardFingerprint>,
}

pub struct ClipboardWrite {
    generation: u64,
    previous: Option<ClipboardFingerprint>,
    previous_write_pending: bool,
}

impl ClipboardFingerprintState {
    pub fn new() -> Self {
        Self::default()
    }

    /// 系统报告一次剪贴板变化时先失效旧值；确认是自身写回后可恢复。
    pub fn begin_observation(&self) -> ClipboardObservation {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state.generation = state.generation.wrapping_add(1);
        let restore_previous = state.write_pending;
        state.write_pending = false;
        ClipboardObservation {
            generation: state.generation,
            previous: state.current.take(),
            restore_previous,
            outbound_image: None,
        }
    }

    /// 仅在观察期间没有更新的剪贴板事件时提交结果，防止慢速文件任务覆盖新内容。
    pub fn commit_observation(
        &self,
        observation: &ClipboardObservation,
        fingerprint: ClipboardFingerprint,
    ) -> bool {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        if state.generation != observation.generation {
            return false;
        }
        state.current = Some(fingerprint);
        state.write_pending = false;
        true
    }

    /// 仅恢复同步写回前预发布的目标；普通写回不能把变化前的旧指纹重新标成当前内容。
    pub fn restore_observation(&self, observation: ClipboardObservation) {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        if state.generation == observation.generation
            && state.current.is_none()
            && observation.restore_previous
        {
            state.current = observation.previous;
            state.write_pending = false;
        }
    }

    pub fn matches(&self, fingerprint: &ClipboardFingerprint) -> bool {
        let state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state.current.as_ref() == Some(fingerprint)
    }

    /// 只与最近真正进入同步队列或由同步自动写入的图片比较，避免影响普通历史去重。
    pub fn matches_recent_synced_image(&self, fingerprint: &ClipboardFingerprint) -> bool {
        if fingerprint.perceptual_image.is_none() {
            return false;
        }
        self.matches_recent_synced_image_at(fingerprint, Instant::now())
    }

    /// 同步事件成功落入本地事件队列后，短期记录其感知指纹以识别远程桌面反射。
    pub fn remember_synced_image(&self, fingerprint: &ClipboardFingerprint) {
        if fingerprint.perceptual_image.is_none() {
            return;
        }
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        remember_synced_image_at(&mut state, fingerprint.clone(), Instant::now(), None);
    }

    /// 写系统剪贴板前先发布目标指纹；写入失败时可按代次安全回滚。
    pub fn begin_write(&self, fingerprint: ClipboardFingerprint) -> ClipboardWrite {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        let recent_image = fingerprint
            .perceptual_image
            .as_ref()
            .map(|_| fingerprint.clone());
        let previous = state.current.replace(fingerprint);
        let previous_write_pending = state.write_pending;
        state.write_pending = true;
        if let Some(recent_image) = recent_image {
            remember_synced_image_at(&mut state, recent_image, Instant::now(), Some(generation));
        }
        ClipboardWrite {
            generation,
            previous,
            previous_write_pending,
        }
    }

    pub fn rollback_write(&self, write: ClipboardWrite) {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state
            .recent_synced_images
            .retain(|recent| recent.write_generation != Some(write.generation));
        if state.generation == write.generation {
            state.current = write.previous;
            state.write_pending = write.previous_write_pending;
        }
    }

    fn matches_recent_synced_image_at(
        &self,
        fingerprint: &ClipboardFingerprint,
        now: Instant,
    ) -> bool {
        let Some(perceptual) = fingerprint.perceptual_image.as_ref() else {
            return false;
        };
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        prune_recent_synced_images(&mut state, now);
        state.recent_synced_images.iter().any(|recent| {
            recent.fingerprint.exact != fingerprint.exact
                && recent
                    .fingerprint
                    .perceptual_image
                    .as_ref()
                    .is_some_and(|candidate| candidate.is_similar_to(perceptual))
        })
    }
}

impl ClipboardObservation {
    /// 把监听阶段已完成的图片指纹带到异步同步入队阶段，不重新读取或解码图片。
    pub fn with_outbound_image(mut self, fingerprint: ClipboardFingerprint) -> Self {
        self.outbound_image = Some(fingerprint);
        self
    }

    pub fn outbound_image(&self) -> Option<&ClipboardFingerprint> {
        self.outbound_image.as_ref()
    }
}

fn remember_synced_image_at(
    state: &mut FingerprintState,
    fingerprint: ClipboardFingerprint,
    now: Instant,
    write_generation: Option<u64>,
) {
    prune_recent_synced_images(state, now);
    if state.recent_synced_images.len() == RECENT_SYNCED_IMAGE_LIMIT {
        state.recent_synced_images.pop_front();
    }
    state.recent_synced_images.push_back(RecentSyncedImage {
        fingerprint,
        recorded_at: now,
        write_generation,
    });
}

fn prune_recent_synced_images(state: &mut FingerprintState, now: Instant) {
    state.recent_synced_images.retain(|recent| {
        now.checked_duration_since(recent.recorded_at)
            .is_some_and(|age| age <= RECENT_SYNCED_IMAGE_TTL)
    });
}

fn hash_parts<'a>(domain: &[u8], parts: impl IntoIterator<Item = &'a [u8]>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    for part in parts {
        update_length_prefixed(&mut hasher, part);
    }
    hasher.finalize().to_hex().to_string()
}

fn update_length_prefixed(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::Platform;
    use chrono::Utc;
    use image::codecs::png::{CompressionType, FilterType, PngEncoder};
    use image::{ExtendedColorType, ImageEncoder};

    #[test]
    fn image_fingerprint_ignores_png_encoding() {
        let pixels = [
            255, 0, 0, 255, 0, 255, 0, 128, 0, 0, 255, 255, 255, 255, 255, 0,
        ];
        let mut fast = Vec::new();
        PngEncoder::new_with_quality(&mut fast, CompressionType::Fast, FilterType::NoFilter)
            .write_image(&pixels, 2, 2, ExtendedColorType::Rgba8)
            .unwrap();
        let mut best = Vec::new();
        PngEncoder::new_with_quality(&mut best, CompressionType::Best, FilterType::Adaptive)
            .write_image(&pixels, 2, 2, ExtendedColorType::Rgba8)
            .unwrap();

        assert_ne!(fast, best);
        assert_eq!(
            ClipboardFingerprint::from_image_bytes(&fast),
            ClipboardFingerprint::from_image_bytes(&best)
        );
    }

    #[test]
    fn rich_and_plain_text_with_same_fallback_are_equivalent() {
        let rich = text_item(
            "<strong>Hello</strong>",
            Some(ClipboardSubKind::Html),
            Some("Hello"),
        );
        let plain = text_item("Hello", None, None);

        assert_eq!(
            ClipboardFingerprint::from_text_item(&rich),
            ClipboardFingerprint::from_text_item(&plain)
        );
    }

    #[test]
    fn file_card_ignores_absolute_paths_and_selection_order() {
        let first = FileEntryFingerprint {
            name: "a.txt".to_owned(),
            is_directory: false,
            content_hash: "hash-a".to_owned(),
        };
        let second = FileEntryFingerprint {
            name: "folder".to_owned(),
            is_directory: true,
            content_hash: "hash-b".to_owned(),
        };

        assert_eq!(
            ClipboardFingerprint::from_file_entries(&[first.clone(), second.clone()]),
            ClipboardFingerprint::from_file_entries(&[second, first])
        );
    }

    #[test]
    fn stale_observation_cannot_replace_newer_clipboard_state() {
        let state = ClipboardFingerprintState::new();
        let slow = state.begin_observation();
        let current = exact_fingerprint("current");
        let write = state.begin_write(current.clone());

        assert!(!state.commit_observation(&slow, exact_fingerprint("stale")));
        assert!(state.matches(&current));
        drop(write);
    }

    #[test]
    fn suppressed_writeback_restores_the_published_fingerprint() {
        let state = ClipboardFingerprintState::new();
        let expected = exact_fingerprint("expected");
        let _write = state.begin_write(expected.clone());
        let observation = state.begin_observation();

        state.restore_observation(observation);

        assert!(state.matches(&expected));
    }

    #[test]
    fn ordinary_writeback_does_not_restore_the_previous_fingerprint() {
        let state = ClipboardFingerprintState::new();
        let previous = exact_fingerprint("previous");
        let copied = state.begin_observation();
        assert!(state.commit_observation(&copied, previous.clone()));

        let ordinary_writeback = state.begin_observation();
        state.restore_observation(ordinary_writeback);

        assert!(!state.matches(&previous));
    }

    #[test]
    fn failed_write_rolls_back_if_no_newer_event_arrived() {
        let state = ClipboardFingerprintState::new();
        let previous = exact_fingerprint("previous");
        let _initial = state.begin_write(previous.clone());
        let write = state.begin_write(exact_fingerprint("failed"));

        state.rollback_write(write);

        assert!(state.matches(&previous));
    }

    #[test]
    fn reflected_content_converges_without_a_second_write() {
        let fingerprint = exact_fingerprint("shared");
        let first_device = ClipboardFingerprintState::new();
        let second_device = ClipboardFingerprintState::new();

        let copied = first_device.begin_observation();
        assert!(first_device.commit_observation(&copied, fingerprint.clone()));

        assert!(!second_device.matches(&fingerprint));
        let _remote_write = second_device.begin_write(fingerprint.clone());
        let local_callback = second_device.begin_observation();
        second_device.restore_observation(local_callback);

        let rustdesk_reflection = first_device.begin_observation();
        assert!(first_device.commit_observation(&rustdesk_reflection, fingerprint.clone()));

        assert!(second_device.matches(&fingerprint));
    }

    #[test]
    fn perceptual_image_fingerprint_tolerates_small_pixel_changes() {
        let original = sample_image(128, 64, 0);
        let converted = sample_image(128, 64, 1);
        let original = PerceptualImageFingerprint::from_rgba(&original);
        let converted = PerceptualImageFingerprint::from_rgba(&converted);

        assert!(original.is_similar_to(&converted));
    }

    #[test]
    fn perceptual_image_fingerprint_requires_matching_dimensions() {
        let original = PerceptualImageFingerprint::from_rgba(&sample_image(128, 64, 0));
        let resized = PerceptualImageFingerprint::from_rgba(&sample_image(129, 64, 0));

        assert!(!original.is_similar_to(&resized));
    }

    #[test]
    fn perceptual_image_fingerprint_rejects_visually_different_images() {
        let dark = RgbaImage::from_pixel(128, 64, Rgba([0, 0, 0, u8::MAX]));
        let light = RgbaImage::from_pixel(128, 64, Rgba([255, 255, 255, u8::MAX]));
        let dark = PerceptualImageFingerprint::from_rgba(&dark);
        let light = PerceptualImageFingerprint::from_rgba(&light);

        assert!(!dark.is_similar_to(&light));
    }

    #[test]
    fn ordinary_image_observation_is_not_an_echo_until_queued_for_sync() {
        let state = ClipboardFingerprintState::new();
        let fingerprint = image_fingerprint(sample_image(128, 64, 0), "original");
        let converted = image_fingerprint(sample_image(128, 64, 1), "converted");
        let observation = state.begin_observation();
        assert!(state.commit_observation(&observation, fingerprint.clone()));

        assert!(!state.matches_recent_synced_image(&converted));
        state.remember_synced_image(&fingerprint);
        assert!(state.matches_recent_synced_image(&converted));
        assert!(!state.matches_recent_synced_image(&fingerprint));
    }

    #[test]
    fn expired_synced_image_is_not_treated_as_an_echo() {
        let state = ClipboardFingerprintState::new();
        let fingerprint = image_fingerprint(sample_image(128, 64, 0), "original");
        let converted = image_fingerprint(sample_image(128, 64, 1), "converted");
        let now = Instant::now();
        {
            let mut inner = state.inner.lock().unwrap();
            remember_synced_image_at(
                &mut inner,
                fingerprint,
                now - RECENT_SYNCED_IMAGE_TTL - Duration::from_millis(1),
                None,
            );
        }

        assert!(!state.matches_recent_synced_image_at(&converted, now));
    }

    fn exact_fingerprint(value: &str) -> ClipboardFingerprint {
        ClipboardFingerprint::exact(value.to_owned())
    }

    fn image_fingerprint(image: RgbaImage, exact: &str) -> ClipboardFingerprint {
        ClipboardFingerprint {
            exact: exact.to_owned(),
            perceptual_image: Some(PerceptualImageFingerprint::from_rgba(&image)),
        }
    }

    fn sample_image(width: u32, height: u32, offset: u8) -> RgbaImage {
        RgbaImage::from_fn(width, height, |x, y| {
            let red = ((x * 3 + y) % 240) as u8 + offset;
            let green = ((x + y * 2) % 240) as u8 + offset;
            let blue = ((x * 2 + y * 3) % 240) as u8 + offset;
            Rgba([red, green, blue, u8::MAX])
        })
    }

    fn text_item(
        content: &str,
        sub_kind: Option<ClipboardSubKind>,
        search_text: Option<&str>,
    ) -> ClipboardItem {
        ClipboardItem {
            id: "item".to_owned(),
            kind: ClipboardKind::Text,
            sub_kind,
            group_id: None,
            source_app_id: None,
            source_revision: "revision".to_owned(),
            content: content.to_owned(),
            content_hash: String::new(),
            search_text: search_text.map(str::to_owned),
            summary: None,
            text_char_count: None,
            file_types: None,
            size: None,
            width: None,
            height: None,
            use_count: 1,
            is_favorite: false,
            is_pinned: false,
            is_sensitive: false,
            platform: Platform::Macos,
            note: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            source_app_accent_start: None,
            source_app_accent_end: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        }
    }
}
