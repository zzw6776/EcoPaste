//! 系统剪贴板的跨设备稳定语义指纹与本地写回抑制状态机。
//!
//! 数据库 `content_hash` 负责历史去重；本模块同时承担两项职责：
//! 1. 判断远端同步内容是否已等同于当前系统剪贴板语义内容，避免远程桌面或跨端重入重复写剪贴板；
//! 2. 统一管理本地手动复制/粘贴与同步自动写回的待确认令牌（带 TTL 兜底），在系统监听回调
//!    到达时按语义指纹（文本纯文本表示、图片解码像素、文件规范化路径）精准拦截自身写回回环。

use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{Duration, Instant},
};

#[cfg(any(not(target_os = "android"), test))]
use std::sync::LazyLock;

#[cfg(any(not(target_os = "android"), test))]
use image::{imageops::FilterType, ImageReader, Rgba, RgbaImage};

use crate::db::models::{ClipboardItem, ClipboardKind, ClipboardSubKind};

const TEXT_DOMAIN: &[u8] = b"ecopaste-clipboard-text-v1\0";
#[cfg(any(not(target_os = "android"), test))]
const IMAGE_DOMAIN: &[u8] = b"ecopaste-clipboard-image-v1\0";
const FILES_DOMAIN: &[u8] = b"ecopaste-clipboard-files-v1\0";
#[cfg(any(not(target_os = "android"), test))]
const LOCAL_FILES_DOMAIN: &[u8] = b"ecopaste-clipboard-local-files-v1\0";
/// 登记的本地写回令牌有效时长：监听事件通常在毫秒级到达，给足冗余同时避免系统未发变更通知时吞掉后续真实复制。
const WRITEBACK_SUPPRESS_TTL: Duration = Duration::from_secs(2);
#[cfg(any(not(target_os = "android"), test))]
const PERCEPTUAL_HASH_COLUMNS: u32 = 16;
#[cfg(any(not(target_os = "android"), test))]
const PERCEPTUAL_HASH_ROWS: u32 = 16;
#[cfg(any(not(target_os = "android"), test))]
const PERCEPTUAL_SAMPLE_COUNT: usize =
    ((PERCEPTUAL_HASH_COLUMNS + 1) * PERCEPTUAL_HASH_ROWS) as usize;
#[cfg(any(not(target_os = "android"), test))]
const PERCEPTUAL_HASH_DISTANCE_LIMIT: u32 = 32;
#[cfg(any(not(target_os = "android"), test))]
const PERCEPTUAL_LUMA_MAE_LIMIT: u32 = 2;
#[cfg(any(not(target_os = "android"), test))]
const RECENT_SYNCED_IMAGE_LIMIT: usize = 8;
#[cfg(any(not(target_os = "android"), test))]
const RECENT_SYNCED_IMAGE_TTL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClipboardFingerprint {
    exact: String,
    #[cfg(any(not(target_os = "android"), test))]
    perceptual_image: Option<PerceptualImageFingerprint>,
}

#[cfg(any(not(target_os = "android"), test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PerceptualImageFingerprint {
    width: u32,
    height: u32,
    difference_hash: [u64; 4],
    reduced_luma: [u8; PERCEPTUAL_SAMPLE_COUNT],
}

impl ClipboardFingerprint {
    /// Builds the canonical cross-device fingerprint for a plain-text clipboard value.
    pub fn from_text(text: &str) -> Self {
        Self::exact(hash_parts(TEXT_DOMAIN, [text.as_bytes()]))
    }

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

        Some(Self::from_text(text))
    }

    /// 图片使用 RGBA 语义指纹；最多缓存 64 个字节摘要到指纹的映射，不保留原图或像素。
    /// 编码字节变化时重新解码，PNG/TIFF/DIB 重新编码仍按像素判等。
    #[cfg(any(not(target_os = "android"), test))]
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

    /// 本地文件剪贴板写回令牌：仅对路径列表做零文件 I/O 的规范化摘要，避免监听线程阻塞在大文件读取上。
    #[cfg(any(not(target_os = "android"), test))]
    pub fn local_files_suppression_token(content: &str) -> String {
        local_files_suppression_token_with_case(content, cfg!(target_os = "windows"))
    }

    /// 写回保护继续使用与监听端一致的字符串形式。
    pub fn as_str(&self) -> &str {
        &self.exact
    }

    fn exact(value: String) -> Self {
        Self {
            exact: value,
            #[cfg(any(not(target_os = "android"), test))]
            perceptual_image: None,
        }
    }
}

#[cfg(any(not(target_os = "android"), test))]
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

#[cfg(any(not(target_os = "android"), test))]
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
    pending_writebacks: VecDeque<PendingWriteback>,
    #[cfg(any(not(target_os = "android"), test))]
    recent_synced_images: VecDeque<RecentSyncedImage>,
}

struct PendingWriteback {
    generation: u64,
    token: String,
    published: Option<ClipboardFingerprint>,
    recorded_at: Instant,
}

#[cfg(any(not(target_os = "android"), test))]
struct RecentSyncedImage {
    fingerprint: ClipboardFingerprint,
    recorded_at: Instant,
    write_generation: Option<u64>,
}

#[derive(Clone)]
pub struct ClipboardObservation {
    generation: u64,
    #[cfg(any(not(target_os = "android"), test))]
    outbound_image: Option<ClipboardFingerprint>,
}

pub struct ClipboardWrite {
    generation: u64,
    previous: Option<ClipboardFingerprint>,
}

impl ClipboardFingerprintState {
    pub fn new() -> Self {
        Self::default()
    }

    /// 系统报告一次剪贴板变化时先失效当前语义指纹；若后续确认命中自身写回则按登记值恢复。
    pub fn begin_observation(&self) -> ClipboardObservation {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state.generation = state.generation.wrapping_add(1);
        state.current = None;
        ClipboardObservation {
            generation: state.generation,
            #[cfg(any(not(target_os = "android"), test))]
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
        true
    }

    /// 判断本次系统监听事件是否对应未过期的自身写回：命中则消费单次令牌，并在当前代次恢复语义指纹。
    pub fn consume_pending_writeback(
        &self,
        observation: &ClipboardObservation,
        token: &str,
        observed_fingerprint: Option<&ClipboardFingerprint>,
    ) -> bool {
        self.consume_pending_writeback_at(observation, token, observed_fingerprint, Instant::now())
    }

    fn consume_pending_writeback_at(
        &self,
        observation: &ClipboardObservation,
        token: &str,
        observed_fingerprint: Option<&ClipboardFingerprint>,
        now: Instant,
    ) -> bool {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        prune_pending_writebacks(&mut state, now);
        let Some(index) = state
            .pending_writebacks
            .iter()
            .position(|entry| entry.token == token)
        else {
            return false;
        };
        let entry = state
            .pending_writebacks
            .remove(index)
            .expect("checked pending writeback index");
        if state.generation == observation.generation && state.current.is_none() {
            state.current = entry.published.or_else(|| observed_fingerprint.cloned());
        }
        true
    }

    /// Captures clipboard changes while an asynchronous sync download is running.
    pub fn generation(&self) -> u64 {
        self.inner
            .lock()
            .expect("clipboard fingerprint state poisoned")
            .generation
    }

    pub fn matches(&self, fingerprint: &ClipboardFingerprint) -> bool {
        let state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state.current.as_ref() == Some(fingerprint)
    }

    /// 只与最近真正进入同步队列或由同步自动写入的图片比较，避免影响普通历史去重。
    #[cfg(any(not(target_os = "android"), test))]
    pub fn matches_recent_synced_image(&self, fingerprint: &ClipboardFingerprint) -> bool {
        if fingerprint.perceptual_image.is_none() {
            return false;
        }
        self.matches_recent_synced_image_at(fingerprint, Instant::now())
    }

    /// 同步事件成功落入本地事件队列后，短期记录其感知指纹以识别远程桌面反射。
    #[cfg(any(not(target_os = "android"), test))]
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

    /// 写系统剪贴板前登记本地抑制令牌并按需发布目标语义指纹；写入失败时可按代次安全回滚。
    pub fn begin_write_with_token(
        &self,
        token: String,
        published: Option<ClipboardFingerprint>,
        synced: bool,
    ) -> ClipboardWrite {
        self.begin_write_at(token, published, synced, Instant::now())
    }

    /// 同步文本等令牌与语义指纹同源时的便捷写前登记入口。
    #[cfg(test)]
    pub fn begin_write(&self, fingerprint: ClipboardFingerprint) -> ClipboardWrite {
        let token = fingerprint.as_str().to_owned();
        self.begin_write_with_token(token, Some(fingerprint), true)
    }

    fn begin_write_at(
        &self,
        token: String,
        published: Option<ClipboardFingerprint>,
        synced: bool,
        now: Instant,
    ) -> ClipboardWrite {
        let _ = synced;
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        prune_pending_writebacks(&mut state, now);
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        #[cfg(any(not(target_os = "android"), test))]
        let recent_image = if synced {
            published.as_ref().and_then(|fingerprint| {
                fingerprint
                    .perceptual_image
                    .as_ref()
                    .map(|_| fingerprint.clone())
            })
        } else {
            None
        };
        let previous = std::mem::replace(&mut state.current, published.clone());
        state.pending_writebacks.push_back(PendingWriteback {
            generation,
            token,
            published,
            recorded_at: now,
        });
        #[cfg(any(not(target_os = "android"), test))]
        if let Some(recent_image) = recent_image {
            remember_synced_image_at(&mut state, recent_image, now, Some(generation));
        }
        ClipboardWrite {
            generation,
            previous,
        }
    }

    pub fn rollback_write(&self, write: ClipboardWrite) {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state
            .pending_writebacks
            .retain(|entry| entry.generation != write.generation);
        #[cfg(any(not(target_os = "android"), test))]
        state
            .recent_synced_images
            .retain(|recent| recent.write_generation != Some(write.generation));
        if state.generation == write.generation {
            state.current = write.previous;
        }
    }

    #[cfg(any(not(target_os = "android"), test))]
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

#[cfg(any(not(target_os = "android"), test))]
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

fn prune_pending_writebacks(state: &mut FingerprintState, now: Instant) {
    state.pending_writebacks.retain(|entry| {
        now.checked_duration_since(entry.recorded_at)
            .is_some_and(|age| age <= WRITEBACK_SUPPRESS_TTL)
    });
}

#[cfg(any(not(target_os = "android"), test))]
fn local_files_suppression_token_with_case(content: &str, case_insensitive: bool) -> String {
    let mut normalized = content
        .split('\n')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| normalize_local_file_path(value, case_insensitive))
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    hash_parts(LOCAL_FILES_DOMAIN, normalized.iter().map(String::as_bytes))
}

#[cfg(any(not(target_os = "android"), test))]
fn normalize_local_file_path(path: &str, case_insensitive: bool) -> String {
    let stripped = path.strip_prefix("file://").unwrap_or(path);
    let unified = if case_insensitive {
        stripped.replace('\\', "/")
    } else {
        stripped.to_owned()
    };
    let trimmed = trim_trailing_path_separators(&unified);
    if case_insensitive {
        trimmed.to_lowercase()
    } else {
        trimmed.to_owned()
    }
}

#[cfg(any(not(target_os = "android"), test))]
fn trim_trailing_path_separators(path: &str) -> &str {
    if path == "/" {
        return path;
    }
    if path.len() == 3 && path.as_bytes()[1] == b':' && path.as_bytes()[2] == b'/' {
        return path;
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/";
    }
    if trimmed.len() == 2 && trimmed.as_bytes()[1] == b':' {
        return &path[..3];
    }
    trimmed
}

#[cfg(any(not(target_os = "android"), test))]
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

#[cfg(any(not(target_os = "android"), test))]
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

        assert!(state.consume_pending_writeback(&observation, expected.as_str(), Some(&expected)));
        assert!(state.matches(&expected));
        // 单次消费后，同内容的下一次真实复制不再被吞。
        let next = state.begin_observation();
        assert!(!state.consume_pending_writeback(&next, expected.as_str(), Some(&expected)));
    }

    #[test]
    fn ordinary_observation_without_pending_write_does_not_restore_previous() {
        let state = ClipboardFingerprintState::new();
        let previous = exact_fingerprint("previous");
        let copied = state.begin_observation();
        assert!(state.commit_observation(&copied, previous.clone()));

        let ordinary = state.begin_observation();
        assert!(!state.consume_pending_writeback(&ordinary, previous.as_str(), Some(&previous)));
        assert!(!state.matches(&previous));
    }

    #[test]
    fn supports_multiple_pending_writebacks_and_preserves_unrelated_copy() {
        let state = ClipboardFingerprintState::new();
        let first = exact_fingerprint("first");
        let second = exact_fingerprint("second");
        let unrelated = exact_fingerprint("unrelated");
        let _w1 = state.begin_write(first.clone());
        let _w2 = state.begin_write(second.clone());

        let obs_unrelated = state.begin_observation();
        assert!(!state.consume_pending_writeback(
            &obs_unrelated,
            unrelated.as_str(),
            Some(&unrelated)
        ));
        assert!(state.commit_observation(&obs_unrelated, unrelated));

        let obs_first = state.begin_observation();
        assert!(state.consume_pending_writeback(&obs_first, first.as_str(), Some(&first)));
        let obs_second = state.begin_observation();
        assert!(state.consume_pending_writeback(&obs_second, second.as_str(), Some(&second)));
        assert!(state.matches(&second));
    }

    #[test]
    fn expired_pending_writeback_is_ignored() {
        let state = ClipboardFingerprintState::new();
        let now = Instant::now();
        let expected = exact_fingerprint("expired");
        let _write = state.begin_write_at(
            expected.as_str().to_owned(),
            Some(expected.clone()),
            false,
            now - WRITEBACK_SUPPRESS_TTL - Duration::from_millis(1),
        );
        let observation = state.begin_observation();

        assert!(!state.consume_pending_writeback_at(
            &observation,
            expected.as_str(),
            Some(&expected),
            now,
        ));
    }

    #[test]
    fn failed_write_rolls_back_state_and_cancels_suppression() {
        let state = ClipboardFingerprintState::new();
        let previous = exact_fingerprint("previous");
        let failed = exact_fingerprint("failed");
        let _initial = state.begin_write(previous.clone());
        let write = state.begin_write(failed.clone());

        state.rollback_write(write);

        assert!(state.matches(&previous));
        let observation = state.begin_observation();
        assert!(!state.consume_pending_writeback(&observation, failed.as_str(), Some(&failed)));
    }

    #[test]
    fn windows_cf_html_wrapped_writeback_matches_original_fragment() {
        let state = ClipboardFingerprintState::new();
        let macos_fragment = text_item(
            "<meta charset='utf-8'><span>Hello Windows</span>",
            Some(ClipboardSubKind::Html),
            Some("Hello Windows"),
        );
        let written_fp = ClipboardFingerprint::from_text_item(&macos_fragment).unwrap();
        let _write = state.begin_write(written_fp);

        let windows_readback = text_item(
            "<html>\r\n<body>\r\n<!--StartFragment--><meta charset='utf-8'><span>Hello Windows</span><!--EndFragment-->\r\n</body>\r\n</html>",
            Some(ClipboardSubKind::Html),
            Some("Hello Windows"),
        );
        let observed_fp = ClipboardFingerprint::from_text_item(&windows_readback).unwrap();
        let observation = state.begin_observation();

        assert!(state.consume_pending_writeback(
            &observation,
            observed_fp.as_str(),
            Some(&observed_fp),
        ));
        assert!(state.matches(&observed_fp));
    }

    #[test]
    fn local_files_token_normalizes_separators_trailing_slashes_and_case() {
        assert_eq!(
            local_files_suppression_token_with_case(
                "C:\\Users\\Test\\Docs\\\nD:\\Data\\report.pdf",
                true,
            ),
            local_files_suppression_token_with_case(
                "file://d:/data/report.pdf\nc:/users/test/docs",
                true,
            )
        );
        assert_eq!(
            local_files_suppression_token_with_case("/Users/test/folder/\n/tmp/a.txt", false),
            local_files_suppression_token_with_case("file:///tmp/a.txt\n/Users/test/folder", false)
        );
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
        assert!(second_device.consume_pending_writeback(
            &local_callback,
            fingerprint.as_str(),
            Some(&fingerprint),
        ));

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
            source_device_id: None,
            source_device_name: None,
            source_updated_at: None,
            source_device_is_local: false,
            content: content.to_owned(),
            content_hash: String::new(),
            sync_key: String::new(),
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
            remote_file_state: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        }
    }
}
