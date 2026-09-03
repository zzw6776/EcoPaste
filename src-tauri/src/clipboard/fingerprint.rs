//! 系统剪贴板的跨设备稳定语义指纹。
//!
//! 数据库 `content_hash` 负责历史去重；这里的指纹只用于判断同步内容是否已经是当前
//! 系统剪贴板内容，从而避免远程桌面与 EcoPaste 双向同步形成反馈循环。

use std::sync::Mutex;

use image::ImageReader;

use crate::db::models::{ClipboardItem, ClipboardKind, ClipboardSubKind};

const TEXT_DOMAIN: &[u8] = b"ecopaste-clipboard-text-v1\0";
const IMAGE_DOMAIN: &[u8] = b"ecopaste-clipboard-image-v1\0";
const FILES_DOMAIN: &[u8] = b"ecopaste-clipboard-files-v1\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClipboardFingerprint(String);

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

        Some(Self(hash_parts(TEXT_DOMAIN, [text.as_bytes()])))
    }

    /// 图片使用解码后的 RGBA 像素，避免 PNG/TIFF/DIB 重新编码改变字节哈希。
    pub fn from_image_bytes(bytes: &[u8]) -> Option<Self> {
        let image = ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .ok()?
            .decode()
            .ok()?
            .to_rgba8();
        let width = image.width().to_le_bytes();
        let height = image.height().to_le_bytes();

        Some(Self(hash_parts(
            IMAGE_DOMAIN,
            [width.as_slice(), height.as_slice(), image.as_raw()],
        )))
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

        Some(Self(hasher.finalize().to_hex().to_string()))
    }

    /// 写回保护继续使用与监听端一致的字符串形式。
    pub fn as_str(&self) -> &str {
        &self.0
    }
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
}

#[derive(Clone)]
pub struct ClipboardObservation {
    generation: u64,
    previous: Option<ClipboardFingerprint>,
    restore_previous: bool,
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

    /// 写系统剪贴板前先发布目标指纹；写入失败时可按代次安全回滚。
    pub fn begin_write(&self, fingerprint: ClipboardFingerprint) -> ClipboardWrite {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        state.generation = state.generation.wrapping_add(1);
        let previous = state.current.replace(fingerprint);
        let previous_write_pending = state.write_pending;
        state.write_pending = true;
        ClipboardWrite {
            generation: state.generation,
            previous,
            previous_write_pending,
        }
    }

    pub fn rollback_write(&self, write: ClipboardWrite) {
        let mut state = self
            .inner
            .lock()
            .expect("clipboard fingerprint state poisoned");
        if state.generation == write.generation {
            state.current = write.previous;
            state.write_pending = write.previous_write_pending;
        }
    }
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
        let current = ClipboardFingerprint("current".to_owned());
        let write = state.begin_write(current.clone());

        assert!(!state.commit_observation(&slow, ClipboardFingerprint("stale".to_owned())));
        assert!(state.matches(&current));
        drop(write);
    }

    #[test]
    fn suppressed_writeback_restores_the_published_fingerprint() {
        let state = ClipboardFingerprintState::new();
        let expected = ClipboardFingerprint("expected".to_owned());
        let _write = state.begin_write(expected.clone());
        let observation = state.begin_observation();

        state.restore_observation(observation);

        assert!(state.matches(&expected));
    }

    #[test]
    fn ordinary_writeback_does_not_restore_the_previous_fingerprint() {
        let state = ClipboardFingerprintState::new();
        let previous = ClipboardFingerprint("previous".to_owned());
        let copied = state.begin_observation();
        assert!(state.commit_observation(&copied, previous.clone()));

        let ordinary_writeback = state.begin_observation();
        state.restore_observation(ordinary_writeback);

        assert!(!state.matches(&previous));
    }

    #[test]
    fn failed_write_rolls_back_if_no_newer_event_arrived() {
        let state = ClipboardFingerprintState::new();
        let previous = ClipboardFingerprint("previous".to_owned());
        let _initial = state.begin_write(previous.clone());
        let write = state.begin_write(ClipboardFingerprint("failed".to_owned()));

        state.rollback_write(write);

        assert!(state.matches(&previous));
    }

    #[test]
    fn reflected_content_converges_without_a_second_write() {
        let fingerprint = ClipboardFingerprint("shared".to_owned());
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
