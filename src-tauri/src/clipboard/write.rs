//! 剪贴板写回：把 [`ClipboardItem`] 按类型写回系统剪贴板（text / html / rtf / image / files）。
//!
//! 时序约束：[`ClipboardContext`] 是 `!Send`，调用方需在不跨 await 的同步段内完成调用
//! （命令层照 `read_clipboard` 的写法处理）。
//!
//! 回环抑制：写回前统一向 [`ClipboardFingerprintState`] 登记语义抑制令牌并按需发布当前语义指纹，
//! OS 监听重新读到同语义内容时跳过入库，避免「点击粘贴 / 自动同步写回 → 再次入库」回环：
//! - text / html / rtf：统一按系统剪贴板纯文本表示生成语义指纹，兼容 Windows `CF_HTML` 包装；
//! - image：使用解码后的 RGBA 像素指纹，兼容 macOS / Windows 重编码；
//! - files：使用零文件 I/O 的规范化路径列表令牌，同步写回同时保留跨设备文件语义指纹。
//!
//! 纯文本模式（`plain = true`）：忽略 `sub_kind`，写 `search_text`（OS 提供的纯文本表示），
//! 缺失时退回 `content`。供「纯文本粘贴」快捷路径使用。

#[cfg(not(any(target_os = "android", target_os = "macos")))]
use std::time::Duration;

#[cfg(not(any(target_os = "android", target_os = "macos")))]
use clipboard_rs::common::RustImage;
#[cfg(not(target_os = "android"))]
use clipboard_rs::{Clipboard, ClipboardContent, ClipboardContext};

use super::fingerprint::{ClipboardFingerprint, ClipboardFingerprintState};
#[cfg(not(target_os = "android"))]
use super::storage::ImageStore;
use crate::core::{AppError, Result};
#[cfg(all(test, not(target_os = "android")))]
use crate::db::items::content_hash;
use crate::db::models::ClipboardItem;
#[cfg(not(target_os = "android"))]
use crate::db::models::ClipboardKind;
#[cfg(not(target_os = "android"))]
use crate::db::models::ClipboardSubKind;

/// 把 `item` 写回系统剪贴板；`plain = true` 强制只写纯文本（剥离 HTML/RTF）。
#[cfg(not(target_os = "android"))]
pub fn write_to_clipboard(
    store: &ImageStore,
    fingerprints: &ClipboardFingerprintState,
    item: &ClipboardItem,
    plain: bool,
) -> Result<()> {
    let ctx = ClipboardContext::new().map_err(clip_err)?;

    match item.kind {
        ClipboardKind::Text => {
            let fingerprint = text_write_fingerprint(item, plain);
            let write = fingerprints.begin_write_with_token(
                fingerprint.as_str().to_owned(),
                Some(fingerprint),
                false,
            );
            if let Err(error) = write_text_raw(&ctx, item, plain) {
                fingerprints.rollback_write(write);
                return Err(error);
            }
        }
        ClipboardKind::Image => {
            let bytes = read_image_bytes(store, item)?;
            let fingerprint = ClipboardFingerprint::from_image_bytes(&bytes);
            let token = fingerprint
                .as_ref()
                .map(|value| value.as_str().to_owned())
                .unwrap_or_else(|| item.content_hash.clone());
            let write = fingerprints.begin_write_with_token(token, fingerprint, false);
            if let Err(error) = write_image_raw(&ctx, &bytes) {
                fingerprints.rollback_write(write);
                return Err(error);
            }
        }
        // files + plain：把路径列表当文本写回，供「粘贴为路径」使用。
        ClipboardKind::Files if plain => {
            let fingerprint = ClipboardFingerprint::from_text(item.content.trim());
            let write = fingerprints.begin_write_with_token(
                fingerprint.as_str().to_owned(),
                Some(fingerprint),
                false,
            );
            if let Err(error) = write_files_as_text_raw(&ctx, item) {
                fingerprints.rollback_write(write);
                return Err(error);
            }
        }
        ClipboardKind::Files => {
            let paths = parse_file_paths(item)?;
            let token = ClipboardFingerprint::local_files_suppression_token(&item.content);
            let write = fingerprints.begin_write_with_token(token, None, false);
            if let Err(error) = ctx.set_files(paths).map_err(clip_err) {
                fingerprints.rollback_write(write);
                return Err(error);
            }
        }
    }
    Ok(())
}

/// 同步条目仅在语义内容变化时写入系统剪贴板，返回是否实际执行了写入。
#[cfg(not(target_os = "android"))]
pub fn write_synced_item_to_clipboard(
    store: &ImageStore,
    fingerprints: &ClipboardFingerprintState,
    item: &ClipboardItem,
    files_fingerprint: Option<&ClipboardFingerprint>,
) -> Result<bool> {
    let image_bytes = if item.kind == ClipboardKind::Image {
        Some(read_image_bytes(store, item)?)
    } else {
        None
    };
    let published = match item.kind {
        ClipboardKind::Text => Some(text_write_fingerprint(item, false)),
        ClipboardKind::Image => image_bytes
            .as_deref()
            .and_then(ClipboardFingerprint::from_image_bytes),
        ClipboardKind::Files => files_fingerprint.cloned(),
    };
    if published
        .as_ref()
        .is_some_and(|fingerprint| fingerprints.matches(fingerprint))
    {
        return Ok(false);
    }

    let file_paths = if item.kind == ClipboardKind::Files {
        Some(parse_file_paths(item)?)
    } else {
        None
    };
    let token = match item.kind {
        ClipboardKind::Text => published
            .as_ref()
            .expect("text fingerprint prepared")
            .as_str()
            .to_owned(),
        ClipboardKind::Image => published
            .as_ref()
            .map(|value| value.as_str().to_owned())
            .unwrap_or_else(|| item.content_hash.clone()),
        ClipboardKind::Files => ClipboardFingerprint::local_files_suppression_token(&item.content),
    };
    let write = fingerprints.begin_write_with_token(token, published, true);
    let result = (|| {
        let ctx = ClipboardContext::new().map_err(clip_err)?;
        match item.kind {
            ClipboardKind::Text => write_text_raw(&ctx, item, false),
            ClipboardKind::Image => {
                write_image_raw(&ctx, image_bytes.as_deref().expect("image bytes prepared"))
            }
            ClipboardKind::Files => ctx
                .set_files(file_paths.expect("file paths prepared"))
                .map_err(clip_err),
        }
    })();
    if result.is_err() {
        fingerprints.rollback_write(write);
    }
    result.map(|_| true)
}

#[cfg(target_os = "android")]
pub fn write_to_clipboard_app(
    _app: &tauri::AppHandle,
    fingerprints: &ClipboardFingerprintState,
    item: &ClipboardItem,
) -> Result<()> {
    let text = item.search_text.as_deref().unwrap_or(&item.content);
    write_text_to_clipboard_app(fingerprints, text, false)
}

/// Android 同步文本仅在纯文本语义变化时写回系统剪贴板。
#[cfg(target_os = "android")]
pub fn write_synced_item_to_clipboard_app(
    _app: &tauri::AppHandle,
    fingerprints: &ClipboardFingerprintState,
    item: &ClipboardItem,
) -> Result<bool> {
    let text = item.search_text.as_deref().unwrap_or(&item.content).trim();
    if text.is_empty() {
        return Ok(false);
    }
    let fingerprint = ClipboardFingerprint::from_text(text);
    if fingerprints.matches(&fingerprint) {
        return Ok(false);
    }

    write_text_to_clipboard_app(fingerprints, text, true).map(|_| true)
}

#[cfg(target_os = "android")]
fn write_text_to_clipboard_app(
    fingerprints: &ClipboardFingerprintState,
    text: &str,
    synced: bool,
) -> Result<()> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    let fingerprint = ClipboardFingerprint::from_text(trimmed);
    let write = fingerprints.begin_write_with_token(
        fingerprint.as_str().to_owned(),
        Some(fingerprint),
        synced,
    );
    let result = crate::commands::android::write_android_clipboard_text(text)
        .map_err(|error| AppError::Clipboard(error.to_string()));
    if result.is_err() {
        fingerprints.rollback_write(write);
    }
    result
}

#[cfg(not(target_os = "android"))]
fn text_write_fingerprint(item: &ClipboardItem, plain: bool) -> ClipboardFingerprint {
    let text = if plain
        || matches!(
            item.sub_kind,
            Some(ClipboardSubKind::Html | ClipboardSubKind::Rtf)
        ) {
        item.search_text.as_deref().unwrap_or(&item.content)
    } else {
        &item.content
    };
    ClipboardFingerprint::from_text(text.trim())
}

#[cfg(not(target_os = "android"))]
fn write_text_raw(ctx: &ClipboardContext, item: &ClipboardItem, plain: bool) -> Result<()> {
    // 纯文本模式下，OS 提供的 plain 表示优先；缺失时退回 content（plain 文本场景下 content 即纯文本）。
    let (content, sub_kind) = if plain {
        let text = item
            .search_text
            .clone()
            .unwrap_or_else(|| item.content.clone());
        (text, None)
    } else {
        (item.content.clone(), item.sub_kind)
    };

    match sub_kind {
        // 纯文本模式必须只写 Text flavor，确保清掉剪贴板中可能残留的 HTML/RTF。
        None if plain => ctx
            .set(vec![ClipboardContent::Text(content)])
            .map_err(clip_err)?,
        // HTML / RTF 必须同时写入纯文本回退：clipboard-rs 的 set_html / set_rich_text
        // 会先 clearContents，单独写时只剩富格式，多数应用读 plain/text 拿不到就拒绝粘贴。
        // 走 set(Vec<ClipboardContent>) 一次写多格式（内部不再相互清空）。
        Some(ClipboardSubKind::Html) => {
            let plain = item.search_text.clone().unwrap_or_else(|| content.clone());
            ctx.set(vec![
                ClipboardContent::Text(plain),
                ClipboardContent::Html(content),
            ])
            .map_err(clip_err)?;
        }
        Some(ClipboardSubKind::Rtf) => {
            let plain = item.search_text.clone().unwrap_or_else(|| content.clone());
            ctx.set(vec![
                ClipboardContent::Text(plain),
                ClipboardContent::Rtf(content),
            ])
            .map_err(clip_err)?;
        }
        // url / email / color / path 及无 sub_kind 都走纯文本通道。
        _ => ctx.set_text(content).map_err(clip_err)?,
    }
    Ok(())
}

#[cfg(not(target_os = "android"))]
fn read_image_bytes(store: &ImageStore, item: &ClipboardItem) -> Result<Vec<u8>> {
    let path = store.origin_path(&item.content);
    std::fs::read(&path).map_err(|err| {
        log::error!("read image {path:?} failed: {err}");
        AppError::Clipboard(err.to_string())
    })
}

#[cfg(not(target_os = "android"))]
fn write_image_raw(
    #[cfg_attr(target_os = "macos", allow(unused_variables))] ctx: &ClipboardContext,
    bytes: &[u8],
) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use objc2::runtime::ProtocolObject;
        use objc2_app_kit::{NSPasteboard, NSPasteboardItem, NSPasteboardTypePNG};
        use objc2_foundation::{NSArray, NSData};

        let pasteboard = NSPasteboard::generalPasteboard();
        pasteboard.clearContents();
        let ns_data = unsafe {
            NSData::dataWithBytes_length(bytes.as_ptr() as *const std::ffi::c_void, bytes.len())
        };
        let item_obj = NSPasteboardItem::new();
        unsafe {
            item_obj.setData_forType(&ns_data, NSPasteboardTypePNG);
            let write_objects =
                NSArray::from_retained_slice(&[ProtocolObject::from_retained(item_obj)]);
            if !pasteboard.writeObjects(&write_objects) {
                return Err(AppError::Clipboard("writeObjects failed".to_owned()));
            }
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        set_image_with_retry(ctx, bytes)?;
    }

    Ok(())
}

/// Windows 的系统剪贴板可能被其它进程短暂占用；图片写入做有限重试。
#[cfg(not(any(target_os = "android", target_os = "macos")))]
fn set_image_with_retry(ctx: &ClipboardContext, bytes: &[u8]) -> Result<()> {
    const RETRY_DELAYS: [Duration; 4] = [
        Duration::ZERO,
        Duration::from_millis(15),
        Duration::from_millis(35),
        Duration::from_millis(75),
    ];

    let attempts = if cfg!(target_os = "windows") {
        RETRY_DELAYS.len()
    } else {
        1
    };
    let mut last_error = None;
    for delay in RETRY_DELAYS.iter().take(attempts) {
        if !delay.is_zero() {
            std::thread::sleep(*delay);
        }
        let image = clipboard_rs::RustImageData::from_bytes(bytes).map_err(clip_err)?;
        match ctx.set_image(image) {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }

    Err(clip_err(
        last_error.expect("image write attempted at least once"),
    ))
}

#[cfg(not(target_os = "android"))]
fn parse_file_paths(item: &ClipboardItem) -> Result<Vec<String>> {
    let paths: Vec<String> = item
        .content
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if paths.is_empty() {
        return Err(AppError::Clipboard("no files to write".to_owned()));
    }
    Ok(paths)
}

#[cfg(not(target_os = "android"))]
/// 把 files 条目的路径列表当文本写回（换行分隔，多文件按行展开）。
fn write_files_as_text_raw(ctx: &ClipboardContext, item: &ClipboardItem) -> Result<()> {
    ctx.set_text(item.content.clone()).map_err(clip_err)
}

#[cfg(not(target_os = "android"))]
fn clip_err<E: std::fmt::Display>(err: E) -> AppError {
    AppError::Clipboard(err.to_string())
}

#[cfg(all(test, not(target_os = "android")))]
mod tests {
    use super::super::payload::ImagePayload;
    use super::super::read::ClipboardReader;
    use super::*;
    use crate::clipboard::{build_item, ImageStore};
    use crate::db::models::Platform;
    use chrono::Utc;

    fn text_item(
        content: &str,
        sub: Option<ClipboardSubKind>,
        search: Option<&str>,
    ) -> ClipboardItem {
        ClipboardItem {
            id: uuid::Uuid::new_v4().to_string(),
            kind: ClipboardKind::Text,
            sub_kind: sub,
            group_id: None,
            source_app_id: None,
            source_revision: uuid::Uuid::new_v4().simple().to_string(),
            source_device_id: None,
            source_device_name: None,
            source_updated_at: None,
            source_device_is_local: false,
            content_hash: content_hash(ClipboardKind::Text, content),
            sync_key: content_hash(ClipboardKind::Text, content),
            content: content.to_owned(),
            search_text: search.map(str::to_owned),
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

    fn temp_store() -> (TempDir, ImageStore) {
        let dir = TempDir::new();
        let store = ImageStore::for_test(dir.path().join("resources").join("clipboard-images"));
        (dir, store)
    }

    // 触碰真实剪贴板：写入纯文本 → 读回应为同串，且语义状态机已登记本次抑制令牌。
    #[test]
    #[ignore = "touches the real system clipboard; run with --ignored on a desktop session"]
    fn writes_plain_text_and_arms_fingerprint_state() {
        let _serial = crate::clipboard::test_lock::serial();
        let (_dir, store) = temp_store();
        let fingerprints = ClipboardFingerprintState::new();

        let item = text_item("hello write", None, None);
        write_to_clipboard(&store, &fingerprints, &item, false).unwrap();

        let reader = ClipboardReader::new().unwrap();
        let payload = reader
            .read_with_capture(&crate::settings::Capture::default())
            .unwrap()
            .expect("should read");
        let read_item = build_item(&store, &payload).unwrap().unwrap();
        assert_eq!(read_item.content, "hello write");
        let observed = ClipboardFingerprint::from_text_item(&read_item).unwrap();
        let observation = fingerprints.begin_observation();
        assert!(fingerprints.consume_pending_writeback(
            &observation,
            observed.as_str(),
            Some(&observed),
        ));
    }

    // 纯文本模式：强制丢弃 HTML，写 search_text。
    #[test]
    #[ignore = "touches the real system clipboard; run with --ignored on a desktop session"]
    fn plain_mode_strips_html() {
        let _serial = crate::clipboard::test_lock::serial();
        let (_dir, store) = temp_store();
        let fingerprints = ClipboardFingerprintState::new();

        let item = text_item(
            "<b>Hello</b> World",
            Some(ClipboardSubKind::Html),
            Some("Hello World"),
        );
        write_to_clipboard(&store, &fingerprints, &item, true).unwrap();

        let reader = ClipboardReader::new().unwrap();
        let payload = reader
            .read_with_capture(&crate::settings::Capture::default())
            .unwrap()
            .expect("should read");
        let read_item = build_item(&store, &payload).unwrap().unwrap();
        assert_eq!(read_item.kind, ClipboardKind::Text);
        assert_eq!(read_item.sub_kind, None);
        assert_eq!(read_item.content, "Hello World");
    }

    // 图片往返：即使系统重编码 PNG，解码后的像素指纹仍应命中回环抑制。
    #[test]
    #[ignore = "touches the real system clipboard; run with --ignored on a desktop session"]
    fn round_trip_image_matches_hash() {
        let _serial = crate::clipboard::test_lock::serial();
        let (_dir, store) = temp_store();
        let fingerprints = ClipboardFingerprintState::new();

        // 先落盘一张原图（模拟历史记录里的 image item）。
        let png = sample_png(48, 32);
        let stored = store
            .store(&ImagePayload {
                bytes: png,
                width: 48,
                height: 32,
            })
            .unwrap();
        let item = ClipboardItem {
            id: uuid::Uuid::new_v4().to_string(),
            kind: ClipboardKind::Image,
            sub_kind: None,
            group_id: None,
            source_app_id: None,
            source_revision: uuid::Uuid::new_v4().simple().to_string(),
            source_device_id: None,
            source_device_name: None,
            source_updated_at: None,
            source_device_is_local: false,
            content_hash: content_hash(ClipboardKind::Image, &stored.file_name),
            sync_key: content_hash(ClipboardKind::Image, &stored.file_name),
            content: stored.file_name.clone(),
            search_text: None,
            summary: None,
            text_char_count: None,
            file_types: None,
            size: Some(stored.size),
            width: Some(stored.width),
            height: Some(stored.height),
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
        };

        write_to_clipboard(&store, &fingerprints, &item, false).unwrap();

        let reader = ClipboardReader::new().unwrap();
        let payload = reader
            .read_with_capture(&crate::settings::Capture::default())
            .unwrap()
            .expect("should read image");
        let fingerprint = match &payload {
            crate::clipboard::ClipboardPayload::Image(image) => {
                ClipboardFingerprint::from_image_bytes(&image.bytes).unwrap()
            }
            _ => panic!("expected image payload"),
        };
        let read_item = build_item(&store, &payload).unwrap().unwrap();
        assert_eq!(read_item.kind, ClipboardKind::Image);
        let observation = fingerprints.begin_observation();
        assert!(fingerprints.consume_pending_writeback(
            &observation,
            fingerprint.as_str(),
            Some(&fingerprint),
        ));
    }

    fn sample_png(w: u32, h: u32) -> Vec<u8> {
        use std::io::Cursor;
        let buf = image::RgbaImage::from_pixel(w, h, image::Rgba([4, 5, 6, 255]));
        let mut out = Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(buf)
            .write_to(&mut out, image::ImageFormat::Png)
            .unwrap();
        out.into_inner()
    }

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("ecopaste-write-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }
}
