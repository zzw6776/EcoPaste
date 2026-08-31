use crate::i18n::keys::CommandKey as Key;

/// 返回美式英文 Tauri 命令错误根因文案。
pub fn label(key: Key) -> &'static str {
    match key {
        Key::AndroidDirectoryUnsupported => "Folders cannot be opened or saved as a copy yet",
        Key::AndroidFileMissing => "The file no longer exists",
        Key::AndroidFileOpenFailed => "The file could not be opened",
        Key::AndroidFileOpenUnavailable => "No app is available to open this file",
        Key::AndroidFileSaveFailed => "The file could not be saved",
        #[cfg(not(target_os = "android"))]
        Key::DragSourceFilesMissing => "The dragged source files no longer exist",
        #[cfg(not(target_os = "android"))]
        Key::DragImageMissing => "The image file no longer exists",
        #[cfg(not(target_os = "android"))]
        Key::DragTextEmpty => "Text content is empty",
        Key::ExternalUrlUnsupported => "Only links starting with http or https can be opened",
        #[cfg(target_os = "macos")]
        Key::PasteAccessibilityRequired => {
            "Allow EcoPaste in System Settings → Privacy & Security → Accessibility, then paste again"
        }
        Key::SaveImage => "Save Image",
    }
}
