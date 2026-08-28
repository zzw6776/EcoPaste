use crate::i18n::keys::CommandKey as Key;

/// 返回简体中文 Tauri 命令错误根因文案。
pub fn label(key: Key) -> &'static str {
    match key {
        Key::AndroidDirectoryUnsupported => "文件夹暂不支持直接打开或另存为",
        Key::AndroidFileMissing => "文件已不存在",
        Key::AndroidFileOpenFailed => "无法打开文件",
        Key::AndroidFileOpenUnavailable => "没有可用于打开该文件的应用",
        Key::AndroidFileSaveFailed => "无法保存文件",
        Key::DragSourceFilesMissing => "拖拽源文件已不存在",
        Key::DragImageMissing => "图片文件已不存在",
        Key::DragTextEmpty => "文本内容为空",
        Key::ExternalUrlUnsupported => "只能打开 http 或 https 开头的链接",
    }
}
