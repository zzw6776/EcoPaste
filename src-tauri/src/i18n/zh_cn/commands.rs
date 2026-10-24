use crate::i18n::keys::CommandKey as Key;

/// 返回简体中文 Tauri 命令错误根因文案。
pub fn label(key: Key) -> &'static str {
    match key {
        Key::AndroidDirectoryUnsupported => "文件夹暂不支持直接打开或另存为",
        Key::AndroidFileMissing => "文件已不存在",
        Key::AndroidFileOpenFailed => "无法打开文件",
        Key::AndroidFileOpenUnavailable => "没有可用于打开该文件的应用",
        Key::AndroidFileSaveFailed => "无法保存文件",
        #[cfg(not(target_os = "android"))]
        Key::DragSourceFilesMissing => "拖拽源文件已不存在",
        #[cfg(not(target_os = "android"))]
        Key::DragImageMissing => "图片文件已不存在",
        #[cfg(not(target_os = "android"))]
        Key::DragTextEmpty => "文本内容为空",
        #[cfg(not(target_os = "android"))]
        Key::UpdateInstallRequiresRelease => {
            "开发构建仅支持检查和下载更新，请在已安装的正式版中安装更新"
        }
        Key::ExternalUrlUnsupported => "只能打开 http 或 https 开头的链接",
        #[cfg(target_os = "macos")]
        Key::PasteAccessibilityRequired => {
            "请在“系统设置 → 隐私与安全性 → 辅助功能”中允许 EcoPaste，然后重新粘贴"
        }
        #[cfg(target_os = "macos")]
        Key::PasteTargetUnavailable => "无法恢复原输入窗口，请重新唤起后再试",
        Key::SaveImage => "保存图片",
        Key::SyncDisabled => "请先启用多端同步",
        Key::SyncGroupRequired => "请先创建或加入同步设备组",
        Key::SyncLanDisabled => "局域网同步已关闭",
        Key::SyncOperationPending => "此记录仍在等待前序同步操作，请稍后重试",
        Key::SyncSensitiveDisabled => "敏感内容同步已关闭",
    }
}
