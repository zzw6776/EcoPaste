use crate::i18n::keys::SyncKey as Key;

/// 返回简体中文同步状态错误文案。
pub fn label(key: Key) -> &'static str {
    match key {
        Key::OpenStream => "打开云端同步流超时",
        Key::SendRequest => "发送云端同步请求超时",
        Key::WaitForResponse => "等待云端同步响应超时",
        Key::ReadResponse => "读取云端同步响应超时",
        Key::OpenUpload => "打开附件上传连接超时",
        Key::SendUploadRequest => "发送附件上传请求超时",
        Key::WaitForUpload => "等待服务端允许上传附件超时",
        Key::ReadUploadFile => "读取本地附件超时",
        Key::WriteUpload => "附件上传长时间没有进展",
        Key::ConfirmUpload => "等待服务端确认附件超时",
    }
}
