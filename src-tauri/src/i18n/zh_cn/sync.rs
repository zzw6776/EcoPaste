use crate::i18n::keys::SyncKey as Key;

/// 返回简体中文同步状态错误文案。
pub fn label(key: Key) -> &'static str {
    match key {
        Key::OpenStream => "打开云端同步流超时",
        Key::SendRequest => "发送云端同步请求超时",
        Key::WaitForResponse => "等待云端同步响应超时",
        Key::ReadResponse => "读取云端同步响应超时",
    }
}
