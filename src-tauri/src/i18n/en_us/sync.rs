use crate::i18n::keys::SyncKey as Key;

/// Returns user-visible sync status errors in American English.
pub fn label(key: Key) -> &'static str {
    match key {
        Key::OpenStream => "Timed out opening the cloud sync stream",
        Key::SendRequest => "Timed out sending the cloud sync request",
        Key::WaitForResponse => "Timed out waiting for the cloud sync response",
        Key::ReadResponse => "Timed out reading the cloud sync response",
    }
}
