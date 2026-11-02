use crate::i18n::keys::SyncKey as Key;

/// Returns user-visible sync status errors in American English.
pub fn label(key: Key) -> &'static str {
    match key {
        Key::OpenStream => "Timed out opening the cloud sync stream",
        Key::SendRequest => "Timed out sending the cloud sync request",
        Key::WaitForResponse => "Timed out waiting for the cloud sync response",
        Key::ReadResponse => "Timed out reading the cloud sync response",
        Key::OpenUpload => "Timed out opening the attachment upload stream",
        Key::SendUploadRequest => "Timed out sending the attachment upload request",
        Key::WaitForUpload => "Timed out waiting for attachment upload readiness",
        Key::ReadUploadFile => "Timed out reading the local attachment",
        Key::WriteUpload => "Attachment upload stopped making progress",
        Key::ConfirmUpload => "Timed out waiting for attachment confirmation",
    }
}
