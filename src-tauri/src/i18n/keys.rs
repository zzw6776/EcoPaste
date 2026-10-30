#[cfg(not(target_os = "android"))]
#[derive(Debug, Clone, Copy)]
pub enum ClipboardMenuKey {
    Paste,
    PasteAsPlainText,
    PasteAsPath,
    Copy,
    SaveImage,
    OpenLink,
    SendEmail,
    RevealInFinder,
    RevealInExplorer,
    Favorite,
    Unfavorite,
    PinItem,
    UnpinItem,
    MoveToGroup,
    AddNote,
    EditNote,
    Delete,
}

#[derive(Debug, Clone, Copy)]
pub enum CommandKey {
    AndroidDirectoryUnsupported,
    AndroidFileMissing,
    AndroidFileOpenFailed,
    AndroidFileOpenUnavailable,
    AndroidFileSaveFailed,
    #[cfg(not(target_os = "android"))]
    DragSourceFilesMissing,
    #[cfg(not(target_os = "android"))]
    DragImageMissing,
    #[cfg(not(target_os = "android"))]
    DragTextEmpty,
    #[cfg(not(target_os = "android"))]
    UpdateInstallRequiresRelease,
    ExternalUrlUnsupported,
    #[cfg(target_os = "macos")]
    PasteAccessibilityRequired,
    #[cfg(target_os = "macos")]
    PasteTargetUnavailable,
    SaveImage,
    SyncDisabled,
    SyncGroupRequired,
    SyncLanDisabled,
    SyncOperationPending,
    SyncSensitiveDisabled,
}

#[derive(Debug, Clone, Copy)]
pub enum SyncKey {
    OpenStream,
    SendRequest,
    WaitForResponse,
    ReadResponse,
}

#[cfg(not(target_os = "android"))]
#[derive(Debug, Clone, Copy)]
pub enum TrayKey {
    Preference,
    StartListening,
    StopListening,
    OpenSourceAddress,
    CheckForUpdates,
    Version,
    Relaunch,
    Exit,
}
