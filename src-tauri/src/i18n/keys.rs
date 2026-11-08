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
    #[cfg(any(target_os = "android", test))]
    AndroidShareEmpty,
    #[cfg(target_os = "android")]
    AndroidShareFailed,
    #[cfg(any(target_os = "android", test))]
    AndroidShareResult,
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
    #[cfg(not(target_os = "android"))]
    RemoteFileSave,
    #[cfg(not(target_os = "android"))]
    RemoteFileSaveDirectory,
    #[cfg(not(target_os = "android"))]
    RemoteFileInvalidDestination,
    #[cfg(not(target_os = "android"))]
    RemoteFileInvalidDirectory,
    RemoteFileNotDownloaded,
    SyncDisabled,
    SyncGroupRequired,
    SyncLanDisabled,
    SyncOperationPending,
    SyncPreparationFailed,
    SyncSensitiveDisabled,
    SyncTransferCancelFailed,
}

#[derive(Debug, Clone, Copy)]
pub enum SyncKey {
    OpenStream,
    SendRequest,
    WaitForResponse,
    ReadResponse,
    OpenUpload,
    SendUploadRequest,
    WaitForUpload,
    ReadUploadFile,
    WriteUpload,
    ConfirmUpload,
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
