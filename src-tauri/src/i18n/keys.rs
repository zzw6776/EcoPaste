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
    ExternalUrlUnsupported,
    SaveImage,
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
