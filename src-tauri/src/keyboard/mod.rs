//! 剪贴板窗口 focusable=false 后，Windows 上 WebView 收不到键盘事件，
//! 需要装 OS 级低级钩子捕获导航键再 emit 给前端。
//! 与 `keystroke/`（向外注入按键模拟粘贴）方向相反：本模块是向内捕获。
//! macOS 走 NSPanel 自己接管键盘事件，无需本模块——故仅 windows target 启用。

pub const NAV_EVENT: &str = "keyboard://nav";
pub const SEARCH_HANDOFF_EVENT: &str = "keyboard://search-handoff";

mod windows;
pub use windows::{
    cancel_search_handoff, cancel_search_handoff_and_suspend, confirm_search_handoff,
    disable_navigation_keys, enable_navigation_keys, is_search_handoff_active,
    suspend_navigation_keys,
};
