use std::collections::HashSet;
use std::mem::{size_of, zeroed};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::anyhow;
use serde_json::json;
use tauri::{AppHandle, Emitter};
use winapi::shared::minwindef::{LPARAM, LRESULT, UINT, WPARAM};
use winapi::um::processthreadsapi::GetCurrentThreadId;
use winapi::um::winuser::{
    CallNextHookEx, GetAsyncKeyState, GetMessageW, PostThreadMessageW, SendInput,
    SetWindowsHookExW, UnhookWindowsHookEx, INPUT, INPUT_KEYBOARD, KBDLLHOOKSTRUCT, KEYBDINPUT,
    KEYEVENTF_EXTENDEDKEY, KEYEVENTF_KEYUP, KEYEVENTF_SCANCODE, LLKHF_EXTENDED, MSG, VK_BACK,
    VK_CONTROL, VK_DELETE, VK_DOWN, VK_ESCAPE, VK_LCONTROL, VK_LEFT, VK_LSHIFT, VK_LWIN, VK_MENU,
    VK_RCONTROL, VK_RETURN, VK_RIGHT, VK_RSHIFT, VK_RWIN, VK_SHIFT, VK_SPACE, VK_TAB, VK_UP,
    WH_KEYBOARD_LL, WM_KEYDOWN, WM_KEYUP, WM_QUIT, WM_SYSKEYDOWN, WM_SYSKEYUP,
};

use super::{NAV_EVENT, SEARCH_HANDOFF_EVENT};
use crate::core::Result;
use crate::keyboard_search_handoff::{PushResult, SearchHandoffBuffer};

static NAV_ENABLED: AtomicBool = AtomicBool::new(false);
static NEXT_HANDOFF_SESSION_ID: AtomicU64 = AtomicU64::new(1);
static HOOK_THREAD_ID: Mutex<Option<u32>> = Mutex::new(None);
static APP_HANDLE: OnceLock<AppHandle> = OnceLock::new();

const SEARCH_HANDOFF_TIMEOUT: Duration = Duration::from_millis(1_500);
const SEARCH_HANDOFF_BUFFER_CAPACITY: usize = 128;
const SEARCH_HANDOFF_INJECT_MARKER: usize = 0x4543_4F50;

#[derive(Clone, Copy)]
struct BufferedKeyEvent {
    extended: bool,
    key_up: bool,
    scan_code: u16,
    virtual_key: u16,
}

fn search_handoff() -> &'static Mutex<SearchHandoffBuffer<BufferedKeyEvent>> {
    static STATE: OnceLock<Mutex<SearchHandoffBuffer<BufferedKeyEvent>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(SearchHandoffBuffer::new(SEARCH_HANDOFF_BUFFER_CAPACITY)))
}

fn consumed_keys() -> &'static Mutex<HashSet<u32>> {
    static SET: OnceLock<Mutex<HashSet<u32>>> = OnceLock::new();
    SET.get_or_init(|| Mutex::new(HashSet::new()))
}

/// 仅放行当前前端需要的 Ctrl 快捷键：C、D、F、K、M、N、O、P、Q、T、Enter、Backspace、Delete、逗号与数字 0-9。
fn ctrl_shortcut_key(vk: u32) -> Option<String> {
    match vk as i32 {
        0x43 => Some("c".to_string()),
        0x44 => Some("d".to_string()),
        0x46 => Some("f".to_string()),
        0x4B => Some("k".to_string()),
        0x4D => Some("m".to_string()),
        0x4E => Some("n".to_string()),
        0x4F => Some("o".to_string()),
        0x50 => Some("p".to_string()),
        0x51 => Some("q".to_string()),
        0x54 => Some("t".to_string()),
        0xBC => Some(",".to_string()),
        0x30..=0x39 => Some(((vk as u8) as char).to_string()),
        VK_RETURN => Some("Enter".to_string()),
        VK_BACK => Some("Backspace".to_string()),
        VK_DELETE => Some("Delete".to_string()),
        _ => None,
    }
}

fn nav_key(vk: u32) -> Option<&'static str> {
    match vk as i32 {
        VK_LEFT => Some("ArrowLeft"),
        VK_RIGHT => Some("ArrowRight"),
        VK_UP => Some("ArrowUp"),
        VK_DOWN => Some("ArrowDown"),
        VK_RETURN => Some("Enter"),
        VK_ESCAPE => Some("Escape"),
        VK_TAB => Some("Tab"),
        _ => None,
    }
}

/// 预览按键需要 keydown / keyup 配对发送，供前端按住显示、松开关闭。
fn preview_key(vk: u32) -> Option<&'static str> {
    match vk as i32 {
        VK_SPACE => Some(" "),
        _ => None,
    }
}

pub fn enable_navigation_keys(app: &AppHandle) {
    let _ = APP_HANDLE.set(app.clone());
    NAV_ENABLED.store(true, Ordering::Release);

    // 已有钩子线程时不再起新线程；NAV_ENABLED 的恢复就够了。
    if HOOK_THREAD_ID
        .lock()
        .expect("hook thread id poisoned")
        .is_some()
    {
        return;
    }

    std::thread::spawn(|| unsafe {
        let hook = SetWindowsHookExW(WH_KEYBOARD_LL, Some(hook_proc), null_mut(), 0);
        if hook.is_null() {
            log::error!("SetWindowsHookExW failed");
            return;
        }

        *HOOK_THREAD_ID.lock().expect("hook thread id poisoned") = Some(GetCurrentThreadId());

        let mut msg: MSG = std::mem::zeroed();
        // GetMessageW 收到 WM_QUIT 返回 0 → 消息泵自然退出。
        while GetMessageW(&mut msg, null_mut(), 0, 0) > 0 {}

        UnhookWindowsHookEx(hook);
        *HOOK_THREAD_ID.lock().expect("hook thread id poisoned") = None;
        consumed_keys()
            .lock()
            .expect("consumed keys poisoned")
            .clear();
    });
}

pub fn disable_navigation_keys() {
    suspend_navigation_keys();
    cancel_search_handoff(None);

    let tid = HOOK_THREAD_ID
        .lock()
        .expect("hook thread id poisoned")
        .take();
    if let Some(tid) = tid {
        unsafe {
            PostThreadMessageW(tid, WM_QUIT, 0, 0);
        }
    }
}

/// 编辑输入期间仅暂停键盘拦截，保留钩子线程以便退出编辑后立即恢复。
pub fn suspend_navigation_keys() {
    NAV_ENABLED.store(false, Ordering::Release);
    clear_suspended_navigation_state();
}

pub fn is_search_handoff_active(session_id: u64) -> bool {
    search_handoff()
        .lock()
        .expect("search handoff poisoned")
        .is_active(session_id)
}

/// 取消指定交接；`None` 用于窗口隐藏等无条件清理路径。
pub fn cancel_search_handoff(session_id: Option<u64>) -> bool {
    search_handoff()
        .lock()
        .expect("search handoff poisoned")
        .cancel(session_id)
}

/// 在持有交接状态锁时同步暂停钩子，避免取消后尚未恢复窗口期间启动新会话。
pub fn cancel_search_handoff_and_suspend(session_id: Option<u64>) -> bool {
    let canceled = {
        let mut guard = search_handoff().lock().expect("search handoff poisoned");
        let canceled = guard.cancel(session_id);
        if canceled {
            NAV_ENABLED.store(false, Ordering::Release);
        }
        canceled
    };

    if canceled {
        clear_suspended_navigation_state();
    }
    canceled
}

/// 焦点确认后停止全局拦截，并把交接期间的物理按键按原顺序重放给 WebView。
pub fn confirm_search_handoff(session_id: u64) -> Result<bool> {
    let events = {
        let mut guard = search_handoff().lock().expect("search handoff poisoned");
        let Some(events) = guard.take(session_id) else {
            return Ok(false);
        };

        NAV_ENABLED.store(false, Ordering::Release);
        events
    };

    clear_suspended_navigation_state();
    replay_buffered_events(&events)?;
    Ok(true)
}

unsafe extern "system" fn hook_proc(code: i32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    if code < 0 || !NAV_ENABLED.load(Ordering::Acquire) {
        return CallNextHookEx(null_mut(), code, wparam, lparam);
    }

    let kbd = &*(lparam as *const KBDLLHOOKSTRUCT);
    if kbd.dwExtraInfo == SEARCH_HANDOFF_INJECT_MARKER {
        return CallNextHookEx(null_mut(), code, wparam, lparam);
    }

    let vk = kbd.vkCode;
    let msg = wparam as UINT;

    if is_key_message(msg) && buffer_active_handoff(kbd, msg) {
        return 1;
    }

    // 确认或取消线程可能刚在交接锁内暂停钩子；此时当前按键应直接进入已聚焦的 WebView，
    // 不能继续走文本意图判断并创建第二个会话。
    if !NAV_ENABLED.load(Ordering::Acquire) {
        return CallNextHookEx(null_mut(), code, wparam, lparam);
    }

    let ctrl_down = (GetAsyncKeyState(VK_CONTROL) as u16) & 0x8000 != 0;

    let is_ctrl = matches!(vk as i32, VK_CONTROL | VK_LCONTROL | VK_RCONTROL);

    if is_ctrl {
        if let Some(app) = APP_HANDLE.get() {
            let event_type = if msg == WM_KEYDOWN || msg == WM_SYSKEYDOWN {
                Some("keydown")
            } else if msg == WM_KEYUP || msg == WM_SYSKEYUP {
                Some("keyup")
            } else {
                None
            };

            if let Some(event_type) = event_type {
                if let Err(err) = app.emit(
                    NAV_EVENT,
                    json!({ "type": event_type, "key": "Control", "ctrlKey": event_type == "keydown" }),
                ) {
                    log::warn!("emit nav event failed: {err:?}");
                }
            }
        }

        // Ctrl 状态只用于前端展示与组合键识别，不在此处吞键，避免影响系统行为。
        return CallNextHookEx(null_mut(), code, wparam, lparam);
    }

    let nav_key = nav_key(vk);
    let preview_key = preview_key(vk);
    let shortcut_key = if ctrl_down {
        ctrl_shortcut_key(vk)
    } else {
        None
    };

    if msg == WM_KEYDOWN || msg == WM_SYSKEYDOWN {
        if let Some(shortcut_key) = shortcut_key {
            if let Some(app) = APP_HANDLE.get() {
                if let Err(err) = app.emit(
                    NAV_EVENT,
                    json!({ "type": "keydown", "key": shortcut_key, "ctrlKey": true }),
                ) {
                    log::warn!("emit nav event failed: {err:?}");
                }
            }

            consumed_keys()
                .lock()
                .expect("consumed keys poisoned")
                .insert(vk);
            return 1;
        }

        if let Some(preview_key) = preview_key {
            let mut consumed = consumed_keys().lock().expect("consumed keys poisoned");
            if consumed.contains(&vk) {
                return 1;
            }

            consumed.insert(vk);
            drop(consumed);

            if let Some(app) = APP_HANDLE.get() {
                if let Err(err) = app.emit(
                    NAV_EVENT,
                    json!({ "type": "keydown", "key": preview_key, "code": "Space" }),
                ) {
                    log::warn!("emit nav event failed: {err:?}");
                }
            }

            return 1;
        }

        if let Some(nav_key) = nav_key {
            if let Some(app) = APP_HANDLE.get() {
                let shift_down = if nav_key == "Tab" {
                    (GetAsyncKeyState(VK_SHIFT) as u16) & 0x8000 != 0
                } else {
                    false
                };

                if let Err(err) = app.emit(
                    NAV_EVENT,
                    json!({ "type": "keydown", "key": nav_key, "shiftKey": shift_down }),
                ) {
                    log::warn!("emit nav event failed: {err:?}");
                }
            }
            // 记下 KEYDOWN 的 VK，配对的 KEYUP 也要吞——
            // 否则背后被聚焦的应用会收到孤立 KEYUP，造成奇怪行为。
            consumed_keys()
                .lock()
                .expect("consumed keys poisoned")
                .insert(vk);
            return 1;
        }

        if is_text_input_intent(vk) && begin_search_handoff(kbd, msg) {
            return 1;
        }
    } else if msg == WM_KEYUP || msg == WM_SYSKEYUP {
        if let Some(preview_key) = preview_key {
            if consumed_keys()
                .lock()
                .expect("consumed keys poisoned")
                .remove(&vk)
            {
                if let Some(app) = APP_HANDLE.get() {
                    if let Err(err) = app.emit(
                        NAV_EVENT,
                        json!({ "type": "keyup", "key": preview_key, "code": "Space" }),
                    ) {
                        log::warn!("emit nav event failed: {err:?}");
                    }
                }

                return 1;
            }
        }

        if consumed_keys()
            .lock()
            .expect("consumed keys poisoned")
            .remove(&vk)
        {
            return 1;
        }
    }

    CallNextHookEx(null_mut(), code, wparam, lparam)
}

fn is_key_message(message: UINT) -> bool {
    matches!(message, WM_KEYDOWN | WM_SYSKEYDOWN | WM_KEYUP | WM_SYSKEYUP)
}

fn clear_suspended_navigation_state() {
    consumed_keys()
        .lock()
        .expect("consumed keys poisoned")
        .clear();

    // 暂停后真实 Ctrl keyup 不再进入列表键盘链路，主动清掉前端修饰键镜像。
    if let Some(app) = APP_HANDLE.get() {
        if let Err(err) = app.emit(
            NAV_EVENT,
            json!({ "type": "keyup", "key": "Control", "ctrlKey": false }),
        ) {
            log::warn!("emit nav event failed: {err:?}");
        }
    }
}

fn is_text_input_intent(virtual_key: u32) -> bool {
    let ctrl_down = unsafe { (GetAsyncKeyState(VK_CONTROL) as u16) & 0x8000 != 0 };
    let alt_down = unsafe { (GetAsyncKeyState(VK_MENU) as u16) & 0x8000 != 0 };
    let win_down = unsafe {
        (GetAsyncKeyState(VK_LWIN) as u16) & 0x8000 != 0
            || (GetAsyncKeyState(VK_RWIN) as u16) & 0x8000 != 0
    };
    if ctrl_down || alt_down || win_down {
        return false;
    }

    matches!(
        virtual_key,
        0x30..=0x5A
            | 0x60..=0x6F
            | 0xBA..=0xC0
            | 0xDB..=0xDF
            | 0xE2
            | 0xE5
            | 0xE7
    )
}

fn begin_search_handoff(kbd: &KBDLLHOOKSTRUCT, message: UINT) -> bool {
    let session_id = NEXT_HANDOFF_SESSION_ID.fetch_add(1, Ordering::Relaxed);
    let pressed_shifts = pressed_shift_events();
    let mut events = pressed_shifts.clone();
    events.push(buffered_key_event(kbd, message));

    {
        let mut guard = search_handoff().lock().expect("search handoff poisoned");
        if !guard.begin(session_id, events) {
            return true;
        }
    }

    let Some(app) = APP_HANDLE.get() else {
        cancel_search_handoff(Some(session_id));
        return false;
    };
    if let Err(err) = release_shift_from_previous_target(&pressed_shifts) {
        log::warn!("release shift from previous target failed: {err}");
        restore_shift_to_previous_target(&pressed_shifts);
        cancel_search_handoff(Some(session_id));
        return false;
    }
    if let Err(err) = app.emit(SEARCH_HANDOFF_EVENT, json!({ "sessionId": session_id })) {
        log::warn!("emit search handoff event failed: {err:?}");
        restore_shift_to_previous_target(&pressed_shifts);
        cancel_search_handoff(Some(session_id));
        return false;
    }

    let timeout_app = app.clone();
    std::thread::spawn(move || {
        std::thread::sleep(SEARCH_HANDOFF_TIMEOUT);
        if !is_search_handoff_active(session_id) {
            return;
        }

        log::warn!("search handoff timed out: session={session_id}");
        if let Err(err) =
            crate::window::cancel_clipboard_search_handoff(&timeout_app, Some(session_id))
        {
            log::warn!("cancel timed out search handoff failed: {err}");
        }
    });

    true
}

fn buffer_active_handoff(kbd: &KBDLLHOOKSTRUCT, message: UINT) -> bool {
    let result = search_handoff()
        .lock()
        .expect("search handoff poisoned")
        .push(buffered_key_event(kbd, message));
    let overflowed_session = match result {
        PushResult::Inactive => return false,
        PushResult::Overflowed(session_id) => Some(session_id),
        PushResult::Buffered | PushResult::OverflowPending => None,
    };

    if let (Some(session_id), Some(app)) = (overflowed_session, APP_HANDLE.get()) {
        let overflow_app = app.clone();
        std::thread::spawn(move || {
            log::warn!("search handoff buffer capacity exceeded: session={session_id}");
            if let Err(err) = crate::window::cancel_clipboard_search_handoff(&overflow_app, None) {
                log::warn!("cancel overflowing search handoff failed: {err}");
            }
        });
    }

    true
}

fn buffered_key_event(kbd: &KBDLLHOOKSTRUCT, message: UINT) -> BufferedKeyEvent {
    BufferedKeyEvent {
        extended: kbd.flags & LLKHF_EXTENDED != 0,
        key_up: message == WM_KEYUP || message == WM_SYSKEYUP,
        scan_code: kbd.scanCode as u16,
        virtual_key: kbd.vkCode as u16,
    }
}

fn pressed_shift_events() -> Vec<BufferedKeyEvent> {
    [VK_LSHIFT, VK_RSHIFT]
        .into_iter()
        .filter(|virtual_key| unsafe { (GetAsyncKeyState(*virtual_key) as u16) & 0x8000 != 0 })
        .map(|virtual_key| BufferedKeyEvent {
            extended: false,
            key_up: false,
            scan_code: 0,
            virtual_key: virtual_key as u16,
        })
        .collect()
}

fn release_shift_from_previous_target(pressed_shifts: &[BufferedKeyEvent]) -> Result<()> {
    let shift_ups = pressed_shifts
        .iter()
        .map(|event| BufferedKeyEvent {
            key_up: true,
            ..*event
        })
        .collect::<Vec<_>>();

    replay_buffered_events(&shift_ups)
}

fn restore_shift_to_previous_target(pressed_shifts: &[BufferedKeyEvent]) {
    if let Err(err) = replay_buffered_events(pressed_shifts) {
        log::warn!("restore shift to previous target failed: {err}");
    }
}

fn replay_buffered_events(events: &[BufferedKeyEvent]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut inputs = events
        .iter()
        .map(|event| {
            let mut input: INPUT = unsafe { zeroed() };
            input.type_ = INPUT_KEYBOARD;

            let mut flags = if event.scan_code == 0 {
                0
            } else {
                KEYEVENTF_SCANCODE
            };
            if event.extended {
                flags |= KEYEVENTF_EXTENDEDKEY;
            }
            if event.key_up {
                flags |= KEYEVENTF_KEYUP;
            }

            unsafe {
                *input.u.ki_mut() = KEYBDINPUT {
                    wVk: if event.scan_code == 0 {
                        event.virtual_key
                    } else {
                        0
                    },
                    wScan: event.scan_code,
                    dwFlags: flags,
                    time: 0,
                    dwExtraInfo: SEARCH_HANDOFF_INJECT_MARKER,
                };
            }

            input
        })
        .collect::<Vec<_>>();

    let sent = unsafe {
        SendInput(
            inputs.len() as u32,
            inputs.as_mut_ptr(),
            size_of::<INPUT>() as i32,
        )
    };
    if sent as usize != inputs.len() {
        return Err(anyhow!(
            "SendInput replayed {sent}/{} search handoff events",
            inputs.len()
        )
        .into());
    }

    Ok(())
}
