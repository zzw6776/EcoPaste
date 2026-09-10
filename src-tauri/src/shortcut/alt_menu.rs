//! Windows 专属：避免含 Alt 的全局快捷键被前台应用误判为单独按下 Alt。

use std::mem::{size_of, zeroed};
use std::ptr::null_mut;
use std::sync::{Mutex, OnceLock};

use tauri_plugin_global_shortcut::{Code, Modifiers, Shortcut};
use winapi::shared::minwindef::{LPARAM, LRESULT, UINT, WPARAM};
use winapi::um::winuser::{
    CallNextHookEx, GetAsyncKeyState, GetMessageW, SendInput, SetWindowsHookExW, UnhookWindowsHookEx,
    INPUT, INPUT_KEYBOARD, KBDLLHOOKSTRUCT, KEYBDINPUT, KEYEVENTF_KEYUP, LLKHF_INJECTED, MSG,
    VK_CONTROL, VK_LWIN, VK_MENU, VK_RWIN, VK_SHIFT, WH_KEYBOARD_LL, WM_KEYDOWN, WM_SYSKEYDOWN,
};

/// 未分配的虚拟键；在 Alt 仍按住时注入它，打断 Windows 的“单独按 Alt”菜单语义。
const VK_MENU_MASK: u16 = 0xE8;

#[derive(Clone, Copy)]
struct AltShortcut {
    ctrl: bool,
    key: u32,
    shift: bool,
    super_key: bool,
}

fn active_shortcuts() -> &'static Mutex<Vec<AltShortcut>> {
    static SHORTCUTS: OnceLock<Mutex<Vec<AltShortcut>>> = OnceLock::new();
    SHORTCUTS.get_or_init(|| Mutex::new(Vec::new()))
}

/// 同步当前成功注册的 Alt 快捷键，并按需启动低级键盘钩子。
pub fn set_shortcuts<'a>(shortcuts: impl IntoIterator<Item = &'a Shortcut>) {
    let next = shortcuts
        .into_iter()
        .filter_map(AltShortcut::from_shortcut)
        .collect::<Vec<_>>();
    let should_start = !next.is_empty();
    *active_shortcuts()
        .lock()
        .expect("alt shortcut state poisoned") = next;

    if should_start {
        ensure_hook();
    }
}

impl AltShortcut {
    fn from_shortcut(shortcut: &Shortcut) -> Option<Self> {
        if !shortcut.mods.contains(Modifiers::ALT) {
            return None;
        }

        Some(Self {
            ctrl: shortcut.mods.contains(Modifiers::CONTROL),
            key: virtual_key(shortcut.key)?,
            shift: shortcut.mods.contains(Modifiers::SHIFT),
            super_key: shortcut
                .mods
                .intersects(Modifiers::SUPER | Modifiers::META),
        })
    }

    fn matches(self, key: u32) -> bool {
        self.key == key
            && self.ctrl == key_down(VK_CONTROL)
            && self.shift == key_down(VK_SHIFT)
            && self.super_key == (key_down(VK_LWIN) || key_down(VK_RWIN))
    }
}

fn ensure_hook() {
    static HOOK_STARTED: OnceLock<()> = OnceLock::new();
    HOOK_STARTED.get_or_init(|| {
        std::thread::spawn(|| unsafe {
            let hook = SetWindowsHookExW(WH_KEYBOARD_LL, Some(hook_proc), null_mut(), 0);
            if hook.is_null() {
                log::error!("SetWindowsHookExW(WH_KEYBOARD_LL) for Alt shortcuts failed");
                return;
            }

            let mut msg: MSG = zeroed();
            while GetMessageW(&mut msg, null_mut(), 0, 0) > 0 {}

            UnhookWindowsHookEx(hook);
        });
    });
}

unsafe extern "system" fn hook_proc(code: i32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    if code < 0 {
        return CallNextHookEx(null_mut(), code, wparam, lparam);
    }

    let message = wparam as UINT;
    if message != WM_KEYDOWN && message != WM_SYSKEYDOWN {
        return CallNextHookEx(null_mut(), code, wparam, lparam);
    }

    let keyboard = &*(lparam as *const KBDLLHOOKSTRUCT);
    if keyboard.flags & LLKHF_INJECTED != 0 || !key_down(VK_MENU) {
        return CallNextHookEx(null_mut(), code, wparam, lparam);
    }

    let matched = active_shortcuts()
        .lock()
        .expect("alt shortcut state poisoned")
        .iter()
        .any(|shortcut| shortcut.matches(keyboard.vkCode));
    if matched {
        suppress_alt_menu();
    }

    CallNextHookEx(null_mut(), code, wparam, lparam)
}

fn key_down(key: i32) -> bool {
    unsafe { (GetAsyncKeyState(key) as u16) & 0x8000 != 0 }
}

/// 在主键进入前台应用前插入无功能组合键，使随后的 Alt 松开不再触发菜单焦点。
fn suppress_alt_menu() {
    let mut inputs: [INPUT; 2] = unsafe { zeroed() };
    inputs[0].type_ = INPUT_KEYBOARD;
    inputs[1].type_ = INPUT_KEYBOARD;

    unsafe {
        *inputs[0].u.ki_mut() = KEYBDINPUT {
            wVk: VK_MENU_MASK,
            wScan: 0,
            dwFlags: 0,
            time: 0,
            dwExtraInfo: 0,
        };
        *inputs[1].u.ki_mut() = KEYBDINPUT {
            wVk: VK_MENU_MASK,
            wScan: 0,
            dwFlags: KEYEVENTF_KEYUP,
            time: 0,
            dwExtraInfo: 0,
        };
    }

    let sent = unsafe {
        SendInput(
            inputs.len() as u32,
            inputs.as_mut_ptr(),
            size_of::<INPUT>() as i32,
        )
    };
    if sent as usize != inputs.len() {
        log::warn!(
            "inject Alt menu mask key sent {sent}/{}",
            inputs.len()
        );
    }
}

fn virtual_key(code: Code) -> Option<u32> {
    Some(match code {
        Code::KeyA
        | Code::KeyB
        | Code::KeyC
        | Code::KeyD
        | Code::KeyE
        | Code::KeyF
        | Code::KeyG
        | Code::KeyH
        | Code::KeyI
        | Code::KeyJ
        | Code::KeyK
        | Code::KeyL
        | Code::KeyM
        | Code::KeyN
        | Code::KeyO
        | Code::KeyP
        | Code::KeyQ
        | Code::KeyR
        | Code::KeyS
        | Code::KeyT
        | Code::KeyU
        | Code::KeyV
        | Code::KeyW
        | Code::KeyX
        | Code::KeyY
        | Code::KeyZ => 0x41 + (code as u32 - Code::KeyA as u32),
        Code::Digit0
        | Code::Digit1
        | Code::Digit2
        | Code::Digit3
        | Code::Digit4
        | Code::Digit5
        | Code::Digit6
        | Code::Digit7
        | Code::Digit8
        | Code::Digit9 => 0x30 + (code as u32 - Code::Digit0 as u32),
        Code::F1
        | Code::F2
        | Code::F3
        | Code::F4
        | Code::F5
        | Code::F6
        | Code::F7
        | Code::F8
        | Code::F9
        | Code::F10
        | Code::F11
        | Code::F12 => 0x70 + (code as u32 - Code::F1 as u32),
        Code::Backquote => 0xC0,
        Code::Backslash => 0xDC,
        Code::Backspace => 0x08,
        Code::BracketLeft => 0xDB,
        Code::BracketRight => 0xDD,
        Code::Comma => 0xBC,
        Code::Delete => 0x2E,
        Code::End => 0x23,
        Code::Enter => 0x0D,
        Code::Equal => 0xBB,
        Code::Escape => 0x1B,
        Code::Home => 0x24,
        Code::Insert => 0x2D,
        Code::Minus => 0xBD,
        Code::PageDown => 0x22,
        Code::PageUp => 0x21,
        Code::Period => 0xBE,
        Code::Quote => 0xDE,
        Code::Semicolon => 0xBA,
        Code::Slash => 0xBF,
        Code::Space => 0x20,
        Code::Tab => 0x09,
        Code::ArrowDown => 0x28,
        Code::ArrowLeft => 0x25,
        Code::ArrowRight => 0x27,
        Code::ArrowUp => 0x26,
        _ => return None,
    })
}
