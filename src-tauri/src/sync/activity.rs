//! 只在实际同步工作存活期间防止 macOS 自动休眠；错误、取消和正常完成都释放断言。
#[derive(Default)]
pub(super) struct SyncActivityGuard {
    #[cfg(target_os = "macos")]
    assertion: Option<u32>,
}

impl SyncActivityGuard {
    pub(super) fn acquire() -> Self {
        #[cfg(target_os = "macos")]
        {
            use objc2_foundation::NSString;
            let kind = NSString::from_str("PreventUserIdleSystemSleep");
            let reason = NSString::from_str("EcoPaste synchronization in progress");
            let mut assertion = 0;
            // NSString 与 CFString 桥接；两个字符串在系统复制参数前保持存活。
            let result = unsafe {
                IOPMAssertionCreateWithName(
                    (&*kind as *const NSString).cast(),
                    255,
                    (&*reason as *const NSString).cast(),
                    &mut assertion,
                )
            };
            if result == 0 {
                return Self {
                    assertion: Some(assertion),
                };
            }
            log::warn!("could not prevent idle sleep during sync: {result}");
        }
        Self::default()
    }
}

impl Drop for SyncActivityGuard {
    fn drop(&mut self) {
        #[cfg(target_os = "macos")]
        if let Some(assertion) = self.assertion.take() {
            let result = unsafe { IOPMAssertionRelease(assertion) };
            if result != 0 {
                log::warn!("could not release sync power assertion: {result}");
            }
        }
    }
}

#[cfg(target_os = "macos")]
#[link(name = "IOKit", kind = "framework")]
unsafe extern "C" {
    fn IOPMAssertionCreateWithName(
        assertion_type: *const std::ffi::c_void,
        level: u32,
        name: *const std::ffi::c_void,
        assertion_id: *mut u32,
    ) -> i32;
    fn IOPMAssertionRelease(assertion_id: u32) -> i32;
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    #[link(name = "IOKit", kind = "framework")]
    unsafe extern "C" {
        fn IOPMAssertionCopyProperties(assertion: u32) -> *const std::ffi::c_void;
    }
    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFRelease(value: *const std::ffi::c_void);
    }

    fn exists(assertion: u32) -> bool {
        let properties = unsafe { IOPMAssertionCopyProperties(assertion) };
        if properties.is_null() {
            return false;
        }
        unsafe {
            CFRelease(properties);
        }
        true
    }

    #[test]
    fn active_transfers_hold_independent_assertions_and_release_on_drop() {
        let first = SyncActivityGuard::acquire();
        let second = SyncActivityGuard::acquire();
        let first_id = first
            .assertion
            .expect("macOS should permit an idle sleep assertion");
        let second_id = second.assertion.unwrap();
        assert!(exists(first_id));
        assert!(exists(second_id));
        drop(first);
        assert!(!exists(first_id));
        assert!(exists(second_id));
        drop(second);
        assert!(!exists(second_id));
    }
}
