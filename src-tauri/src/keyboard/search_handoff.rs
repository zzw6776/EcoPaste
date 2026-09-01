pub struct SearchHandoffBuffer<T> {
    active: Option<SearchHandoff<T>>,
    capacity: usize,
}

struct SearchHandoff<T> {
    events: Vec<T>,
    overflowed: bool,
    session_id: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Buffered,
    Inactive,
    Overflowed(u64),
    OverflowPending,
}

const LLKHF_INJECTED_FLAG: u32 = 0x10;
const VK_PROCESSKEY_CODE: u32 = 0xE5;
const VK_PACKET_CODE: u32 = 0xE7;

/// 交接只重放真实物理键；IME 派生键或其它注入键会在新焦点下由系统重新生成。
pub fn should_buffer_physical_key(flags: u32, scan_code: u32, virtual_key: u32) -> bool {
    flags & LLKHF_INJECTED_FLAG == 0
        && scan_code != 0
        && !matches!(virtual_key, VK_PROCESSKEY_CODE | VK_PACKET_CODE)
}

impl<T> SearchHandoffBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            active: None,
            capacity,
        }
    }

    pub fn begin(&mut self, session_id: u64, events: Vec<T>) -> bool {
        if self.active.is_some() || events.len() > self.capacity {
            return false;
        }

        self.active = Some(SearchHandoff {
            events,
            overflowed: false,
            session_id,
        });
        true
    }

    pub fn is_active(&self, session_id: u64) -> bool {
        self.active
            .as_ref()
            .is_some_and(|handoff| handoff.session_id == session_id)
    }

    pub fn has_active(&self) -> bool {
        self.active.is_some()
    }

    pub fn push(&mut self, event: T) -> PushResult {
        let Some(handoff) = self.active.as_mut() else {
            return PushResult::Inactive;
        };
        if handoff.overflowed {
            return PushResult::OverflowPending;
        }
        if handoff.events.len() >= self.capacity {
            handoff.overflowed = true;
            return PushResult::Overflowed(handoff.session_id);
        }

        handoff.events.push(event);
        PushResult::Buffered
    }

    pub fn take(&mut self, session_id: u64) -> Option<Vec<T>> {
        if !self.is_active(session_id) {
            return None;
        }

        self.active.take().map(|handoff| handoff.events)
    }

    pub fn cancel(&mut self, session_id: Option<u64>) -> bool {
        if let Some(expected) = session_id {
            if !self.is_active(expected) {
                return false;
            }
        }

        self.active.take().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::{PushResult, SearchHandoffBuffer};

    #[test]
    fn normal_session_preserves_event_order() {
        let mut buffer = SearchHandoffBuffer::new(4);

        assert!(buffer.begin(7, vec!["down"]));
        assert!(buffer.has_active());
        assert_eq!(buffer.push("repeat"), PushResult::Buffered);
        assert_eq!(buffer.push("up"), PushResult::Buffered);
        assert_eq!(buffer.take(7), Some(vec!["down", "repeat", "up"]));
        assert!(!buffer.has_active());
        assert!(!buffer.is_active(7));
    }

    #[test]
    fn duplicate_and_stale_sessions_cannot_replace_or_take_active_session() {
        let mut buffer = SearchHandoffBuffer::new(4);

        assert!(buffer.begin(7, vec![1]));
        assert!(!buffer.begin(8, vec![2]));
        assert_eq!(buffer.take(8), None);
        assert!(!buffer.cancel(Some(8)));
        assert_eq!(buffer.take(7), Some(vec![1]));
    }

    #[test]
    fn overflow_is_reported_once_and_remains_cancellable() {
        let mut buffer = SearchHandoffBuffer::new(2);

        assert!(buffer.begin(9, vec![1]));
        assert_eq!(buffer.push(2), PushResult::Buffered);
        assert_eq!(buffer.push(3), PushResult::Overflowed(9));
        assert_eq!(buffer.push(4), PushResult::OverflowPending);
        assert!(buffer.cancel(Some(9)));
    }

    #[test]
    fn buffers_only_physical_keys_for_ime_replay() {
        assert!(super::should_buffer_physical_key(0, 31, 0x53));
        assert!(!super::should_buffer_physical_key(0x10, 31, 0x53));
        assert!(!super::should_buffer_physical_key(0, 0, 0x53));
        assert!(!super::should_buffer_physical_key(0, 31, 0xE5));
        assert!(!super::should_buffer_physical_key(0, 31, 0xE7));
    }
}
