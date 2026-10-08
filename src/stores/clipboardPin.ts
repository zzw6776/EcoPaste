import { proxy } from "valtio";
import { isClipboardWindowPinned, setClipboardWindowPinned } from "@/commands";
import { isTauri } from "@/utils/is";
import { log } from "@/utils/log";

interface ClipboardPinState {
  isPinned: boolean;
}

/**
 * 剪贴板窗口固定状态（控制是否允许失焦及切 Space 自动隐藏）。
 * 与条目置顶（isPinned）严格隔离，真相源在 Rust（`CLIPBOARD_WINDOW_PINNED`）。
 */
export const clipboardPinState = proxy<ClipboardPinState>({
  isPinned: false,
});

/**
 * 切换剪贴板窗口的固定态。
 */
export const toggleClipboardWindowPinned = async () => {
  const next = !clipboardPinState.isPinned;
  clipboardPinState.isPinned = next;

  try {
    await setClipboardWindowPinned(next);
  } catch (error) {
    clipboardPinState.isPinned = !next;
    log.error("toggle clipboard window pinned failed:", error);
    throw error;
  }
};

/**
 * 从 Rust 状态同步初始固定态。
 */
export const syncClipboardWindowPinned = async () => {
  if (!isTauri) {
    return;
  }

  try {
    const pinned = await isClipboardWindowPinned();
    clipboardPinState.isPinned = pinned;
  } catch (error) {
    log.warn("sync clipboard window pinned failed:", error);
  }
};
