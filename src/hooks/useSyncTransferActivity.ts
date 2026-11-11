import { useMount, useUnmount } from "ahooks";
import { useRef, useSyncExternalStore } from "react";
import { listSyncTransfers, type TransferTask } from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import {
  getClipboardTransferRevision,
  getClipboardTransferTasks,
  replaceClipboardTransferSnapshot,
  subscribeClipboardTransferTasks,
  updateClipboardTransferTask,
} from "@/stores/clipboardItemActivity";
import { isAndroid } from "@/utils/is";
import { useTauriListen } from "./useTauriListen";

let refreshInFlight: Promise<void> | null = null;

/** 拉取 Rust 权威快照；并发刷新共用一次 IPC。 */
export function refreshSyncTransferActivity() {
  if (refreshInFlight) return refreshInFlight;

  refreshInFlight = refreshTransferSnapshot();
  return refreshInFlight;
}

async function refreshTransferSnapshot() {
  try {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const requestedAtRevision = getClipboardTransferRevision();
      const tasks = await listSyncTransfers();
      if (replaceClipboardTransferSnapshot(tasks, requestedAtRevision)) return;
    }
  } catch {
    // 命令层已统一显示错误。
  } finally {
    refreshInFlight = null;
  }
}

/** 在页面根部建立唯一的传输事件与恢复快照桥接。 */
export function useSyncTransferActivity() {
  const removeResumeListenersRef = useRef<(() => void) | null>(null);

  useTauriListen<TransferTask>(TAURI_EVENT.SYNC_TRANSFER_UPDATED, (event) => {
    updateClipboardTransferTask(event.payload);
  });

  useMount(() => {
    void refreshSyncTransferActivity();
    if (!isAndroid) return;

    const handleResume = () => {
      if (!document.hidden) void refreshSyncTransferActivity();
    };
    window.addEventListener("focus", handleResume);
    document.addEventListener("visibilitychange", handleResume);
    removeResumeListenersRef.current = () => {
      window.removeEventListener("focus", handleResume);
      document.removeEventListener("visibilitychange", handleResume);
    };
  });

  useUnmount(() => {
    removeResumeListenersRef.current?.();
    removeResumeListenersRef.current = null;
  });
}

/** 传输中心订阅与卡片相同的共享任务投影。 */
export function useSyncTransferTasks() {
  return useSyncExternalStore(
    subscribeClipboardTransferTasks,
    getClipboardTransferTasks,
    getClipboardTransferTasks,
  );
}
