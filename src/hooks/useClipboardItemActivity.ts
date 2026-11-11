import { useCallback, useSyncExternalStore } from "react";
import {
  getClipboardItemActivity,
  subscribeClipboardItemActivity,
} from "@/stores/clipboardItemActivity";

/** 订阅单个条目的同步与传输状态，不把高频更新提升到列表组件。 */
export function useClipboardItemActivity(itemId: string) {
  const subscribe = useCallback(
    (listener: () => void) => {
      return subscribeClipboardItemActivity(itemId, listener);
    },
    [itemId],
  );
  const getSnapshot = useCallback(() => {
    return getClipboardItemActivity(itemId);
  }, [itemId]);

  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
