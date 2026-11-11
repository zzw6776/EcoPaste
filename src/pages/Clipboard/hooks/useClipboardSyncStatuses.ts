import { type RefObject, useCallback, useEffect, useRef } from "react";
import { TAURI_EVENT } from "@/constants/events";
import { useTauriListen } from "@/hooks/useTauriListen";
import { updateClipboardItemSyncStatuses } from "@/stores/clipboardItemActivity";
import type { ClipboardItem } from "@/types/clipboard";
import { getSyncItemStatusesShared } from "../dataRequests";

/** Keeps card status projection current without making the list own sync request scheduling. */
export function useClipboardSyncStatuses(
  items: Map<number, ClipboardItem>,
  active: boolean,
  windowVisibleRef: RefObject<boolean>,
) {
  const itemsRef = useRef(items);
  const activeRef = useRef(active);
  const pendingRef = useRef(false);
  const inFlightRef = useRef(false);
  const mountedRef = useRef(false);
  activeRef.current = active;

  const refresh = useCallback(async () => {
    pendingRef.current = true;
    if (!activeRef.current || !windowVisibleRef.current) return;
    if (inFlightRef.current) return;

    inFlightRef.current = true;
    try {
      while (
        pendingRef.current &&
        mountedRef.current &&
        activeRef.current &&
        windowVisibleRef.current
      ) {
        pendingRef.current = false;
        const currentItems = itemsRef.current;
        const itemIds = [...currentItems.values()].map((item) => {
          return item.id;
        });
        try {
          const statuses = await getSyncItemStatusesShared(itemIds);
          if (!mountedRef.current) return;
          if (!activeRef.current || !windowVisibleRef.current) {
            pendingRef.current = true;
            return;
          }
          if (currentItems !== itemsRef.current) {
            pendingRef.current = true;
            continue;
          }
          updateClipboardItemSyncStatuses(statuses);
        } catch {
          // 命令层处理错误；查询期间的后续更新仍需消费。
        }
      }
    } finally {
      inFlightRef.current = false;
    }
  }, [windowVisibleRef]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  useEffect(() => {
    itemsRef.current = items;
    if (active) void refresh();
  }, [active, items, refresh]);

  useTauriListen(TAURI_EVENT.SYNC_UPDATED, () => {
    void refresh();
  });

  return refresh;
}
