import { useCallback, useEffect, useRef } from "react";
import { TAURI_EVENT } from "@/constants/events";
import { useTauriListen } from "@/hooks/useTauriListen";
import { useWindowLifecycle } from "@/hooks/useWindowLifecycle";
import {
  getClipboardItemActivity,
  updateClipboardItemSyncStatuses,
} from "@/stores/clipboardItemActivity";
import { settingsState } from "@/stores/settings";
import {
  isCurrentWindowDormant,
  isCurrentWindowVisible,
} from "@/stores/windowLifecycle";
import type { ClipboardItem } from "@/types/clipboard";
import { getSyncItemStatusesShared } from "../dataRequests";

const HIDDEN_WARM_SYNC_DEBOUNCE_MS = 120;

/**
 * 维护已加载卡片的增量同步状态投影：
 * - 首帧状态已由 `listClipboardItems` / `getClipboardItem` 同批注水；
 * - `Visible` 阶段收到 `sync://updated` 立即增量刷新；
 * - `HiddenWarm` 阶段 120ms 防抖跟进，保证秒切呼出窗口时直接呈绿态；
 * - `Dormant` 休眠阶段只记脏标记，回到可见时一次性补齐。
 */
export function useClipboardSyncStatuses(
  items: Map<number, ClipboardItem>,
  active: boolean,
) {
  const lifecycle = useWindowLifecycle();
  const itemsRef = useRef(items);
  const activeRef = useRef(active);
  const pendingRef = useRef(false);
  const inFlightRef = useRef(false);
  const mountedRef = useRef(false);
  const warmTimerRef = useRef<number | null>(null);
  activeRef.current = active;

  const clearWarmTimer = useCallback(() => {
    if (warmTimerRef.current === null) return;

    window.clearTimeout(warmTimerRef.current);
    warmTimerRef.current = null;
  }, []);

  const flush = useCallback(async () => {
    clearWarmTimer();
    if (!mountedRef.current || !activeRef.current || isCurrentWindowDormant()) {
      return;
    }
    if (inFlightRef.current) return;

    inFlightRef.current = true;
    try {
      while (
        pendingRef.current &&
        mountedRef.current &&
        activeRef.current &&
        !isCurrentWindowDormant()
      ) {
        pendingRef.current = false;
        const currentItems = itemsRef.current;
        if (currentItems.size === 0) continue;

        const itemIds = [...currentItems.values()].map((item) => {
          return item.id;
        });
        try {
          const statuses = await getSyncItemStatusesShared(itemIds);
          if (!mountedRef.current) return;
          if (!activeRef.current || isCurrentWindowDormant()) {
            pendingRef.current = true;
            return;
          }
          updateClipboardItemSyncStatuses(statuses);
          if (currentItems !== itemsRef.current) {
            pendingRef.current = true;
          }
        } catch {
          // 命令层处理错误；查询期间的后续更新仍需消费。
        }
      }
    } finally {
      inFlightRef.current = false;
    }
  }, [clearWarmTimer]);

  const requestRefresh = useCallback(() => {
    pendingRef.current = true;
    if (!mountedRef.current || !activeRef.current) return;
    if (isCurrentWindowDormant()) {
      clearWarmTimer();
      return;
    }
    if (isCurrentWindowVisible()) {
      void flush();
      return;
    }

    clearWarmTimer();
    warmTimerRef.current = window.setTimeout(() => {
      warmTimerRef.current = null;
      void flush();
    }, HIDDEN_WARM_SYNC_DEBOUNCE_MS);
  }, [clearWarmTimer, flush]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      clearWarmTimer();
    };
  }, [clearWarmTimer]);

  useEffect(() => {
    itemsRef.current = items;
    if (!active || !settingsState.sync.enabled || items.size === 0) return;

    let missingStatus = false;
    for (const item of items.values()) {
      if (!getClipboardItemActivity(item.id).syncStatus) {
        missingStatus = true;
        break;
      }
    }
    if (missingStatus || pendingRef.current) {
      requestRefresh();
    }
  }, [active, items, requestRefresh]);

  useEffect(() => {
    if (lifecycle.phase === "dormant") {
      clearWarmTimer();
      return;
    }
    if (lifecycle.visible && active && pendingRef.current) {
      void flush();
    }
  }, [active, clearWarmTimer, flush, lifecycle.phase, lifecycle.visible]);

  useTauriListen(TAURI_EVENT.SYNC_UPDATED, () => {
    requestRefresh();
  });

  return requestRefresh;
}
