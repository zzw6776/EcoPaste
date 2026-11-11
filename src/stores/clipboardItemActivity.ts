import type { SyncItemStatus, TransferTask } from "@/commands";
import {
  isTransferActive,
  sortTransferTasksForDisplay,
  upsertTransferTask,
} from "@/pages/Clipboard/utils/transferTasks";

export interface ClipboardItemActivitySnapshot {
  syncStatus?: SyncItemStatus;
  transferTasks: TransferTask[];
}

const EMPTY_ACTIVITY: ClipboardItemActivitySnapshot = {
  transferTasks: [],
};
const activityByItemId = new Map<string, ClipboardItemActivitySnapshot>();
const listenersByItemId = new Map<string, Set<() => void>>();
let transferTasks = new Map<string, TransferTask>();
let transferTaskSnapshot: TransferTask[] = [];
let transferRevision = 0;
const transferTaskListeners = new Set<() => void>();

export function getClipboardItemActivity(itemId: string) {
  return activityByItemId.get(itemId) ?? EMPTY_ACTIVITY;
}

export function subscribeClipboardItemActivity(
  itemId: string,
  listener: () => void,
) {
  const listeners = listenersByItemId.get(itemId) ?? new Set();
  listeners.add(listener);
  listenersByItemId.set(itemId, listeners);

  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) listenersByItemId.delete(itemId);
  };
}

export function getClipboardTransferTasks() {
  return transferTaskSnapshot;
}

export function getClipboardTransferRevision() {
  return transferRevision;
}

export function subscribeClipboardTransferTasks(listener: () => void) {
  transferTaskListeners.add(listener);

  return () => {
    transferTaskListeners.delete(listener);
  };
}

/** 只发布实际变化的条目状态，避免一次全局同步事件重渲染所有卡片。 */
export function updateClipboardItemSyncStatuses(statuses: SyncItemStatus[]) {
  for (const status of statuses) {
    const current = getClipboardItemActivity(status.itemId);
    if (syncStatusEquals(current.syncStatus, status)) continue;

    commitActivity(status.itemId, {
      syncStatus: status,
      transferTasks: current.transferTasks,
    });
  }
}

export function updateClipboardItemSyncStatus(status: SyncItemStatus) {
  updateClipboardItemSyncStatuses([status]);
}

/** 合并单条传输进度，并且只唤醒对应 itemId 的卡片。 */
export function updateClipboardTransferTask(task: TransferTask) {
  const next = upsertTransferTask(transferTasks, task);
  if (next === transferTasks) return;

  const affectedItems = getChangedTransferItemIds(transferTasks, next);
  transferRevision += 1;
  commitTransferTasks(next, affectedItems);
}

/** 仅在请求期间没有实时事件时，用 Rust 权威快照完整替换本地投影。 */
export function replaceClipboardTransferSnapshot(
  snapshot: TransferTask[],
  requestedAtRevision: number,
) {
  if (requestedAtRevision !== transferRevision) return false;

  const next = new Map(snapshot.map((task) => [task.id, task]));
  const affectedItems = getChangedTransferItemIds(transferTasks, next);
  commitTransferTasks(next, affectedItems);
  return true;
}

export function hasActiveClipboardItemDownload(itemId: string) {
  return [...transferTasks.values()].some((task) => {
    return (
      task.itemId === itemId &&
      task.direction === "download" &&
      isTransferActive(task)
    );
  });
}

function refreshTransferSnapshots(itemIds: Set<string>) {
  for (const itemId of itemIds) {
    const current = getClipboardItemActivity(itemId);
    const nextTasks = sortTransferTasksForDisplay(
      [...transferTasks.values()].filter((task) => {
        return task.itemId === itemId;
      }),
    );
    if (transferTasksEqual(current.transferTasks, nextTasks)) continue;

    commitActivity(itemId, {
      syncStatus: current.syncStatus,
      transferTasks: nextTasks,
    });
  }
}

/** 同时发布全局任务列表和对应卡片投影，保证两处 UI 观察同一份状态。 */
function commitTransferTasks(
  next: Map<string, TransferTask>,
  affectedItems: Set<string>,
) {
  transferTasks = next;
  transferTaskSnapshot = [...next.values()];
  refreshTransferSnapshots(affectedItems);
  for (const listener of [...transferTaskListeners]) listener();
}

function commitActivity(
  itemId: string,
  snapshot: ClipboardItemActivitySnapshot,
) {
  activityByItemId.set(itemId, snapshot);
  const listeners = listenersByItemId.get(itemId);
  if (!listeners) return;

  for (const listener of [...listeners]) listener();
}

function syncStatusEquals(
  left: SyncItemStatus | undefined,
  right: SyncItemStatus,
) {
  if (!left || left.itemId !== right.itemId) return false;

  return (
    left.origin === right.origin &&
    left.localState === right.localState &&
    channelStatusEquals(left.lan, right.lan) &&
    channelStatusEquals(left.cloud, right.cloud)
  );
}

function channelStatusEquals(
  left: SyncItemStatus["lan"],
  right: SyncItemStatus["lan"],
) {
  return (
    left.availability === right.availability &&
    left.activity === right.activity &&
    left.action === right.action &&
    left.deliveredTargets === right.deliveredTargets &&
    left.contentTargets === right.contentTargets &&
    left.totalTargets === right.totalTargets &&
    left.lastError === right.lastError
  );
}

function transferTasksEqual(left: TransferTask[], right: TransferTask[]) {
  if (left.length !== right.length) return false;

  return left.every((task, index) => {
    const next = right[index];
    return next !== void 0 && transferTaskEquals(task, next);
  });
}

function getChangedTransferItemIds(
  previous: Map<string, TransferTask>,
  next: Map<string, TransferTask>,
) {
  const itemIds = new Set<string>();
  for (const [id, task] of previous) {
    const nextTask = next.get(id);
    if (!nextTask || !transferTaskEquals(task, nextTask)) {
      itemIds.add(task.itemId);
      if (nextTask) itemIds.add(nextTask.itemId);
    }
  }
  for (const [id, task] of next) {
    if (!previous.has(id)) itemIds.add(task.itemId);
  }
  return itemIds;
}

function transferTaskEquals(left: TransferTask, right: TransferTask) {
  return left.id === right.id && left.revision === right.revision;
}
