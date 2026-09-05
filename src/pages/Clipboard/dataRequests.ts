import {
  getSyncItemStatuses,
  listClipboardGroups,
  type SyncItemStatus,
} from "@/commands";
import type { ClipboardGroupRecord } from "@/types/clipboard";

interface PendingSyncStatusRequest {
  itemIds: Set<string>;
  reject: (reason?: unknown) => void;
  resolve: (statuses: SyncItemStatus[]) => void;
}

let clipboardGroupsRequest: Promise<ClipboardGroupRecord[]> | null = null;
let pendingSyncStatusRequests: PendingSyncStatusRequest[] = [];
let syncStatusFlushScheduled = false;

/** 合并同一事件循环内的分组读取，避免移动端多个预加载列表重复调用 Rust。 */
export async function listClipboardGroupsShared() {
  if (clipboardGroupsRequest) return await clipboardGroupsRequest;

  const request = listClipboardGroups();
  clipboardGroupsRequest = request;

  try {
    return await request;
  } finally {
    if (clipboardGroupsRequest === request) clipboardGroupsRequest = null;
  }
}

/** 合并同一批列表的同步状态读取，再按原请求 ID 集合拆分结果。 */
export function getSyncItemStatusesShared(itemIds: string[]) {
  const uniqueItemIds = new Set(itemIds);
  if (uniqueItemIds.size === 0) return Promise.resolve<SyncItemStatus[]>([]);

  const request = new Promise<SyncItemStatus[]>((resolve, reject) => {
    pendingSyncStatusRequests.push({
      itemIds: uniqueItemIds,
      reject,
      resolve,
    });
  });

  if (!syncStatusFlushScheduled) {
    syncStatusFlushScheduled = true;
    queueMicrotask(() => {
      void flushSyncStatusRequests();
    });
  }

  return request;
}

/** 执行一次合并后的状态查询，并保持每个调用方收到与自身 ID 对应的结果。 */
async function flushSyncStatusRequests() {
  const requests = pendingSyncStatusRequests;
  pendingSyncStatusRequests = [];
  syncStatusFlushScheduled = false;

  const itemIds = [
    ...new Set(
      requests.flatMap((request) => {
        return [...request.itemIds];
      }),
    ),
  ];

  try {
    const statuses = await getSyncItemStatuses(itemIds);
    for (const request of requests) {
      request.resolve(
        statuses.filter((status) => {
          return request.itemIds.has(status.itemId);
        }),
      );
    }
  } catch (error) {
    for (const request of requests) request.reject(error);
  }
}
