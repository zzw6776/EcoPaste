import { createCoalescedRequest } from "@/utils/coalescedRequest";

interface SyncStatusRefreshOptions<T> {
  isActive: () => boolean;
  onError: (error: unknown) => void;
  read: () => Promise<T>;
  write: (status: T) => void;
}

/** 合并状态事件洪峰；请求期间只保留一次补刷，隐藏或卸载后不再读写。 */
export function createSyncStatusRefresh<T>(
  options: SyncStatusRefreshOptions<T>,
) {
  const request = createCoalescedRequest(async () => {
    if (!options.isActive()) return;

    try {
      const status = await options.read();
      if (options.isActive()) options.write(status);
    } catch (error) {
      options.onError(error);
    }
  });

  return () => {
    if (!options.isActive()) return;

    return request();
  };
}
