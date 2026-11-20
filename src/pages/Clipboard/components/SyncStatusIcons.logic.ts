import type { SyncChannelState, SyncPeerStatus } from "@/commands";
import { createCoalescedRequest } from "@/utils/coalescedRequest";

interface SyncStatusRefreshOptions<T> {
  isActive: () => boolean;
  onError: (error: unknown) => void;
  read: () => Promise<T>;
  write: (status: T) => void;
}

export type UsageTone = "normal" | "warning" | "error";

export interface LanPeerSummary {
  connectingCount: number;
  errorCount: number;
  onlineCount: number;
  totalCount: number;
}

const PEER_STATE_ORDER: Record<SyncChannelState, number> = {
  connecting: 1,
  degraded: 3,
  disabled: 5,
  error: 2,
  idle: 4,
  online: 0,
};

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

/** 计算限额使用率（0~100 整数）；上限为 0 或无效时返回 null。 */
export function resolveUsagePercent(
  used: number,
  limit: number,
): number | null {
  if (!Number.isFinite(used) || !Number.isFinite(limit) || limit <= 0) {
    return null;
  }
  if (used <= 0) return 0;

  return Math.min(100, Math.round((used / limit) * 100));
}

/** 按占用率映射配额告警级别：90% 以上为错误色，75% 以上为警告色。 */
export function resolveUsageTone(percent: number | null): UsageTone {
  if (percent === null) return "normal";
  if (percent >= 90) return "error";
  if (percent >= 75) return "warning";

  return "normal";
}

/** 局域网设备排序：在线优先，其次连接中/异常，最后离线，同状态按名称排序。 */
export function sortLanPeersForDisplay(
  peers: readonly SyncPeerStatus[],
): SyncPeerStatus[] {
  return [...peers].sort((left, right) => {
    const rankDiff =
      PEER_STATE_ORDER[left.state] - PEER_STATE_ORDER[right.state];
    if (rankDiff !== 0) return rankDiff;

    return left.deviceName.localeCompare(right.deviceName);
  });
}

/** 统计局域网设备各状态数量。 */
export function summarizeLanPeers(
  peers: readonly SyncPeerStatus[],
): LanPeerSummary {
  let onlineCount = 0;
  let connectingCount = 0;
  let errorCount = 0;

  for (const peer of peers) {
    if (peer.state === "online") {
      onlineCount += 1;
    } else if (peer.state === "connecting") {
      connectingCount += 1;
    } else if (peer.state === "error" || peer.state === "degraded") {
      errorCount += 1;
    }
  }

  return {
    connectingCount,
    errorCount,
    onlineCount,
    totalCount: peers.length,
  };
}

/** 折叠展示过长的 Endpoint ID，保留首尾便于核对。 */
export function shortenEndpointId(
  endpointId: string,
  head = 6,
  tail = 6,
): string {
  const trimmed = endpointId.trim();
  if (trimmed.length <= head + tail + 1) return trimmed;

  return `${trimmed.slice(0, head)}…${trimmed.slice(-tail)}`;
}

/** 格式化紧凑时间戳，适用于窄浮层状态行，完整时间保留在 tooltip 中。 */
export function formatCompactTimestamp(
  iso: string | null | undefined,
  locale?: string,
): string | null {
  if (!iso) return null;

  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return null;

  return date.toLocaleString(locale, {
    day: "2-digit",
    hour: "2-digit",
    hour12: false,
    minute: "2-digit",
    month: "2-digit",
    second: "2-digit",
  });
}

/** 映射设备平台名称。 */
export function formatPlatformLabel(platform: string): string {
  switch (platform.toLowerCase()) {
    case "macos":
      return "macOS";
    case "windows":
      return "Windows";
    case "android":
      return "Android";
    default:
      return platform;
  }
}

/** 映射设备平台图标类名。 */
export function platformIconClass(platform: string): string {
  return platform.toLowerCase() === "android"
    ? "i-lucide:smartphone"
    : "i-lucide:monitor";
}
