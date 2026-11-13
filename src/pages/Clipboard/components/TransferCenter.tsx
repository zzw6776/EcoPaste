import { useMount, useUnmount } from "ahooks";
import { Button, Progress } from "antd";
import type { TFunction } from "i18next";
import type { FC, KeyboardEvent, MouseEvent } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Virtuoso } from "react-virtuoso";
import {
  cancelSyncTransfer,
  clearFinishedSyncTransfers,
  downloadRemoteFiles,
  getClipboardItem,
  getSyncStatus,
  type SyncPeerStatus,
  syncItemNow,
  type TransferTask,
} from "@/commands";
import Popover from "@/components/Popover";
import Tooltip from "@/components/Tooltip";
import { TAURI_EVENT } from "@/constants/events";
import {
  refreshSyncTransferActivity,
  useSyncTransferTasks,
} from "@/hooks/useSyncTransferActivity";
import { useTauriListen } from "@/hooks/useTauriListen";
import { formatBytes } from "@/pages/Preference/utils/storageUsage";
import { requestClipboardItemLocation } from "@/stores/clipboardItemLocation";
import { clipboardViewState } from "@/stores/clipboardView";
import type { ClipboardGroupRecord, ClipboardItem } from "@/types/clipboard";
import { cn } from "@/utils/cn";
import { listClipboardGroupsShared } from "../dataRequests";
import {
  isTransferActive,
  sortTransferTasksForDisplay,
  transferRouteIconClass,
} from "../utils/transferTasks";
import { getFileCollectionMeta } from "./cards/fileCollectionMeta";
import HeaderActionPopoverPanel from "./HeaderActionPopoverPanel";

interface TransferCenterProps {
  compact?: boolean;
}

const TransferCenter: FC<TransferCenterProps> = (props) => {
  const { compact = false } = props;
  const { t, i18n } = useTranslation("clipboard");
  const transferTasks = useSyncTransferTasks();
  const [open, setOpen] = useState(false);
  const [clearing, setClearing] = useState(false);
  const [groups, setGroups] = useState<ClipboardGroupRecord[]>([]);
  const [peers, setPeers] = useState<SyncPeerStatus[]>([]);
  const [itemsById, setItemsById] = useState(
    () => new Map<string, ClipboardItem | null>(),
  );
  const itemsByIdRef = useRef(itemsById);
  const pendingItemIdsRef = useRef(new Set<string>());
  const mountedRef = useRef(false);

  function cacheItems(entries: Array<readonly [string, ClipboardItem | null]>) {
    if (!mountedRef.current || entries.length === 0) return;

    const next = new Map(itemsByIdRef.current);
    for (const [itemId, item] of entries) next.set(itemId, item);
    itemsByIdRef.current = next;
    setItemsById(next);
  }

  /** 每个任务记录只读取一次，传输进度的高频事件不会重复查询数据库。 */
  async function loadItemContexts(itemIds: string[]) {
    const missingIds = [...new Set(itemIds)].filter((itemId) => {
      return (
        !itemsByIdRef.current.has(itemId) &&
        !pendingItemIdsRef.current.has(itemId)
      );
    });
    if (missingIds.length === 0) return;

    for (const itemId of missingIds) pendingItemIdsRef.current.add(itemId);
    const entries = await Promise.all(
      missingIds.map(async (itemId) => {
        try {
          return [itemId, await getClipboardItem(itemId)] as const;
        } catch {
          return [itemId, null] as const;
        } finally {
          pendingItemIdsRef.current.delete(itemId);
        }
      }),
    );
    cacheItems(entries);
  }

  async function loadGroups() {
    try {
      const records = await listClipboardGroupsShared();
      if (mountedRef.current) setGroups(records);
    } catch {
      // 命令层已统一显示错误。
    }
  }

  async function loadPeers() {
    try {
      const status = await getSyncStatus();
      if (mountedRef.current) setPeers(status.peers);
    } catch {
      // 命令层已统一显示错误。
    }
  }

  useMount(() => {
    mountedRef.current = true;
    void loadGroups();
    void loadPeers();
  });

  useUnmount(() => {
    mountedRef.current = false;
  });

  useTauriListen(TAURI_EVENT.CLIPBOARD_GROUPS_UPDATED, () => {
    void loadGroups();
  });

  useTauriListen(TAURI_EVENT.SYNC_UPDATED, () => {
    void loadPeers();
  });

  const activeCount = transferTasks.filter((task) => {
    return isTransferActive(task);
  }).length;
  const sorted = sortTransferTasksForDisplay(
    transferTasks.filter((task) => {
      return (
        task.target !== "import" ||
        (task.phase !== "queued" && task.phase !== "completed")
      );
    }),
  );
  const recentCount = sorted.length - activeCount;
  const visibleItemIds = sorted.map((task) => {
    return task.itemId;
  });
  const visibleItemIdsKey = visibleItemIds.join("\0");

  // biome-ignore lint/correctness/useExhaustiveDependencies: key 只在可见任务集合变化时触发条目补载。
  useEffect(() => {
    void loadItemContexts(visibleItemIds);
  }, [visibleItemIdsKey]);

  async function handleCancel(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    const task = transferTasks.find((candidate) => {
      return candidate.id === event.currentTarget.dataset.taskId;
    });
    if (!task) return;
    try {
      await cancelSyncTransfer(task.id);
    } catch {
      // 命令层已统一显示错误。
    }
  }

  async function handleRetry(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    const task = transferTasks.find((candidate) => {
      return candidate.id === event.currentTarget.dataset.taskId;
    });
    if (!task) return;
    try {
      if (task.direction === "download") {
        await downloadRemoteFiles(task.itemId);
        return;
      }
      await syncItemNow(task.itemId, task.route);
    } catch {
      // 命令层已统一显示错误。
    }
  }

  async function locateItem(itemId: string) {
    try {
      const item = await getClipboardItem(itemId);
      cacheItems([[itemId, item]]);
      if (!item) return;

      const groupId = groups.some((group) => {
        return group.id === item.groupId && !group.isHidden;
      })
        ? item.groupId
        : null;
      const category = groupId ? null : item.kind;
      clipboardViewState.keyword = "";
      clipboardViewState.range = "all";
      clipboardViewState.groupId = groupId;
      clipboardViewState.category = category;
      requestClipboardItemLocation({
        category,
        groupId,
        itemId,
        range: "all",
      });
      setOpen(false);
    } catch {
      // 命令层已统一显示错误。
    }
  }

  function handleLocate(event: MouseEvent<HTMLDivElement>) {
    const itemId = event.currentTarget.dataset.itemId;
    if (itemId) void locateItem(itemId);
  }

  function handleLocateKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    if (event.target !== event.currentTarget) return;
    if (event.key !== "Enter" && event.key !== " ") return;

    event.preventDefault();
    const itemId = event.currentTarget.dataset.itemId;
    if (itemId) void locateItem(itemId);
  }

  function handleOpenChange(nextOpen: boolean) {
    setOpen(nextOpen);
    if (nextOpen) void refreshSyncTransferActivity();
  }

  async function handleClearFinished(event: MouseEvent<HTMLElement>) {
    event.stopPropagation();
    if (clearing) return;

    setClearing(true);
    try {
      await clearFinishedSyncTransfers();
      await refreshSyncTransferActivity();
    } catch {
      // 命令层已统一显示错误。
    } finally {
      setClearing(false);
    }
  }

  function renderTransferTask(index: number, task: TransferTask) {
    const active = isTransferActive(task);
    const section =
      index === 0 && activeCount > 0
        ? t("transferCenter.active")
        : index === activeCount
          ? t("transferCenter.recent")
          : null;
    const item = itemsById.get(task.itemId);
    const itemTitle = getTransferItemTitle(task, item, t);
    const groupName = item?.groupId
      ? groups.find((group) => {
          return group.id === item.groupId && !group.isHidden;
        })?.name
      : null;
    const location = item ? (groupName ?? t(`groups.${item.kind}`)) : null;
    const phase =
      task.target === "import"
        ? t(`transferCenter.import.${task.phase}`)
        : t(`transferCenter.${task.direction}.${task.phase}`);
    const route = getTransferRouteLabel(task, peers, t);
    const alreadyAvailable = task.phase === "available";
    const startedAt = new Date(task.startedAtMs);
    const percent = task.totalBytes
      ? Math.min(
          100,
          Math.round((task.transferredBytes / task.totalBytes) * 100),
        )
      : 0;

    return (
      <div className="pb-2">
        {section && (
          <div
            className={cn("mb-2", {
              "mt-3 border-ant-border border-t pt-3": index > 0,
            })}
          >
            <div className="flex items-center justify-between rounded-1.5 bg-ant-fill-tertiary px-2 py-1.5">
              <span className="font-medium text-sm">{section}</span>
              <span className="text-ant-secondary text-xs tabular-nums">
                {active ? activeCount : recentCount}
              </span>
            </div>
          </div>
        )}
        {/* biome-ignore lint/a11y/useSemanticElements: 任务卡片内含取消和重试按钮，不能使用嵌套 button。 */}
        <div
          className="cursor-pointer rounded-2 border border-ant-border bg-ant-container p-2 transition-colors hover:bg-ant-fill-tertiary"
          data-item-id={task.itemId}
          onClick={handleLocate}
          onKeyDown={handleLocateKeyDown}
          role="button"
          tabIndex={0}
        >
          <div className="flex items-center gap-1 text-sm">
            <i
              className={cn("size-3.5 shrink-0", transferRouteIconClass(task))}
            />
            <span className="min-w-0 flex-1 truncate">{itemTitle}</span>
            {active && task.cancellable ? (
              <button
                className="cursor-pointer border-0 bg-transparent p-0 text-ant-primary text-xs"
                data-task-id={task.id}
                onClick={handleCancel}
                type="button"
              >
                {t("remoteFile.cancel")}
              </button>
            ) : (task.phase === "failed" || task.phase === "canceled") &&
              task.target !== "import" ? (
              <button
                className="cursor-pointer border-0 bg-transparent p-0 text-ant-primary text-xs"
                data-task-id={task.id}
                onClick={handleRetry}
                type="button"
              >
                {t("transferCenter.retry")}
              </button>
            ) : null}
          </div>
          <div className="truncate text-ant-secondary text-xs">
            {location ? `${location} · ` : ""}
            {route} · {phase}
          </div>
          {!alreadyAvailable && (
            <Progress percent={percent} showInfo={false} size="small" />
          )}
          {active && (
            <div className="text-ant-secondary text-xs">
              {formatBytes(task.speedBytesPerSecond)}/s ·{" "}
              {formatDuration(task.elapsedMs)}
              {task.etaSeconds !== null
                ? ` · ${t("remoteFile.eta", { seconds: task.etaSeconds })}`
                : ""}
            </div>
          )}
          {task.error && (
            <div className="truncate text-ant-error text-xs" title={task.error}>
              {task.error}
            </div>
          )}
          <div className="mt-1 flex items-center justify-between gap-2 whitespace-nowrap text-ant-secondary text-xs tabular-nums">
            <span>
              {alreadyAvailable
                ? formatBytes(task.totalBytes)
                : formatBytes(task.transferredBytes)}
              {!alreadyAvailable &&
                task.totalBytes > 0 &&
                ` / ${formatBytes(task.totalBytes)}`}
            </span>
            <span
              className="shrink-0"
              title={t("transferCenter.startedAt", {
                time: startedAt.toLocaleString(i18n.resolvedLanguage),
              })}
            >
              {startedAt.toLocaleString(i18n.resolvedLanguage, {
                day: "2-digit",
                hour: "2-digit",
                hour12: false,
                minute: "2-digit",
                month: "2-digit",
                second: "2-digit",
              })}
            </span>
          </div>
        </div>
      </div>
    );
  }

  const content = (
    <HeaderActionPopoverPanel className="gap-2">
      <div className="flex shrink-0 items-center justify-between gap-2">
        <span className="text-ant-secondary text-sm">
          {t("transferCenter.title")}
        </span>
        <Tooltip title={t("transferCenter.clearFinished")}>
          <Button
            aria-label={t("transferCenter.clearFinished")}
            disabled={recentCount === 0}
            icon={<i className="i-lucide:trash-2 size-4" />}
            loading={clearing}
            onClick={handleClearFinished}
            shape="circle"
            size="small"
            type="text"
          />
        </Tooltip>
      </div>
      {sorted.length === 0 ? (
        <span className="flex flex-1 items-center justify-center text-center text-ant-secondary text-sm">
          {t("transferCenter.empty")}
        </span>
      ) : (
        <Virtuoso
          className="min-h-0 flex-1"
          computeItemKey={getTransferTaskKey}
          data={sorted}
          itemContent={renderTransferTask}
        />
      )}
    </HeaderActionPopoverPanel>
  );

  return (
    <Popover
      content={content}
      onOpenChange={handleOpenChange}
      open={open}
      placement="bottomRight"
      trigger="click"
    >
      <button
        aria-label={t("transferCenter.title")}
        className={cn(
          "group relative flex cursor-pointer items-center justify-center rounded-full border-0 bg-transparent transition-colors hover:bg-ant-fill-tertiary",
          compact ? "size-8" : "size-7",
        )}
        type="button"
      >
        <i
          className={cn(
            "i-lucide:arrow-up-down size-4 stroke-[1.5] transition-colors",
            activeCount > 0
              ? "text-ant-info"
              : "text-ant-tertiary group-hover:text-ant-text",
          )}
        />
        {activeCount > 0 && (
          <span className="absolute top-0 right-0 flex size-4 items-center justify-center rounded-full bg-ant-info-bg text-[10px] text-ant-info">
            {Math.min(activeCount, 9)}
          </span>
        )}
      </button>
    </Popover>
  );
};

function getTransferTaskKey(_index: number, task: TransferTask) {
  return task.id;
}

function getTransferItemTitle(
  task: TransferTask,
  item: ClipboardItem | null | undefined,
  t: TFunction<"clipboard">,
) {
  if (task.fileName) return task.fileName;
  if (!item) return t("transferCenter.item");
  if (item.kind === "files") {
    const entries = item.fileEntries ?? [];
    const { count } = getFileCollectionMeta(item.content, item.fileTypes);
    if (count === 1) return entries[0]?.name ?? t("groups.files");
    if (count > 1) {
      return t("transferCenter.itemCount", { count });
    }
  }
  if (item.kind === "text" && item.summary) return item.summary;

  return t(`groups.${item.kind}`);
}

function getTransferRouteLabel(
  task: TransferTask,
  peers: SyncPeerStatus[],
  t: TFunction<"clipboard">,
) {
  if (task.target === "import") return t("transferCenter.route.local");
  if (task.route === "cloud") return t("transferCenter.route.cloud");

  const deviceId = task.target.startsWith("peer:")
    ? task.target.slice("peer:".length)
    : task.target;
  const deviceName = peers.find((peer) => {
    return peer.deviceId === deviceId;
  })?.deviceName;
  if (!deviceName) return t("transferCenter.route.lan");

  return t("transferCenter.route.lanDevice", { device: deviceName });
}

function formatDuration(milliseconds: number) {
  const totalSeconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
}

export default TransferCenter;
