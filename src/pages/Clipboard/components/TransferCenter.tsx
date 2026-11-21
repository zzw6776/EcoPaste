import { useMount, useUnmount } from "ahooks";
import { App, Button, Progress } from "antd";
import type { TFunction } from "i18next";
import type { FC, KeyboardEvent, MouseEvent } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Virtuoso } from "react-virtuoso";
import {
  cancelSyncTransfer,
  clearFinishedSyncTransfers,
  downloadRemoteFiles,
  getClipboardItem,
  getSyncStatus,
  type SyncBackfillProgress,
  type SyncInboundCatchupProgress,
  type SyncInboundCatchupSources,
  type SyncPeerCatchupStatus,
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
import { useWindowLifecycle } from "@/hooks/useWindowLifecycle";
import { formatBytes } from "@/pages/Preference/utils/storageUsage";
import { requestClipboardItemLocation } from "@/stores/clipboardItemLocation";
import { clipboardViewState } from "@/stores/clipboardView";
import { isCurrentWindowDormant } from "@/stores/windowLifecycle";
import type { ClipboardGroupRecord, ClipboardItem } from "@/types/clipboard";
import { cn } from "@/utils/cn";
import { listClipboardGroupsShared } from "../dataRequests";
import {
  isTransferActive,
  isTransferFailed,
  sortTransferTasksForDisplay,
  transferDirectionIconClass,
  transferRouteIconClass,
} from "../utils/transferTasks";
import { getFileCollectionMeta } from "./cards/fileCollectionMeta";
import HeaderActionPopoverPanel from "./HeaderActionPopoverPanel";

const SYNC_STATUS_REFRESH_DEBOUNCE_MS = 150;
const PANEL_MAX_WIDTH = 360;

type TransferFilter = "all" | "active" | "finished";

interface TransferCenterProps {
  compact?: boolean;
}

const TransferCenter: FC<TransferCenterProps> = (props) => {
  const { compact = false } = props;
  const { message } = App.useApp();
  const { t, i18n } = useTranslation("clipboard");
  const lifecycle = useWindowLifecycle();
  const transferTasks = useSyncTransferTasks();

  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState<TransferFilter>("all");
  const [clearing, setClearing] = useState(false);
  const [groups, setGroups] = useState<ClipboardGroupRecord[]>([]);
  const [peers, setPeers] = useState<SyncPeerStatus[]>([]);
  const [backfill, setBackfill] = useState<SyncBackfillProgress | null>(null);
  const [inboundCatchup, setInboundCatchup] =
    useState<SyncInboundCatchupProgress | null>(null);
  const [servingCatchups, setServingCatchups] = useState<
    SyncPeerCatchupStatus[]
  >([]);
  const [itemsById, setItemsById] = useState(() => {
    return new Map<string, ClipboardItem | null>();
  });

  const itemsByIdRef = useRef(itemsById);
  const pendingItemIdsRef = useRef(new Set<string>());
  const mountedRef = useRef(false);
  const statusTimerRef = useRef<number | null>(null);
  const statusDirtyRef = useRef(false);
  const statusLoadedRef = useRef(false);

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

  const clearStatusTimer = useCallback(() => {
    if (statusTimerRef.current === null) return;

    window.clearTimeout(statusTimerRef.current);
    statusTimerRef.current = null;
  }, []);

  const loadSyncStatus = useCallback(async () => {
    try {
      const status = await getSyncStatus();
      if (!mountedRef.current) return;

      statusLoadedRef.current = true;
      setPeers(status.peers);
      setBackfill(status.backfill);
      setInboundCatchup(status.inboundCatchup);
      setServingCatchups(status.servingCatchups);
    } catch {
      // 命令层已统一显示错误。
    }
  }, []);

  const scheduleSyncStatusRefresh = useCallback(() => {
    if (!mountedRef.current) return;
    if (isCurrentWindowDormant()) {
      statusDirtyRef.current = true;
      clearStatusTimer();
      return;
    }

    clearStatusTimer();
    statusTimerRef.current = window.setTimeout(() => {
      statusTimerRef.current = null;
      if (!mountedRef.current || isCurrentWindowDormant()) {
        statusDirtyRef.current = true;
        return;
      }
      void loadSyncStatus();
    }, SYNC_STATUS_REFRESH_DEBOUNCE_MS);
  }, [clearStatusTimer, loadSyncStatus]);

  useMount(() => {
    mountedRef.current = true;
    if (isCurrentWindowDormant()) {
      statusDirtyRef.current = true;
      return;
    }
    void loadSyncStatus();
  });

  useUnmount(() => {
    mountedRef.current = false;
    clearStatusTimer();
  });

  useTauriListen(TAURI_EVENT.CLIPBOARD_GROUPS_UPDATED, () => {
    if (open) void loadGroups();
  });

  useTauriListen(TAURI_EVENT.SYNC_UPDATED, () => {
    scheduleSyncStatusRefresh();
  });

  useEffect(() => {
    if (!lifecycle.initialized) return;
    if (!lifecycle.visible) {
      setOpen(false);
      if (lifecycle.phase === "dormant") clearStatusTimer();
      return;
    }

    if (statusDirtyRef.current || !statusLoadedRef.current) {
      statusDirtyRef.current = false;
      clearStatusTimer();
      void loadSyncStatus();
    }
  }, [
    clearStatusTimer,
    lifecycle.initialized,
    lifecycle.phase,
    lifecycle.visible,
    loadSyncStatus,
  ]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: 每次打开读取当前详情，关闭期间不查询展示数据。
  useEffect(() => {
    if (!open) return;

    void loadGroups();
    void loadSyncStatus();
  }, [open, loadSyncStatus]);

  const sorted = useMemo(() => {
    return sortTransferTasksForDisplay(
      transferTasks.filter((task) => {
        return task.target !== "import" || task.phase !== "completed";
      }),
    );
  }, [transferTasks]);

  const activeCount = useMemo(() => {
    return sorted.filter((task) => {
      return isTransferActive(task);
    }).length;
  }, [sorted]);

  const failedCount = useMemo(() => {
    return sorted.filter((task) => {
      return isTransferFailed(task);
    }).length;
  }, [sorted]);

  const finishedCount = sorted.length - activeCount;

  const filteredTasks = useMemo(() => {
    if (filter === "active") {
      return sorted.filter((task) => {
        return isTransferActive(task);
      });
    }
    if (filter === "finished") {
      return sorted.filter((task) => {
        return !isTransferActive(task);
      });
    }
    return sorted;
  }, [filter, sorted]);

  const visibleItemIdsKey = useMemo(() => {
    return filteredTasks
      .map((task) => {
        return task.itemId;
      })
      .join("\0");
  }, [filteredTasks]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: 仅在弹窗打开且展示集合变化时补载上下文，进度心跳不重复查询数据库。
  useEffect(() => {
    if (!open) return;

    void loadItemContexts(
      filteredTasks.map((task) => {
        return task.itemId;
      }),
    );
  }, [open, visibleItemIdsKey]);

  async function handleCancel(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    const taskId = event.currentTarget.dataset.taskId;
    const task = sorted.find((candidate) => {
      return candidate.id === taskId;
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
    const taskId = event.currentTarget.dataset.taskId;
    const task = sorted.find((candidate) => {
      return candidate.id === taskId;
    });
    if (!task || task.direction === "serve") return;

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
      if (!item) {
        void message.info(t("transferCenter.itemMissing"));
        return;
      }

      const visibleGroupId = groups.some((group) => {
        return group.id === item.groupId && !group.isHidden;
      })
        ? item.groupId
        : null;

      const canKeepCurrentView =
        clipboardViewState.keyword === "" &&
        clipboardViewState.range === "all" &&
        (clipboardViewState.groupId === null
          ? clipboardViewState.category === null ||
            clipboardViewState.category === item.kind
          : clipboardViewState.groupId === visibleGroupId);

      const nextGroupId = canKeepCurrentView
        ? clipboardViewState.groupId
        : visibleGroupId;
      const nextCategory = canKeepCurrentView
        ? clipboardViewState.category
        : visibleGroupId
          ? null
          : item.kind;

      clipboardViewState.keyword = "";
      clipboardViewState.range = "all";
      clipboardViewState.groupId = nextGroupId;
      clipboardViewState.category = nextCategory;

      requestClipboardItemLocation({
        category: nextCategory,
        groupId: nextGroupId,
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

  function handleSelectFilter(event: MouseEvent<HTMLButtonElement>) {
    const nextFilter = event.currentTarget.dataset.filter as
      | TransferFilter
      | undefined;
    if (nextFilter) setFilter(nextFilter);
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

  function renderCatchupSection() {
    const hasCatchup =
      backfill !== null ||
      inboundCatchup !== null ||
      servingCatchups.length > 0;
    if (!hasCatchup) return null;

    const outboundPercent =
      backfill && backfill.total > 0
        ? Math.min(100, Math.round((backfill.completed / backfill.total) * 100))
        : 0;
    const inboundMetadataPercent =
      inboundCatchup && inboundCatchup.metadataTotal > 0
        ? Math.min(
            100,
            Math.round(
              (inboundCatchup.metadataCompleted /
                inboundCatchup.metadataTotal) *
                100,
            ),
          )
        : 0;
    const inboundAttachmentPercent =
      inboundCatchup && inboundCatchup.attachmentTotal > 0
        ? Math.min(
            100,
            Math.round(
              (inboundCatchup.attachmentCompleted /
                inboundCatchup.attachmentTotal) *
                100,
            ),
          )
        : 0;
    const metadataSourceText = inboundCatchup
      ? formatInboundCatchupSources(inboundCatchup.metadataSources, t)
      : null;
    const attachmentSourceText = inboundCatchup
      ? formatInboundCatchupSources(inboundCatchup.attachmentSources, t)
      : null;

    return (
      <div className="flex shrink-0 flex-col gap-2 rounded-2.5 border border-ant-border bg-ant-container p-2.5">
        <div className="flex items-center justify-between gap-2">
          <div className="flex min-w-0 items-center gap-1.5">
            <i className="i-lucide:history size-3.5 shrink-0 text-ant-info" />
            <span className="truncate font-medium text-xs">
              {t("transferCenter.backfill.title")}
            </span>
          </div>
          <span className="truncate text-ant-secondary text-xs">
            {t("transferCenter.backfill.description")}
          </span>
        </div>

        {backfill && (
          <div className="flex flex-col gap-1">
            <div className="flex items-center justify-between gap-2 text-xs">
              <span className="flex items-center gap-1 text-ant-text">
                <i className="i-lucide:arrow-up-right size-3 text-ant-info" />
                {t("transferCenter.backfill.outbound")}
              </span>
              <span className="text-ant-secondary tabular-nums">
                {backfill.completed} / {backfill.total}
              </span>
            </div>
            <Progress percent={outboundPercent} showInfo={false} size="small" />
          </div>
        )}

        {inboundCatchup && inboundCatchup.metadataTotal > 0 && (
          <div className="flex flex-col gap-1">
            <div className="flex items-center justify-between gap-2 text-xs">
              <span className="flex min-w-0 items-center gap-1 text-ant-text">
                <i className="i-lucide:arrow-down-left size-3 shrink-0 text-ant-info" />
                <span className="shrink-0">
                  {t("transferCenter.backfill.inbound")}
                </span>
                {metadataSourceText && (
                  <span className="truncate text-ant-secondary">
                    · {metadataSourceText}
                  </span>
                )}
              </span>
              <span className="shrink-0 text-ant-secondary tabular-nums">
                {t("transferCenter.backfill.inboundMetadata", {
                  percent: inboundMetadataPercent,
                })}
              </span>
            </div>
            <Progress
              percent={inboundMetadataPercent}
              showInfo={false}
              size="small"
            />
          </div>
        )}

        {inboundCatchup && inboundCatchup.attachmentTotal > 0 && (
          <div className="flex flex-col gap-1">
            <div className="flex items-center justify-between gap-2 text-xs">
              <span className="flex min-w-0 items-center gap-1 text-ant-text">
                <i className="i-lucide:download size-3 shrink-0 text-ant-info" />
                <span className="shrink-0">
                  {t("transferCenter.backfill.attachments")}
                </span>
                {attachmentSourceText && (
                  <span className="truncate text-ant-secondary">
                    · {attachmentSourceText}
                  </span>
                )}
              </span>
              <span className="shrink-0 text-ant-secondary tabular-nums">
                {t("transferCenter.backfill.inboundAttachments", {
                  completed: inboundCatchup.attachmentCompleted,
                  total:
                    inboundCatchup.metadataTotal > 0
                      ? `${inboundCatchup.attachmentTotal}+`
                      : `${inboundCatchup.attachmentTotal}`,
                })}
              </span>
            </div>
            <Progress
              percent={inboundAttachmentPercent}
              showInfo={false}
              size="small"
            />
          </div>
        )}

        {servingCatchups.map((serving) => {
          const totalItems = serving.servedItems + serving.remainingItems;
          const servingPercent =
            totalItems > 0
              ? Math.min(
                  100,
                  Math.round((serving.servedItems / totalItems) * 100),
                )
              : 100;
          const baseSummary =
            serving.remainingItems > 0
              ? t("transferCenter.backfill.servingProgress", {
                  served: serving.servedItems,
                  total: totalItems,
                })
              : t("transferCenter.backfill.servingDone", {
                  served: serving.servedItems,
                });
          const summaryParts = [baseSummary];
          if (serving.servedAttachments > 0) {
            summaryParts.push(
              t("transferCenter.backfill.servingMedia", {
                count: serving.servedAttachments,
              }),
            );
          }
          if (serving.activeTransfers > 0) {
            summaryParts.push(
              t("transferCenter.backfill.servingActive", {
                transfers: serving.activeTransfers,
              }),
            );
          }
          const summary = summaryParts.join(" · ");

          return (
            <div className="flex flex-col gap-1" key={serving.deviceId}>
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="flex min-w-0 items-center gap-1 text-ant-text">
                  <i className="i-lucide:arrow-left-right size-3 shrink-0 text-ant-info" />
                  <span className="truncate">
                    {t("transferCenter.backfill.serving", {
                      device: serving.deviceName,
                    })}
                  </span>
                </span>
                <span className="shrink-0 text-ant-secondary tabular-nums">
                  {summary}
                </span>
              </div>
              <Progress
                percent={servingPercent}
                showInfo={false}
                size="small"
              />
            </div>
          );
        })}
      </div>
    );
  }

  function renderTransferTask(index: number, task: TransferTask) {
    const active = isTransferActive(task);
    const failed = isTransferFailed(task);
    const canceled = task.phase === "canceled";
    const alreadyAvailable = task.phase === "available";
    const showSectionHeader =
      filter === "all" &&
      ((index === 0 && activeCount > 0) ||
        (index === activeCount && finishedCount > 0));
    const sectionLabel =
      index === 0 && activeCount > 0
        ? t("transferCenter.active")
        : t("transferCenter.recent");
    const sectionCount =
      index === 0 && activeCount > 0 ? activeCount : finishedCount;

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
    const startedAt = new Date(task.startedAtMs);
    const percent = task.totalBytes
      ? Math.min(
          100,
          Math.round((task.transferredBytes / task.totalBytes) * 100),
        )
      : 0;

    return (
      <div className="pb-2">
        {showSectionHeader && (
          <div
            className={cn("mb-1.5 flex items-center justify-between px-1", {
              "mt-1": index > 0,
            })}
          >
            <span className="font-medium text-ant-secondary text-xs">
              {sectionLabel}
            </span>
            <span className="rounded-full bg-ant-fill-tertiary px-1.5 py-0.5 text-ant-secondary text-xs tabular-nums">
              {sectionCount}
            </span>
          </div>
        )}
        {/* biome-ignore lint/a11y/useSemanticElements: 卡片内含取消和重试动作，外层使用 role="button" 避开嵌套 button。 */}
        <div
          className="group cursor-pointer rounded-2.5 border border-ant-border bg-ant-container p-2.5 transition-colors hover:border-ant-primary/40 hover:bg-ant-fill-tertiary"
          data-item-id={task.itemId}
          onClick={handleLocate}
          onKeyDown={handleLocateKeyDown}
          role="button"
          tabIndex={0}
        >
          <div className="flex items-start gap-2">
            <div
              className={cn(
                "relative flex size-8 shrink-0 items-center justify-center rounded-2",
                {
                  "bg-ant-error-bg text-ant-error": failed,
                  "bg-ant-fill-secondary text-ant-secondary": canceled,
                  "bg-ant-info-bg text-ant-info": active,
                  "bg-ant-success-bg text-ant-success":
                    !active && !failed && !canceled,
                },
              )}
            >
              <i className={cn("size-4", transferDirectionIconClass(task))} />
              <i
                className={cn(
                  "absolute -right-0.5 -bottom-0.5 size-3 rounded-full bg-ant-container p-0.5",
                  transferRouteIconClass(task),
                )}
              />
            </div>

            <div className="min-w-0 flex-1">
              <div className="flex items-center justify-between gap-2">
                <span className="min-w-0 flex-1 truncate font-medium text-ant-text text-sm">
                  {itemTitle}
                </span>
                <div className="flex shrink-0 items-center gap-1.5">
                  <Tooltip title={t("transferCenter.locateHint")}>
                    <i className="i-lucide:locate-fixed size-3.5 text-ant-tertiary opacity-0 transition-opacity group-hover:opacity-100" />
                  </Tooltip>
                  {active && task.cancellable ? (
                    <button
                      className="cursor-pointer rounded-1.5 border-0 bg-ant-fill-secondary px-1.5 py-0.5 text-ant-text text-xs transition-colors hover:bg-ant-fill"
                      data-task-id={task.id}
                      onClick={handleCancel}
                      type="button"
                    >
                      {t("remoteFile.cancel")}
                    </button>
                  ) : (failed || canceled) &&
                    task.target !== "import" &&
                    task.direction !== "serve" &&
                    (task.direction !== "download" ||
                      item?.kind === "files") ? (
                    <button
                      className="cursor-pointer rounded-1.5 border-0 bg-ant-primary-bg px-1.5 py-0.5 text-ant-primary text-xs transition-colors hover:opacity-85"
                      data-task-id={task.id}
                      onClick={handleRetry}
                      type="button"
                    >
                      {t("transferCenter.retry")}
                    </button>
                  ) : null}
                </div>
              </div>

              <div className="mt-0.5 flex items-center justify-between gap-2 text-ant-secondary text-xs">
                <span className="min-w-0 truncate">
                  {location ? `${location} · ` : ""}
                  {route}
                </span>
                <span
                  className={cn("shrink-0", {
                    "text-ant-error": failed,
                    "text-ant-info": active,
                    "text-ant-success": !active && !failed && !canceled,
                  })}
                >
                  {phase}
                </span>
              </div>

              {active && (
                <>
                  <Progress
                    className="mt-1"
                    percent={percent}
                    showInfo={false}
                    size="small"
                  />
                  <div className="flex items-center justify-between gap-2 text-ant-secondary text-xs tabular-nums">
                    <span>
                      {formatBytes(task.transferredBytes)}
                      {task.totalBytes > 0 &&
                        ` / ${formatBytes(task.totalBytes)}`}
                    </span>
                    <span className="truncate">
                      {formatBytes(task.speedBytesPerSecond)}/s ·{" "}
                      {formatDuration(task.elapsedMs)}
                      {task.etaSeconds !== null
                        ? ` · ${t("remoteFile.eta", {
                            seconds: task.etaSeconds,
                          })}`
                        : ""}
                    </span>
                  </div>
                </>
              )}

              {task.error && (
                <div
                  className="mt-1 truncate rounded-1 bg-ant-error-bg px-1.5 py-0.5 text-ant-error text-xs"
                  title={task.error}
                >
                  {task.error}
                </div>
              )}

              {!active && (
                <div className="mt-1 flex items-center justify-between gap-2 text-ant-secondary text-xs tabular-nums">
                  <span>
                    {alreadyAvailable || !failed
                      ? formatBytes(task.totalBytes)
                      : `${formatBytes(task.transferredBytes)} / ${formatBytes(task.totalBytes)}`}
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
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }

  const hasCatchupActivity =
    backfill !== null || inboundCatchup !== null || servingCatchups.length > 0;
  const hasIndicatorActivity = activeCount > 0 || hasCatchupActivity;

  const filterTabs: Array<{
    count: number;
    key: TransferFilter;
    label: string;
  }> = [
    {
      count: sorted.length,
      key: "all",
      label: t("transferCenter.filter.all"),
    },
    {
      count: activeCount,
      key: "active",
      label: t("transferCenter.filter.active"),
    },
    {
      count: finishedCount,
      key: "finished",
      label: t("transferCenter.filter.finished"),
    },
  ];

  const emptyMessage =
    filter === "active"
      ? t("transferCenter.emptyActive")
      : filter === "finished"
        ? t("transferCenter.emptyFinished")
        : t("transferCenter.empty");

  const content = (
    <HeaderActionPopoverPanel className="gap-2" maxWidth={PANEL_MAX_WIDTH}>
      <div className="flex shrink-0 items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="font-medium text-ant-text text-sm">
            {t("transferCenter.title")}
          </span>
          {activeCount > 0 && (
            <span className="rounded-full bg-ant-info-bg px-1.5 py-0.5 text-ant-info text-xs tabular-nums">
              {activeCount}
            </span>
          )}
          {failedCount > 0 && (
            <span className="rounded-full bg-ant-error-bg px-1.5 py-0.5 text-ant-error text-xs tabular-nums">
              {failedCount}
            </span>
          )}
        </div>

        <Tooltip title={t("transferCenter.clearFinished")}>
          <Button
            aria-label={t("transferCenter.clearFinished")}
            disabled={finishedCount === 0}
            icon={<i className="i-lucide:trash-2 size-4" />}
            loading={clearing}
            onClick={handleClearFinished}
            shape="circle"
            size="small"
            type="text"
          />
        </Tooltip>
      </div>

      {renderCatchupSection()}

      {sorted.length > 0 && (
        <div className="flex shrink-0 items-center gap-1 rounded-2 bg-ant-fill-tertiary p-1">
          {filterTabs.map((tab) => {
            const selected = filter === tab.key;
            return (
              <button
                aria-pressed={selected}
                className={cn(
                  "flex flex-1 cursor-pointer items-center justify-center gap-1 rounded-1.5 border-0 py-1 text-xs transition-colors",
                  {
                    "bg-ant-container font-medium text-ant-text shadow-xs":
                      selected,
                    "bg-transparent text-ant-secondary hover:text-ant-text":
                      !selected,
                  },
                )}
                data-filter={tab.key}
                key={tab.key}
                onClick={handleSelectFilter}
                type="button"
              >
                <span>{tab.label}</span>
                <span className="tabular-nums opacity-75">{tab.count}</span>
              </button>
            );
          })}
        </div>
      )}

      {filteredTasks.length === 0 ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-1.5 text-center text-ant-secondary text-sm">
          <i className="i-lucide:inbox size-6 text-ant-tertiary" />
          <span>{emptyMessage}</span>
        </div>
      ) : (
        <Virtuoso
          className="min-h-0 flex-1"
          computeItemKey={getTransferTaskKey}
          data={filteredTasks}
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
            hasIndicatorActivity
              ? "text-ant-info"
              : "text-ant-tertiary group-hover:text-ant-text",
          )}
        />
        {activeCount > 0 ? (
          <span className="pointer-events-none absolute top-0.5 right-0.5 origin-top-right scale-75 font-semibold text-ant-info text-xs tabular-nums leading-none">
            {Math.min(activeCount, 9)}
          </span>
        ) : hasCatchupActivity ? (
          <span className="absolute top-1 right-1 size-1.5 rounded-full bg-ant-info" />
        ) : null}
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

function formatInboundCatchupSources(
  sources: SyncInboundCatchupSources | undefined,
  t: TFunction<"clipboard">,
) {
  if (!sources) return null;

  const labels = [...sources.peers];
  if (sources.cloud) {
    labels.push(t("transferCenter.route.cloud"));
  }
  if (labels.length === 0) return null;

  return t("transferCenter.backfill.fromSource", {
    source: labels.join(t("transferCenter.backfill.sourceSeparator")),
  });
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
