import { useMount } from "ahooks";
import { Progress } from "antd";
import type { FC, MouseEvent } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  cancelSyncTransfer,
  downloadRemoteFiles,
  listSyncTransfers,
  syncItemNow,
  type TransferTask,
} from "@/commands";
import Popover from "@/components/Popover";
import { TAURI_EVENT } from "@/constants/events";
import { useTauriListen } from "@/hooks/useTauriListen";
import { formatBytes } from "@/pages/Preference/utils/storageUsage";
import { cn } from "@/utils/cn";
import {
  aggregateTransferTasks,
  isTransferActive,
  mergeTransferTaskSnapshot,
  transferRouteIconClass,
  upsertTransferTask,
} from "../utils/transferTasks";

interface TransferCenterProps {
  compact?: boolean;
}

const TransferCenter: FC<TransferCenterProps> = (props) => {
  const { compact = false } = props;
  const { t } = useTranslation("clipboard");
  const [tasks, setTasks] = useState(() => new Map<string, TransferTask>());

  useMount(() => {
    async function loadTransfers() {
      const current = await listSyncTransfers();
      setTasks((tasks) => {
        return mergeTransferTaskSnapshot(tasks, current);
      });
    }

    void loadTransfers();
  });
  useTauriListen<TransferTask>(TAURI_EVENT.SYNC_TRANSFER_UPDATED, (event) => {
    setTasks((current) => {
      return upsertTransferTask(current, event.payload);
    });
  });

  const aggregated = aggregateTransferTasks([...tasks.values()]);
  const activeCount = aggregated.filter((task) => {
    return isTransferActive(task);
  }).length;
  const sorted = aggregated.reverse().slice(0, 10);

  async function handleCancel(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    const task = tasks.get(event.currentTarget.dataset.taskId ?? "");
    if (!task) return;
    try {
      await cancelSyncTransfer(task.id);
    } catch {
      // 命令层已统一显示错误。
    }
  }

  async function handleRetry(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    const task = tasks.get(event.currentTarget.dataset.taskId ?? "");
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

  const content = (
    <div className="flex max-h-80 w-72 flex-col gap-2 overflow-y-auto">
      {sorted.length === 0 ? (
        <span className="py-4 text-center text-ant-secondary text-sm">
          {t("transferCenter.empty")}
        </span>
      ) : (
        sorted.map((task) => {
          const active = isTransferActive(task);
          const percent = task.totalBytes
            ? Math.min(
                100,
                Math.round((task.transferredBytes / task.totalBytes) * 100),
              )
            : 0;
          return (
            <div
              className="rounded-2 border border-ant-border bg-ant-container p-2"
              key={task.id}
            >
              <div className="flex items-center gap-1 text-sm">
                <i
                  className={cn(
                    "size-3.5 shrink-0",
                    transferRouteIconClass(task),
                  )}
                />
                <span className="min-w-0 flex-1 truncate">
                  {task.fileName ??
                    t(`transferCenter.${task.direction}.${task.phase}`)}
                </span>
                {active && task.cancellable ? (
                  <button
                    className="cursor-pointer border-0 bg-transparent p-0 text-ant-primary text-xs"
                    data-task-id={task.id}
                    onClick={handleCancel}
                    type="button"
                  >
                    {t("remoteFile.cancel")}
                  </button>
                ) : task.phase === "failed" || task.phase === "canceled" ? (
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
              <Progress percent={percent} showInfo={false} size="small" />
              <div className="truncate text-ant-secondary text-xs">
                {formatBytes(task.transferredBytes)} /{" "}
                {formatBytes(task.totalBytes)}
                {active &&
                  ` · ${formatBytes(task.speedBytesPerSecond)}/s · ${formatDuration(task.elapsedMs)}`}
                {active && task.etaSeconds !== null
                  ? ` · ${t("remoteFile.eta", { seconds: task.etaSeconds })}`
                  : ""}
              </div>
              {task.error && (
                <div
                  className="truncate text-ant-error text-xs"
                  title={task.error}
                >
                  {task.error}
                </div>
              )}
            </div>
          );
        })
      )}
    </div>
  );

  return (
    <Popover content={content} placement="bottomRight" trigger="click">
      <button
        aria-label={t("transferCenter.title")}
        className={cn(
          "relative flex cursor-pointer items-center justify-center border-0 bg-transparent text-ant-secondary",
          compact ? "size-9 rounded-xl" : "size-7 rounded-lg",
        )}
        type="button"
      >
        <i className="i-lucide:arrow-up-down size-4" />
        {activeCount > 0 && (
          <span className="absolute top-0 right-0 flex size-4 items-center justify-center rounded-full bg-ant-primary text-[10px] text-white">
            {Math.min(activeCount, 9)}
          </span>
        )}
      </button>
    </Popover>
  );
};

function formatDuration(milliseconds: number) {
  const totalSeconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
}

export default TransferCenter;
