import { Progress } from "antd";
import type { TFunction } from "i18next";
import type { FC, MouseEvent } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useSnapshot } from "valtio";
import {
  cancelSyncTransfer,
  downloadRemoteFiles,
  type TransferTask,
} from "@/commands";
import AssetImage from "@/components/AssetImage";
import Highlight from "@/components/Highlight";
import {
  isTransferActive,
  transferRouteIconClass,
} from "@/pages/Clipboard/utils/transferTasks";
import { formatBytes } from "@/pages/Preference/utils/storageUsage";
import { clipboardViewState } from "@/stores/clipboardView";
import type { ClipboardItem, FileEntry } from "@/types/clipboard";
import { cn } from "@/utils/cn";
import ImageCard from "./ImageCard";

type FilesCardProps = ClipboardItem & {
  downloadedExternally?: boolean;
  externalFileMissing?: boolean;
  transferTasks?: TransferTask[];
};

/**
 * 文件类卡片：`fileEntries` 与 `filesPreviewKind` 均由 Rust 命令层预处理，
 * 前端只按 kind 分支渲染：`imagePreview` 走单图预览，`list` 走文件列表；
 * 路径已删除（`exists = false`）的条目一律走 list 并对文件名划删除线。
 */
const FilesCard: FC<FilesCardProps> = (props) => {
  const entries = props.fileEntries ?? [];
  const { keyword } = useSnapshot(clipboardViewState);
  const { t } = useTranslation("clipboard");
  const [starting, setStarting] = useState(false);
  const downloadTask = props.transferTasks?.find((task) => {
    return task.direction === "download";
  });
  const uploadTasks = props.transferTasks?.filter((task) => {
    return task.direction === "upload" && isTransferActive(task);
  });

  async function startDownload(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    setStarting(true);
    try {
      await downloadRemoteFiles(props.id);
    } catch {
      // 命令层已统一显示错误。
    } finally {
      setStarting(false);
    }
  }

  async function cancelTransfer(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    const taskId = event.currentTarget.dataset.taskId;
    if (!taskId) return;
    try {
      await cancelSyncTransfer(taskId);
    } catch {
      // 命令层已统一显示错误。
    }
  }

  function stopPropagation(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
  }

  if (props.filesPreviewKind === "imagePreview") {
    const [first] = entries;
    return (
      <ImageCard
        {...props}
        imageThumbnailPath={props.imageThumbnailPath ?? first?.path}
      />
    );
  }

  return (
    <div className="relative flex flex-col gap-1">
      {props.remoteFileState && (
        <RemoteDownloadStatus
          downloadedExternally={props.downloadedExternally === true}
          expired={props.remoteFileState === "expired"}
          externalFileMissing={props.externalFileMissing === true}
          onCancel={cancelTransfer}
          onDownload={startDownload}
          onMouseDown={stopPropagation}
          starting={starting}
          t={t}
          task={downloadTask}
        />
      )}
      {uploadTasks?.map((task) => {
        return (
          <UploadTransferStatus
            key={`${task.operationId}:${task.route}:${task.target}`}
            onCancel={cancelTransfer}
            onMouseDown={stopPropagation}
            t={t}
            task={task}
          />
        );
      })}
      {entries.map((entry) => {
        return <FileRow entry={entry} key={entry.entryKey} keyword={keyword} />;
      })}
    </div>
  );
};

interface UploadTransferStatusProps {
  onCancel: (event: MouseEvent<HTMLButtonElement>) => void;
  onMouseDown: (event: MouseEvent<HTMLButtonElement>) => void;
  t: TFunction<"clipboard">;
  task: TransferTask;
}

const UploadTransferStatus: FC<UploadTransferStatusProps> = (props) => {
  const { onCancel, onMouseDown, t, task } = props;
  const percent = task.totalBytes
    ? Math.min(100, Math.round((task.transferredBytes / task.totalBytes) * 100))
    : 0;
  const detail = transferDetail(task);
  const phaseLabel =
    task.target === "import"
      ? t(`transfer.import.${task.phase}`)
      : t(`transfer.upload.${task.phase}`);

  return (
    <div className="mb-1 flex flex-col gap-1 text-ant-secondary text-xs">
      <div className="flex items-center gap-1">
        <i className={cn("size-3.5", transferRouteIconClass(task))} />
        <span>{phaseLabel}</span>
        {task.cancellable && (
          <button
            className="ml-auto cursor-pointer border-0 bg-transparent p-0 text-ant-primary text-xs"
            data-task-id={task.id}
            onClick={onCancel}
            onMouseDown={onMouseDown}
            type="button"
          >
            {t("remoteFile.cancel")}
          </button>
        )}
      </div>
      <Progress percent={percent} showInfo={false} size="small" />
      <span className="truncate" title={detail}>
        {detail}
        {task.etaSeconds !== null
          ? ` · ${t("remoteFile.eta", { seconds: task.etaSeconds })}`
          : ""}
      </span>
    </div>
  );
};

interface RemoteDownloadStatusProps {
  downloadedExternally: boolean;
  expired: boolean;
  externalFileMissing: boolean;
  onCancel: (event: MouseEvent<HTMLButtonElement>) => void;
  onDownload: (event: MouseEvent<HTMLButtonElement>) => void;
  onMouseDown: (event: MouseEvent<HTMLButtonElement>) => void;
  starting: boolean;
  task?: TransferTask;
  t: TFunction<"clipboard">;
}

const RemoteDownloadStatus: FC<RemoteDownloadStatusProps> = (props) => {
  const {
    downloadedExternally,
    expired,
    externalFileMissing,
    onCancel,
    onDownload,
    onMouseDown,
    starting,
    task,
    t,
  } = props;
  const active = task && isTransferActive(task);
  const percent = task?.totalBytes
    ? Math.min(100, Math.round((task.transferredBytes / task.totalBytes) * 100))
    : 0;
  const detail = task ? transferDetail(task) : null;
  const canCancel = active && task.cancellable;
  const actionLabel = canCancel
    ? t("remoteFile.cancel")
    : t(
        downloadedExternally
          ? "remoteFile.downloadAgain"
          : "remoteFile.download",
      );

  return (
    <div className="mb-1 flex flex-col gap-1 text-ant-secondary text-xs">
      <div className="flex items-center gap-1">
        <i
          className={cn(
            "size-3.5",
            task?.route === "lan" ? "i-lucide:wifi" : "i-lucide:cloud-download",
          )}
        />
        <span>
          {expired
            ? t("remoteFile.expired")
            : externalFileMissing && !active
              ? t("remoteFile.externalMissing")
              : downloadedExternally && !active
                ? t("remoteFile.downloadedExternally")
                : t(`remoteFile.${task?.phase ?? "notDownloaded"}`)}
        </span>
        {!expired && (!active || canCancel) && (
          <button
            className="ml-auto cursor-pointer border-0 bg-transparent p-0 text-ant-primary text-xs"
            data-task-id={task?.id}
            disabled={starting}
            onClick={canCancel ? onCancel : onDownload}
            onMouseDown={onMouseDown}
            type="button"
          >
            {actionLabel}
          </button>
        )}
      </div>
      {task && active && (
        <>
          <Progress percent={percent} showInfo={false} size="small" />
          <span className="truncate" title={detail ?? void 0}>
            {detail}
            {task.etaSeconds !== null
              ? ` · ${t("remoteFile.eta", { seconds: task.etaSeconds })}`
              : ""}
          </span>
        </>
      )}
      {task?.phase === "failed" && task.error && (
        <span className="truncate text-ant-error" title={task.error}>
          {task.error}
        </span>
      )}
    </div>
  );
};

function formatDuration(milliseconds: number) {
  const totalSeconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
}

function transferDetail(task: TransferTask) {
  const total = task.totalBytes > 0 ? ` / ${formatBytes(task.totalBytes)}` : "";
  return `${formatBytes(task.transferredBytes)}${total} · ${formatBytes(task.speedBytesPerSecond)}/s · ${formatDuration(task.elapsedMs)}`;
}

interface FileRowProps {
  entry: FileEntry;
  keyword: string;
}

const FileRow: FC<FileRowProps> = (props) => {
  const { entry, keyword } = props;

  return (
    <div className="flex items-center gap-1 truncate" title={entry.path}>
      <AssetImage className="size-5" src={entry.iconPath} />

      <Highlight
        className={cn("truncate", {
          "line-through": !entry.exists && !entry.isRemote,
        })}
        keyword={keyword}
        text={entry.name}
      />
    </div>
  );
};

export default FilesCard;
