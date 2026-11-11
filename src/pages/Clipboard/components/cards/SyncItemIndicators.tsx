import type { FC, MouseEvent } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  downloadRemoteFiles,
  type SyncItemChannelStatus,
  type SyncItemStatus,
  type SyncTarget,
  syncItemNow,
  type TransferTask,
} from "@/commands";
import Tooltip from "@/components/Tooltip";
import { isTransferActive } from "@/pages/Clipboard/utils/transferTasks";
import { cn } from "@/utils/cn";
import { getMessageApi } from "@/utils/feedback";

interface SyncItemIndicatorsProps {
  itemId: string;
  onChange?: (status: SyncItemStatus) => void;
  status?: SyncItemStatus;
  transferTasks: TransferTask[];
}

const IDLE_CHANNEL: SyncItemChannelStatus = {
  action: "none",
  activity: "idle",
  availability: "none",
  contentTargets: 0,
  deliveredTargets: 0,
  lastError: null,
  totalTargets: 0,
};

const IDLE_STATUS: Omit<SyncItemStatus, "itemId"> = {
  cloud: IDLE_CHANNEL,
  lan: IDLE_CHANNEL,
  localState: "managed",
  origin: "local",
};

const SyncItemIndicators: FC<SyncItemIndicatorsProps> = (props) => {
  const { itemId, onChange, status, transferTasks } = props;
  const { t } = useTranslation("clipboard");
  const [activeTarget, setActiveTarget] = useState<SyncTarget | null>(null);
  const resolvedStatus = status ?? { ...IDLE_STATUS, itemId };

  async function synchronize(
    event: MouseEvent<HTMLButtonElement>,
    target: SyncTarget,
  ) {
    event.stopPropagation();
    const channel = resolvedStatus[target];
    if (channel.action === "none") return;

    setActiveTarget(target);
    try {
      if (channel.action === "download") {
        await downloadRemoteFiles(itemId, target);
        return;
      }
      const next = await syncItemNow(itemId, target);
      onChange?.(next);
      getMessageApi().success(t(`syncItem.${target}.started`));
    } catch {
      // 命令层已统一记录并显示错误。
    } finally {
      setActiveTarget(null);
    }
  }

  function stopPropagation(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
  }

  function handleLanClick(event: MouseEvent<HTMLButtonElement>) {
    void synchronize(event, "lan");
  }

  function handleCloudClick(event: MouseEvent<HTMLButtonElement>) {
    void synchronize(event, "cloud");
  }

  function renderChannel(
    target: SyncTarget,
    icon: string,
    onClick: (event: MouseEvent<HTMLButtonElement>) => void,
  ) {
    const transferTask = transferTasks.find((task) => {
      return task.target !== "import" && task.route === target;
    });
    const transferActive =
      transferTask !== void 0 &&
      transferTask.phase !== "confirming" &&
      isTransferActive(transferTask);
    return (
      <ChannelButton
        channel={resolvedStatus[target]}
        disabled={activeTarget !== null}
        icon={icon}
        loading={activeTarget === target || transferActive}
        onClick={onClick}
        onMouseDown={stopPropagation}
        origin={resolvedStatus.origin}
        target={target}
        transferTask={transferTask}
      />
    );
  }

  return (
    <div className="flex items-center gap-0.5">
      {renderChannel("lan", "i-lucide:wifi", handleLanClick)}
      {renderChannel("cloud", "i-lucide:cloud", handleCloudClick)}
    </div>
  );
};

interface ChannelButtonProps {
  channel: SyncItemChannelStatus;
  disabled: boolean;
  icon: string;
  loading: boolean;
  onClick: (event: MouseEvent<HTMLButtonElement>) => void;
  onMouseDown: (event: MouseEvent<HTMLButtonElement>) => void;
  origin: SyncItemStatus["origin"];
  target: SyncTarget;
  transferTask?: TransferTask;
}

const ChannelButton: FC<ChannelButtonProps> = (props) => {
  const {
    channel,
    disabled,
    icon,
    loading,
    onClick,
    onMouseDown,
    origin,
    target,
    transferTask,
  } = props;
  const { t } = useTranslation("clipboard");
  let activity: SyncItemChannelStatus["activity"] | "syncing" =
    channel.activity;
  if (loading) {
    activity = "syncing";
  }
  const actionable = !disabled && channel.action !== "none";
  const titleState = channelTitleState(
    channel,
    activity,
    origin,
    transferTask?.direction,
  );
  const lastError = channel.lastError;
  const title = lastError
    ? t(`syncItem.${target}.errorWithReason`, { reason: lastError })
    : t(`syncItem.${target}.${titleState}`, {
        content: channel.contentTargets,
        delivered: channel.deliveredTargets,
        total: channel.totalTargets,
      });

  return (
    <Tooltip mouseEnterDelay={0.3} title={title}>
      <span className="inline-flex size-5">
        <button
          aria-label={title}
          className={cn(
            "flex size-full items-center justify-center rounded-1.5 border-0 bg-transparent transition-colors",
            channelClassName(channel, activity),
            actionable && "cursor-pointer hover:bg-ant-fill-tertiary",
          )}
          disabled={!actionable}
          onClick={onClick}
          onMouseDown={onMouseDown}
          type="button"
        >
          <i
            className={cn(
              "size-3.5",
              icon,
              activity === "syncing" && "animate-pulse",
            )}
          />
        </button>
      </span>
    </Tooltip>
  );
};

function channelClassName(
  channel: SyncItemChannelStatus,
  activity: SyncItemChannelStatus["activity"] | "syncing",
) {
  if (activity === "error") return "text-ant-error";

  switch (channel.availability) {
    case "complete":
      return "text-ant-success";
    case "metadataOnly":
    case "partial":
      return "text-ant-info";
    default:
      break;
  }
  if (activity === "syncing") return "text-ant-info";
  if (activity === "queued") return "text-ant-secondary";
  return channel.action === "upload" ? "text-ant-warning" : "text-ant-tertiary";
}

function channelTitleState(
  channel: SyncItemChannelStatus,
  activity: SyncItemChannelStatus["activity"] | "syncing",
  origin: SyncItemStatus["origin"],
  activeDirection?: TransferTask["direction"],
) {
  if (activity === "syncing" && activeDirection === "download") {
    return "downloading";
  }
  if (activity !== "idle") return activity;
  if (origin === "remote") {
    if (channel.availability === "complete") return "remoteSuccess";
    if (channel.availability === "metadataOnly") return "remoteMetadataOnly";
  }

  if (channel.availability === "complete") return "success";
  if (channel.availability === "partial") return "partial";
  if (channel.availability === "metadataOnly") return "metadataOnly";
  if (channel.action === "upload") return "manual";
  return "idle";
}

export default SyncItemIndicators;
