import type { FC, MouseEvent } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type SyncItemChannelStatus,
  type SyncItemStatus,
  type SyncTarget,
  syncItemNow,
} from "@/commands";
import { cn } from "@/utils/cn";
import { getMessageApi } from "@/utils/feedback";

interface SyncItemIndicatorsProps {
  itemId: string;
  onChange?: (status: SyncItemStatus) => void;
  status?: SyncItemStatus;
}

const IDLE_CHANNEL: SyncItemChannelStatus = {
  deliveredTargets: 0,
  lastError: null,
  state: "idle",
  totalTargets: 0,
};

const SyncItemIndicators: FC<SyncItemIndicatorsProps> = (props) => {
  const { itemId, onChange, status } = props;
  const { t } = useTranslation("clipboard");
  const [activeTarget, setActiveTarget] = useState<SyncTarget | null>(null);

  async function synchronize(
    event: MouseEvent<HTMLButtonElement>,
    target: SyncTarget,
  ) {
    event.stopPropagation();
    const channel = status?.[target] ?? IDLE_CHANNEL;
    if (channel.state !== "manual" && channel.state !== "error") return;

    setActiveTarget(target);
    try {
      const next = await syncItemNow(itemId, target);
      onChange?.(next);
      getMessageApi().success(t(`syncItem.${target}.queued`));
    } catch {
      // 命令层已统一记录并显示错误。
    } finally {
      setActiveTarget(null);
    }
  }

  function stopPropagation(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
  }

  return (
    <div className="flex items-center gap-0.5">
      <ChannelButton
        channel={status?.lan ?? IDLE_CHANNEL}
        disabled={activeTarget !== null}
        icon="i-lucide:wifi"
        loading={activeTarget === "lan"}
        onClick={(event) => {
          void synchronize(event, "lan");
        }}
        onMouseDown={stopPropagation}
        target="lan"
      />
      <ChannelButton
        channel={status?.cloud ?? IDLE_CHANNEL}
        disabled={activeTarget !== null}
        icon="i-lucide:cloud"
        loading={activeTarget === "cloud"}
        onClick={(event) => {
          void synchronize(event, "cloud");
        }}
        onMouseDown={stopPropagation}
        target="cloud"
      />
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
  target: SyncTarget;
}

const ChannelButton: FC<ChannelButtonProps> = (props) => {
  const { channel, disabled, icon, loading, onClick, onMouseDown, target } =
    props;
  const { t } = useTranslation("clipboard");
  const state = loading ? "syncing" : channel.state;
  const actionable = !disabled && (state === "manual" || state === "error");
  const title = channel.lastError
    ? t(`syncItem.${target}.errorWithReason`, { reason: channel.lastError })
    : t(`syncItem.${target}.${state}`, {
        delivered: channel.deliveredTargets,
        total: channel.totalTargets,
      });

  return (
    <button
      aria-label={title}
      className={cn(
        "flex size-5 items-center justify-center rounded-1.5 border-0 bg-transparent transition-colors",
        channelClassName(state),
        actionable && "cursor-pointer hover:bg-ant-fill-tertiary",
      )}
      disabled={!actionable}
      onClick={onClick}
      onMouseDown={onMouseDown}
      title={title}
      type="button"
    >
      <i
        className={cn("size-3.5", icon, state === "syncing" && "animate-pulse")}
      />
    </button>
  );
};

function channelClassName(state: SyncItemChannelStatus["state"]) {
  switch (state) {
    case "success":
      return "text-ant-success";
    case "syncing":
      return "text-ant-info";
    case "manual":
      return "text-ant-warning";
    case "error":
      return "text-ant-error";
    default:
      return "text-ant-tertiary";
  }
}

export default SyncItemIndicators;
