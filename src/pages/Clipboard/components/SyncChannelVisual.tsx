import { App } from "antd";
import type { FC, MouseEvent } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { SyncChannelState } from "@/commands";
import Tooltip from "@/components/Tooltip";
import { cn } from "@/utils/cn";

/** 返回通道状态的文字颜色类名。 */
export function channelTextClassName(state: SyncChannelState) {
  switch (state) {
    case "online":
      return "text-ant-success";
    case "connecting":
      return "text-ant-info";
    case "degraded":
      return "text-ant-warning";
    case "error":
      return "text-ant-error";
    default:
      return "text-ant-tertiary";
  }
}

/** 返回通道状态的柔和背景 + 文字徽章类名。 */
export function channelBadgeClassName(state: SyncChannelState) {
  switch (state) {
    case "online":
      return "bg-ant-success-bg text-ant-success";
    case "connecting":
      return "bg-ant-info-bg text-ant-info";
    case "degraded":
      return "bg-ant-warning-bg text-ant-warning";
    case "error":
      return "bg-ant-error-bg text-ant-error";
    default:
      return "bg-ant-fill-secondary text-ant-secondary";
  }
}

interface CopyChipProps {
  className?: string;
  displayText?: string;
  prefix?: string;
  value: string;
}

/** 带一键复制和已复制反馈的等宽信息胶囊。 */
export const CopyChip: FC<CopyChipProps> = (props) => {
  const { className, displayText, prefix, value } = props;
  const { message } = App.useApp();
  const { t } = useTranslation("clipboard");
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    return () => {
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
      }
    };
  }, []);

  async function handleCopy(event: MouseEvent<HTMLButtonElement>) {
    event.stopPropagation();
    try {
      await navigator.clipboard?.writeText(value);
      setCopied(true);
      void message.success(t("syncStatus.actions.copied"));
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
      }
      timerRef.current = window.setTimeout(() => {
        timerRef.current = null;
        setCopied(false);
      }, 1500);
    } catch {
      // 某些无剪贴板权限环境静默忽略。
    }
  }

  return (
    <Tooltip
      title={`${copied ? t("syncStatus.actions.copied") : t("syncStatus.actions.copy")}: ${value}`}
    >
      <button
        className={cn(
          "inline-flex max-w-full cursor-pointer items-center gap-1 rounded-1.5 border border-ant-border-secondary bg-ant-fill-quaternary px-1.5 py-0.5 text-left text-ant-secondary text-xs transition-colors hover:border-ant-border hover:text-ant-text",
          className,
        )}
        onClick={handleCopy}
        type="button"
      >
        {prefix ? <span className="shrink-0">{prefix}</span> : null}
        <span className="min-w-0 truncate font-mono">
          {displayText ?? value}
        </span>
        <i
          className={cn(
            "size-3 shrink-0",
            copied
              ? "i-lucide:check text-ant-success"
              : "i-lucide:copy text-ant-tertiary",
          )}
        />
      </button>
    </Tooltip>
  );
};
