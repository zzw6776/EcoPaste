import { Progress } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import type { StorageUsage } from "@/commands";
import { cn } from "@/utils/cn";
import type { PreferenceStorageState } from "../types/preferences";
import {
  formatBytes,
  storageMeterPercent,
  storageTargetBytes,
  storageTone,
} from "../utils/storageUsage";

interface PreferenceStorageUsagePanelProps {
  state: PreferenceStorageState;
  storageUsage: StorageUsage | null;
}

/**
 * 侧栏里的本地存储摘要，展示当前环境数据目录的递归占用。
 */
const PreferenceStorageUsagePanel: FC<PreferenceStorageUsagePanelProps> = (
  props,
) => {
  const { t } = useTranslation("preferences");
  const { state, storageUsage } = props;
  const isReady = state === "ready" && storageUsage !== null;
  const totalLabel = storageUsage ? formatBytes(storageUsage.totalBytes) : "--";
  const targetLabel = storageUsage
    ? formatBytes(storageTargetBytes(storageUsage.totalBytes))
    : "--";
  const usageLabel =
    state === "loading"
      ? t("storage.loading")
      : t("storage.usage", { target: targetLabel, total: totalLabel });
  const meterPercent = isReady
    ? storageMeterPercent(storageUsage.totalBytes)
    : 20;
  const storageToneValue = isReady
    ? storageTone(storageUsage.totalBytes)
    : {
        stroke: "var(--ant-color-success)",
        text: "text-ant-success",
      };
  const meterStrokeColor =
    state === "error" ? "var(--ant-color-error)" : storageToneValue.stroke;

  return (
    <div className="px-3 pb-3">
      <div className="rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary px-3 py-3">
        <div className="flex min-w-0 items-start gap-2.5">
          <span
            className={cn(
              "flex size-7 shrink-0 items-center justify-center text-lg",
              state === "error" ? "text-ant-error" : storageToneValue.text,
            )}
          >
            <i aria-hidden="true" className="i-lucide:hard-drive" />
          </span>
          <div className="min-w-0 flex-1">
            <div className="truncate font-medium text-ant-text text-sm leading-tight">
              {t("storage.title")}
            </div>
            <div
              className={cn(
                "mt-1 truncate font-medium text-xs leading-tight",
                state === "error" ? "text-ant-error" : "text-ant-secondary",
              )}
            >
              {state === "error" ? t("storage.error") : usageLabel}
            </div>
          </div>
        </div>

        <Progress
          aria-label={usageLabel}
          className="mt-3 leading-none"
          percent={meterPercent}
          railColor="var(--ant-color-fill-secondary)"
          showInfo={false}
          size={["100%", 4]}
          status={state === "error" ? "exception" : "normal"}
          strokeColor={meterStrokeColor}
        />
      </div>
    </div>
  );
};

export default PreferenceStorageUsagePanel;
