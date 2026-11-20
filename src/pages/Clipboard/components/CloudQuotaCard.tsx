import { Button, Progress, theme } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import type { CloudHubStatus } from "@/commands";
import Tooltip from "@/components/Tooltip";
import { formatBytes } from "@/pages/Preference/utils/storageUsage";
import { cn } from "@/utils/cn";
import {
  formatCompactTimestamp,
  resolveUsagePercent,
  resolveUsageTone,
  type UsageTone,
} from "./SyncStatusIcons.logic";

interface CloudQuotaCardProps {
  compact?: boolean;
  error?: boolean;
  loading?: boolean;
  onRefresh?: () => void;
  status: CloudHubStatus | null;
}

/** 展示云端 Hub 的记录条数限制、存储容量限制及保留周期。 */
const CloudQuotaCard: FC<CloudQuotaCardProps> = (props) => {
  const {
    compact = false,
    error = false,
    loading = false,
    onRefresh,
    status,
  } = props;
  const { t, i18n } = useTranslation("clipboard");
  const { token } = theme.useToken();

  const eventPercent = status
    ? resolveUsagePercent(status.eventCount, status.maxEvents)
    : null;
  const bytesPercent = status
    ? resolveUsagePercent(status.usedBytes, status.maxBytes)
    : null;
  const eventTone = resolveUsageTone(eventPercent);
  const bytesTone = resolveUsageTone(bytesPercent);
  const oldestCompact = formatCompactTimestamp(
    status?.oldestEventAt,
    i18n.resolvedLanguage,
  );
  const oldestFull = status?.oldestEventAt
    ? new Date(status.oldestEventAt).toLocaleString(i18n.resolvedLanguage)
    : void 0;
  const retentionLabel = status
    ? t("syncStatus.cloud.quota.retention", {
        days:
          status.retentionDays > 0
            ? t("syncStatus.devices.days", { count: status.retentionDays })
            : t("syncStatus.cloud.quota.retentionForever"),
      })
    : null;

  function resolveStrokeColor(tone: UsageTone) {
    if (tone === "error") return token.colorError;
    if (tone === "warning") return token.colorWarning;

    return token.colorPrimary;
  }

  function renderMetricBar(
    label: string,
    summaryText: string,
    percent: number | null,
    tone: UsageTone,
  ) {
    return (
      <div className="flex flex-col gap-1">
        <div className="flex items-center justify-between gap-2 text-xs">
          <span className="text-ant-secondary">{label}</span>
          <span
            className={cn("font-medium tabular-nums", {
              "text-ant-error": tone === "error",
              "text-ant-text": tone === "normal",
              "text-ant-warning": tone === "warning",
            })}
          >
            {summaryText}
          </span>
        </div>
        {percent !== null ? (
          <Progress
            className="m-0!"
            percent={percent}
            showInfo={false}
            size="small"
            strokeColor={resolveStrokeColor(tone)}
          />
        ) : null}
      </div>
    );
  }

  return (
    <div
      className={cn(
        "rounded-2.5 border border-ant-border bg-ant-container",
        compact ? "p-2.5" : "p-3",
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <i className="i-lucide:database size-3.5 shrink-0 text-ant-info" />
          <span className="truncate font-medium text-ant-text text-xs">
            {t("syncStatus.cloud.quota.title")}
          </span>
        </div>
        {onRefresh ? (
          <Tooltip title={t("syncStatus.cloud.quota.refresh")}>
            <Button
              aria-label={t("syncStatus.cloud.quota.refresh")}
              icon={
                <i
                  className={cn(
                    "i-lucide:refresh-cw size-3.5",
                    loading && "animate-spin",
                  )}
                />
              }
              onClick={onRefresh}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
        ) : null}
      </div>

      {status ? (
        <div className="mt-2 flex flex-col gap-2">
          {renderMetricBar(
            t("syncStatus.cloud.quota.eventsCount"),
            eventPercent !== null
              ? t("syncStatus.cloud.quota.eventsUsage", {
                  max: status.maxEvents.toLocaleString(),
                  percent: eventPercent,
                  used: status.eventCount.toLocaleString(),
                })
              : t("syncStatus.cloud.quota.eventsUnlimited", {
                  count: status.eventCount.toLocaleString(),
                }),
            eventPercent,
            eventTone,
          )}

          {renderMetricBar(
            t("syncStatus.cloud.quota.storageSpace"),
            bytesPercent !== null
              ? t("syncStatus.cloud.quota.storageUsage", {
                  max: formatBytes(status.maxBytes),
                  percent: bytesPercent,
                  used: formatBytes(status.usedBytes),
                })
              : t("syncStatus.cloud.quota.storageUnlimited", {
                  used: formatBytes(status.usedBytes),
                }),
            bytesPercent,
            bytesTone,
          )}

          <div className="flex flex-wrap items-center justify-between gap-x-2 gap-y-1 border-ant-border-secondary border-t pt-1.5 text-ant-secondary text-xs tabular-nums">
            <span className="flex items-center gap-1.5">
              <span>{retentionLabel}</span>
              <span className="text-ant-tertiary">·</span>
              <span>
                {t("syncStatus.cloud.quota.blobs", {
                  count: status.blobCount.toLocaleString(),
                })}
              </span>
            </span>
            <span className="shrink-0" title={oldestFull}>
              {oldestCompact
                ? t("syncStatus.cloud.quota.oldest", { time: oldestCompact })
                : t("syncStatus.devices.noRetainedEvents")}
            </span>
          </div>
        </div>
      ) : loading ? (
        <div className="mt-2 flex items-center gap-1.5 text-ant-secondary text-xs">
          <i className="i-lucide:refresh-cw size-3.5 animate-spin text-ant-info" />
          <span>{t("syncStatus.cloud.quota.loading")}</span>
        </div>
      ) : error ? (
        <div className="mt-2 flex items-center justify-between gap-2 text-xs">
          <span className="text-ant-secondary">
            {t("syncStatus.cloud.quota.fetchFailed")}
          </span>
          {onRefresh ? (
            <button
              className="cursor-pointer rounded-1.5 border-0 bg-ant-fill-secondary px-2 py-0.5 text-ant-text text-xs transition-colors hover:bg-ant-fill"
              onClick={onRefresh}
              type="button"
            >
              {t("syncStatus.actions.retry")}
            </button>
          ) : null}
        </div>
      ) : null}
    </div>
  );
};

export default CloudQuotaCard;
