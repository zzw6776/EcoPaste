import { Button } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import type {
  CloudDeviceDirectory,
  SyncChannelState,
  SyncStatus,
} from "@/commands";
import ScrollArea from "@/components/ScrollArea";
import Tooltip from "@/components/Tooltip";
import { cn } from "@/utils/cn";
import CloudQuotaCard from "./CloudQuotaCard";
import { CopyChip, channelBadgeClassName } from "./SyncChannelVisual";
import { formatCompactTimestamp } from "./SyncStatusIcons.logic";

interface SyncCloudDetailsProps {
  directory: CloudDeviceDirectory | null;
  directoryError: boolean;
  directoryLoading: boolean;
  onOpenDetails: () => void;
  onOpenDevices: () => void;
  onOpenRecords: () => void;
  onOpenSettings: () => void;
  onReconnect: () => void;
  onRefreshDirectory: () => void;
  reconnecting: boolean;
  showHeader?: boolean;
  status: SyncStatus | null;
}

/** 云端同步一级悬浮框菜单：仅呈现核心状态、条数容量限额及二级独立弹窗入口。 */
const SyncCloudDetails: FC<SyncCloudDetailsProps> = (props) => {
  const {
    directory,
    directoryError,
    directoryLoading,
    onOpenDetails,
    onOpenDevices,
    onOpenRecords,
    onOpenSettings,
    onReconnect,
    onRefreshDirectory,
    reconnecting,
    showHeader = true,
    status,
  } = props;
  const { t, i18n } = useTranslation("clipboard");

  const cloud = status?.cloud;
  const cloudState: SyncChannelState = cloud?.state ?? "disabled";
  const watchState: SyncChannelState = status?.cloudWatch.state ?? "disabled";
  const isConfigured = Boolean(
    status?.enabled &&
      status.cloudEnabled &&
      status.paired &&
      status.cloudEndpointId,
  );
  const lastSuccessCompact = formatCompactTimestamp(
    cloud?.lastSuccessAt,
    i18n.resolvedLanguage,
  );
  const lastSuccessFull = cloud?.lastSuccessAt
    ? new Date(cloud.lastSuccessAt).toLocaleString(i18n.resolvedLanguage)
    : void 0;
  const nextRetryCompact = formatCompactTimestamp(
    cloud?.nextRetryAt,
    i18n.resolvedLanguage,
  );
  const nextRetryFull = cloud?.nextRetryAt
    ? new Date(cloud.nextRetryAt).toLocaleString(i18n.resolvedLanguage)
    : void 0;

  const hubStatus = directory?.hubStatus ?? null;
  const devices = directory?.devices ?? [];
  const onlineDevicesCount = devices.filter((device) => {
    return device.online;
  }).length;

  function resolveRelayLabel() {
    switch (status?.cloudRelayMode) {
      case "public":
        return t("syncStatus.cloud.publicRelay");
      case "custom":
        return t("syncStatus.cloud.customRelay");
      default:
        return t("syncStatus.cloud.directOnly");
    }
  }

  const body = (
    <>
      <div className="rounded-2.5 border border-ant-border bg-ant-container p-2.5">
        <div className="flex items-start gap-2.5">
          <div
            className={cn(
              "flex size-8 shrink-0 items-center justify-center rounded-2",
              channelBadgeClassName(cloudState),
            )}
          >
            <i
              className={cn(
                "size-4",
                cloudState === "disabled"
                  ? "i-lucide:cloud-off"
                  : "i-lucide:cloud",
                cloudState === "connecting" && "animate-pulse",
              )}
            />
          </div>

          <div className="min-w-0 flex-1">
            <div className="flex items-center justify-between gap-2">
              <span className="truncate font-medium text-ant-text text-sm">
                {t(`syncStatus.cloud.${cloudState}`)}
              </span>
              {status?.cloudEnabled ? (
                <span className="shrink-0 rounded-full bg-ant-fill-tertiary px-1.5 py-0.5 text-ant-secondary text-xs">
                  {resolveRelayLabel()}
                </span>
              ) : null}
            </div>

            {isConfigured ? (
              <>
                <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                  <span
                    className={cn(
                      "inline-flex items-center gap-1 rounded-1.5 px-1.5 py-0.5 text-xs",
                      channelBadgeClassName(watchState),
                    )}
                  >
                    <i className="i-lucide:radio size-3" />
                    <span>
                      {t("syncStatus.cloud.watchLabel", {
                        state: t(`syncStatus.peer.${watchState}`),
                      })}
                    </span>
                  </span>
                </div>

                {status?.cloudConnectedAddress ? (
                  <div className="mt-1.5">
                    <CopyChip
                      prefix={t(
                        `syncStatus.transport.${status.cloudTransport ?? "unknown"}`,
                      )}
                      value={status.cloudConnectedAddress}
                    />
                  </div>
                ) : null}

                {cloud?.lastError ? (
                  <div
                    className="mt-1.5 truncate rounded-1 bg-ant-error-bg px-1.5 py-0.5 text-ant-error text-xs"
                    title={cloud.lastError}
                  >
                    {cloud.lastError}
                  </div>
                ) : null}

                {lastSuccessCompact || nextRetryCompact ? (
                  <div className="mt-1.5 flex flex-wrap items-center justify-between gap-2 text-ant-secondary text-xs tabular-nums">
                    {lastSuccessCompact ? (
                      <span title={lastSuccessFull}>
                        {t("syncStatus.lastSuccess", {
                          time: lastSuccessCompact,
                        })}
                      </span>
                    ) : (
                      <span />
                    )}
                    {nextRetryCompact ? (
                      <span className="text-ant-warning" title={nextRetryFull}>
                        {t("syncStatus.nextRetry", { time: nextRetryCompact })}
                      </span>
                    ) : null}
                  </div>
                ) : null}
              </>
            ) : (
              <div className="mt-1 flex flex-col items-start gap-2">
                <span className="text-ant-secondary text-xs">
                  {status?.enabled && !status.paired
                    ? t("syncStatus.cloud.unpairedPrompt")
                    : t("syncStatus.cloud.setupPrompt")}
                </span>
                <Button
                  icon={<i className="i-lucide:settings size-3.5" />}
                  onClick={onOpenSettings}
                  size="small"
                  type="primary"
                >
                  {t("syncStatus.actions.openSettings")}
                </Button>
              </div>
            )}
          </div>
        </div>
      </div>

      {isConfigured ? (
        <>
          <CloudQuotaCard
            compact
            error={directoryError}
            loading={directoryLoading}
            onRefresh={onRefreshDirectory}
            status={hubStatus}
          />

          <div className="grid grid-cols-2 gap-2">
            <button
              className="flex cursor-pointer items-center gap-2 rounded-2.5 border border-ant-border bg-ant-container p-2.5 text-left transition-colors hover:border-ant-primary/40 hover:bg-ant-fill-tertiary"
              onClick={onOpenDevices}
              type="button"
            >
              <div className="flex size-8 shrink-0 items-center justify-center rounded-2 bg-ant-info-bg text-ant-info">
                <i className="i-lucide:monitor-smartphone size-4" />
              </div>
              <div className="min-w-0 flex-1">
                <div className="truncate font-medium text-ant-text text-xs">
                  {t("syncStatus.devices.title")}
                </div>
                <div className="truncate text-ant-secondary text-xs tabular-nums">
                  {directory
                    ? t("syncStatus.cloud.devicesSummary", {
                        online: onlineDevicesCount,
                        total: devices.length,
                      })
                    : t("syncStatus.cloud.devicesHint")}
                </div>
              </div>
            </button>

            <button
              className="flex cursor-pointer items-center gap-2 rounded-2.5 border border-ant-border bg-ant-container p-2.5 text-left transition-colors hover:border-ant-primary/40 hover:bg-ant-fill-tertiary"
              onClick={onOpenRecords}
              type="button"
            >
              <div className="flex size-8 shrink-0 items-center justify-center rounded-2 bg-ant-primary-bg text-ant-primary">
                <i className="i-lucide:cloud-download size-4" />
              </div>
              <div className="min-w-0 flex-1">
                <div className="truncate font-medium text-ant-text text-xs">
                  {t("syncStatus.records.title")}
                </div>
                <div className="truncate text-ant-secondary text-xs tabular-nums">
                  {hubStatus
                    ? t("syncStatus.cloud.recordsCount", {
                        count: hubStatus.eventCount,
                      })
                    : t("syncStatus.cloud.recordsHint")}
                </div>
              </div>
            </button>
          </div>

          <button
            className="flex w-full cursor-pointer items-center justify-between gap-2 rounded-2.5 border border-ant-border bg-ant-container px-2.5 py-2 text-left transition-colors hover:border-ant-primary/40 hover:bg-ant-fill-tertiary"
            onClick={onOpenDetails}
            type="button"
          >
            <div className="flex min-w-0 items-center gap-2">
              <div className="flex size-7 shrink-0 items-center justify-center rounded-2 bg-ant-fill-tertiary text-ant-secondary">
                <i className="i-lucide:server size-3.5" />
              </div>
              <div className="min-w-0">
                <div className="truncate font-medium text-ant-text text-xs">
                  {t("syncStatus.actions.moreInfo")}
                </div>
                <div className="truncate text-ant-secondary text-xs">
                  {t("syncStatus.actions.nodeDetailsHint")}
                </div>
              </div>
            </div>
            <i className="i-lucide:chevron-right size-4 shrink-0 text-ant-tertiary" />
          </button>
        </>
      ) : null}
    </>
  );

  if (!showHeader) {
    return <div className="flex w-full flex-col gap-2.5 text-sm">{body}</div>;
  }

  return (
    <div className="flex h-full min-h-0 w-full flex-col gap-2 text-sm">
      <div className="flex shrink-0 items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="font-medium text-ant-text text-sm">
            {t("syncStatus.cloud.title")}
          </span>
          {status?.cloudEnabled && status.cloudServerVersion ? (
            <span className="rounded-full bg-ant-fill-tertiary px-1.5 py-0.5 text-ant-secondary text-xs tabular-nums">
              {t("syncStatus.cloud.serverVersion", {
                version: status.cloudServerVersion,
              })}
            </span>
          ) : null}
        </div>

        <div className="flex shrink-0 items-center gap-0.5">
          <Tooltip title={t("syncStatus.cloud.reconnect")}>
            <Button
              aria-label={t("syncStatus.cloud.reconnect")}
              disabled={!isConfigured || cloudState === "connecting"}
              icon={<i className="i-lucide:refresh-cw size-3.5" />}
              loading={reconnecting}
              onClick={onReconnect}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
          <Tooltip title={t("syncStatus.actions.openSettings")}>
            <Button
              aria-label={t("syncStatus.actions.openSettings")}
              icon={<i className="i-lucide:settings size-3.5" />}
              onClick={onOpenSettings}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
        </div>
      </div>

      <ScrollArea
        className="min-h-0 flex-1 overscroll-contain"
        contentClassName="flex flex-col gap-2 pr-1"
      >
        {body}
      </ScrollArea>
    </div>
  );
};

export default SyncCloudDetails;
