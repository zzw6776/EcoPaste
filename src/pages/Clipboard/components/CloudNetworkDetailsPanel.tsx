import { Button } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import type { SyncChannelStatus, SyncStatus } from "@/commands";
import { cn } from "@/utils/cn";
import { CopyChip, channelBadgeClassName } from "./SyncChannelVisual";

interface CloudNetworkDetailsContentProps {
  onOpenSettings: () => void;
  status: SyncStatus | null;
}

/** 展示云端双通道健康状况、节点 ID、直连地址与中继路由详情。 */
export const CloudNetworkDetailsContent: FC<CloudNetworkDetailsContentProps> = (
  props,
) => {
  const { onOpenSettings, status } = props;
  const { t, i18n } = useTranslation("clipboard");

  const directAddresses = status?.cloudDirectAddresses ?? [];
  const relayUrls = status?.cloudRelayUrls ?? [];
  const isConfigured = Boolean(
    status?.enabled &&
      status.cloudEnabled &&
      status.paired &&
      status.cloudEndpointId,
  );

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

  function renderChannelCard(
    title: string,
    icon: string,
    channel: SyncChannelStatus | undefined,
  ) {
    const state = channel?.state ?? "disabled";
    const locale = i18n.resolvedLanguage;

    return (
      <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-3">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <div
              className={cn(
                "flex size-7 shrink-0 items-center justify-center rounded-2",
                channelBadgeClassName(state),
              )}
            >
              <i className={cn("size-3.5", icon)} />
            </div>
            <span className="font-medium text-ant-text text-sm">{title}</span>
          </div>
          <span
            className={cn(
              "rounded-full px-2 py-0.5 font-medium text-xs",
              channelBadgeClassName(state),
            )}
          >
            {t(`syncStatus.peer.${state}`)}
          </span>
        </div>

        <div className="mt-2 flex flex-col gap-1 text-ant-secondary text-xs tabular-nums">
          {channel?.lastSuccessAt ? (
            <span>
              {t("syncStatus.lastSuccess", {
                time: new Date(channel.lastSuccessAt).toLocaleString(locale),
              })}
            </span>
          ) : null}
          {channel?.lastAttemptAt ? (
            <span>
              {t("syncStatus.lastAttempt", {
                time: new Date(channel.lastAttemptAt).toLocaleString(locale),
              })}
            </span>
          ) : null}
          {channel?.nextRetryAt ? (
            <span className="text-ant-warning">
              {t("syncStatus.nextRetry", {
                time: new Date(channel.nextRetryAt).toLocaleString(locale),
              })}
            </span>
          ) : null}
          {channel?.lastError ? (
            <div className="mt-1 break-words rounded-1.5 bg-ant-error-bg px-2 py-1 text-ant-error text-xs">
              {channel.lastError}
            </div>
          ) : null}
        </div>
      </div>
    );
  }

  if (!isConfigured) {
    return (
      <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-4 text-sm">
        <p className="m-0 text-ant-secondary text-xs">
          {status?.enabled && !status.paired
            ? t("syncStatus.cloud.unpairedPrompt")
            : t("syncStatus.cloud.setupPrompt")}
        </p>
        <Button
          className="mt-3"
          icon={<i className="i-lucide:settings size-3.5" />}
          onClick={onOpenSettings}
          size="small"
          type="primary"
        >
          {t("syncStatus.actions.openSettings")}
        </Button>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {renderChannelCard(
        t("syncStatus.cloud.channelTransfer"),
        "i-lucide:cloud",
        status?.cloud,
      )}
      {renderChannelCard(
        t("syncStatus.cloud.channelWatch"),
        "i-lucide:radio",
        status?.cloudWatch,
      )}

      <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="font-medium text-ant-text text-sm">
            {t("syncStatus.actions.nodeDetailsHint")}
          </span>
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="rounded-full bg-ant-fill-tertiary px-2 py-0.5 text-ant-secondary text-xs">
              {status?.cloudServerVersion
                ? t("syncStatus.cloud.serverVersion", {
                    version: status.cloudServerVersion,
                  })
                : t("syncStatus.cloud.serverVersionUnknown")}
            </span>
            <span className="rounded-full bg-ant-fill-tertiary px-2 py-0.5 text-ant-secondary text-xs">
              {resolveRelayLabel()}
            </span>
            {status?.cloudRelayAuthConfigured ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-ant-success-bg px-2 py-0.5 text-ant-success text-xs">
                <i className="i-lucide:shield-check size-3" />
                <span>{t("syncStatus.cloud.relayAuthConfigured")}</span>
              </span>
            ) : null}
          </div>
        </div>

        <div className="mt-2.5 flex flex-col gap-2">
          {status?.cloudConnectedAddress ? (
            <div className="flex flex-col gap-1">
              <span className="text-ant-secondary text-xs">
                {t("syncStatus.cloud.activeRoute", {
                  address: "",
                  transport: t(
                    `syncStatus.transport.${status.cloudTransport ?? "unknown"}`,
                  ),
                }).replace(/：\s*$|:\s*$/, "")}
              </span>
              <CopyChip value={status.cloudConnectedAddress} />
            </div>
          ) : null}

          {status?.cloudEndpointId ? (
            <div className="flex flex-col gap-1">
              <span className="text-ant-secondary text-xs">
                {t("syncStatus.cloud.nodeId")}
              </span>
              <CopyChip value={status.cloudEndpointId} />
            </div>
          ) : null}

          {directAddresses.length > 0 ? (
            <div className="flex flex-col gap-1">
              <span className="text-ant-secondary text-xs">
                {t("syncStatus.cloud.nodeAddresses")}
              </span>
              <div className="flex flex-wrap gap-1.5">
                {directAddresses.map((address) => {
                  return <CopyChip key={address} value={address} />;
                })}
              </div>
            </div>
          ) : null}

          {relayUrls.length > 0 ? (
            <div className="flex flex-col gap-1">
              <span className="text-ant-secondary text-xs">
                {t("syncStatus.cloud.relayUrls")}
              </span>
              <div className="flex flex-wrap gap-1.5">
                {relayUrls.map((url) => {
                  return <CopyChip key={url} value={url} />;
                })}
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
};
