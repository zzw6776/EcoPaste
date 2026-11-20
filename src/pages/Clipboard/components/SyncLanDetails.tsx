import { Button } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import type { SyncChannelState, SyncPeerStatus, SyncStatus } from "@/commands";
import ScrollArea from "@/components/ScrollArea";
import Tooltip from "@/components/Tooltip";
import { cn } from "@/utils/cn";
import { CopyChip, channelBadgeClassName } from "./SyncChannelVisual";
import {
  formatCompactTimestamp,
  formatPlatformLabel,
  platformIconClass,
  sortLanPeersForDisplay,
  summarizeLanPeers,
} from "./SyncStatusIcons.logic";

interface SyncLanDetailsProps {
  onOpenDetails: () => void;
  onOpenSettings: () => void;
  onReconnect: (deviceId?: string) => Promise<void>;
  reconnectingKey: string | null;
  showHeader?: boolean;
  status: SyncStatus | null;
}

/** 局域网一级悬浮框菜单：展示通道概览、精简设备状态与二级详情弹窗入口。 */
const SyncLanDetails: FC<SyncLanDetailsProps> = (props) => {
  const {
    onOpenDetails,
    onOpenSettings,
    onReconnect,
    reconnectingKey,
    showHeader = true,
    status,
  } = props;
  const { t } = useTranslation("clipboard");

  const lanState: SyncChannelState = status?.lan.state ?? "disabled";
  const sortedPeers = sortLanPeersForDisplay(status?.peers ?? []);
  const summary = summarizeLanPeers(sortedPeers);
  const canOperate = Boolean(
    status?.enabled && status.lanEnabled && status.paired,
  );
  const hasPeers = sortedPeers.length > 0;

  function handleReconnectAll() {
    void onReconnect();
  }

  const summaryCard = (
    <div className="rounded-2.5 border border-ant-border bg-ant-container p-2.5">
      <div className="flex items-start gap-2.5">
        <div
          className={cn(
            "flex size-8 shrink-0 items-center justify-center rounded-2",
            channelBadgeClassName(lanState),
          )}
        >
          <i
            className={cn(
              "size-4",
              lanState === "disabled" ? "i-lucide:wifi-off" : "i-lucide:wifi",
              lanState === "connecting" && "animate-pulse",
            )}
          />
        </div>

        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-2">
            <span className="truncate font-medium text-ant-text text-sm">
              {t(`syncStatus.lan.${lanState}`)}
            </span>
            {canOperate && hasPeers ? (
              <span
                className={cn(
                  "shrink-0 rounded-full px-1.5 py-0.5 font-medium text-xs tabular-nums",
                  channelBadgeClassName(lanState),
                )}
              >
                {t("syncStatus.lan.onlineRatio", {
                  online: summary.onlineCount,
                  total: summary.totalCount,
                })}
              </span>
            ) : null}
          </div>

          {canOperate ? (
            <div className="mt-1 flex items-center justify-between gap-2 text-ant-secondary text-xs">
              <span className="truncate">
                {status?.deviceName
                  ? t("syncStatus.lan.currentDevice", {
                      name: status.deviceName,
                    })
                  : null}
              </span>
              <button
                className="flex shrink-0 cursor-pointer items-center gap-0.5 border-0 bg-transparent p-0 text-ant-primary text-xs hover:opacity-80"
                onClick={onOpenDetails}
                type="button"
              >
                <span>{t("syncStatus.actions.details")}</span>
                <i className="i-lucide:chevron-right size-3.5" />
              </button>
            </div>
          ) : (
            <div className="mt-1 flex flex-col items-start gap-2">
              <span className="text-ant-secondary text-xs">
                {!status?.enabled || !status?.lanEnabled
                  ? t("syncStatus.lan.disabledPrompt")
                  : t("syncStatus.lan.unpairedPrompt")}
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
  );

  const peersList = (
    <>
      {summaryCard}
      {hasPeers ? (
        <div className="flex flex-col gap-2">
          {sortedPeers.map((peer) => {
            return (
              <LanPeerSummaryCard
                key={peer.deviceId}
                onOpenDetails={onOpenDetails}
                onReconnect={onReconnect}
                peer={peer}
                reconnectDisabled={
                  !status?.lanEnabled || reconnectingKey !== null
                }
                reconnecting={reconnectingKey === peer.deviceId}
              />
            );
          })}
        </div>
      ) : canOperate ? (
        <div className="flex flex-col items-center justify-center rounded-2.5 border border-ant-border-secondary border-dashed bg-ant-fill-quaternary px-3 py-5 text-center">
          <i className="i-lucide:radar size-5 text-ant-tertiary" />
          <div className="mt-1.5 font-medium text-ant-text text-xs">
            {t("syncStatus.lan.noPeers")}
          </div>
          <div className="mt-0.5 text-ant-secondary text-xs">
            {t("syncStatus.lan.noPeersHint")}
          </div>
        </div>
      ) : null}
    </>
  );

  if (!showHeader) {
    return (
      <div className="flex w-full flex-col gap-2.5 text-sm">{peersList}</div>
    );
  }

  return (
    <div className="flex h-full min-h-0 w-full flex-col gap-2 text-sm">
      <div className="flex shrink-0 items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="font-medium text-ant-text text-sm">
            {t("syncStatus.lan.title")}
          </span>
          {canOperate && hasPeers ? (
            <span
              className={cn(
                "rounded-full px-1.5 py-0.5 text-xs tabular-nums",
                summary.onlineCount > 0
                  ? "bg-ant-success-bg text-ant-success"
                  : "bg-ant-fill-tertiary text-ant-secondary",
              )}
            >
              {summary.onlineCount}/{summary.totalCount}
            </span>
          ) : null}
        </div>

        <div className="flex shrink-0 items-center gap-0.5">
          <Tooltip title={t("syncStatus.lan.reconnectAll")}>
            <Button
              aria-label={t("syncStatus.lan.reconnectAll")}
              disabled={!canOperate || !hasPeers || reconnectingKey !== null}
              icon={<i className="i-lucide:refresh-cw size-3.5" />}
              loading={reconnectingKey === "all"}
              onClick={handleReconnectAll}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
          <Tooltip title={t("syncStatus.lan.detailsTitle")}>
            <Button
              aria-label={t("syncStatus.lan.detailsTitle")}
              disabled={!canOperate}
              icon={<i className="i-lucide:info size-3.5" />}
              onClick={onOpenDetails}
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
        {peersList}
      </ScrollArea>
    </div>
  );
};

interface LanPeerSummaryCardProps {
  onOpenDetails: () => void;
  onReconnect: (deviceId?: string) => Promise<void>;
  peer: SyncPeerStatus;
  reconnectDisabled: boolean;
  reconnecting: boolean;
}

const LanPeerSummaryCard: FC<LanPeerSummaryCardProps> = (props) => {
  const { onOpenDetails, onReconnect, peer, reconnectDisabled, reconnecting } =
    props;
  const { t, i18n } = useTranslation("clipboard");

  const isOnline = peer.state === "online";
  const connectedAddress = isOnline ? peer.connectedAddress : null;
  const lastSeenCompact = formatCompactTimestamp(
    peer.lastSeenAt,
    i18n.resolvedLanguage,
  );
  const lastSeenFull = peer.lastSeenAt
    ? new Date(peer.lastSeenAt).toLocaleString(i18n.resolvedLanguage)
    : void 0;
  const reconnectLabel = t("syncStatus.lan.reconnectDevice", {
    device: peer.deviceName,
  });

  function handleReconnect() {
    void onReconnect(peer.deviceId);
  }

  return (
    <div className="rounded-2.5 border border-ant-border bg-ant-container p-2.5 transition-colors hover:border-ant-primary/40">
      <div className="flex items-start gap-2">
        <div
          className={cn(
            "flex size-8 shrink-0 items-center justify-center rounded-2",
            channelBadgeClassName(peer.state),
          )}
        >
          <i className={cn("size-4", platformIconClass(peer.platform))} />
        </div>

        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-2">
            <span className="min-w-0 flex-1 truncate font-medium text-ant-text text-sm">
              {peer.deviceName}
            </span>
            <div className="flex shrink-0 items-center gap-0.5">
              <span
                className={cn(
                  "rounded-full px-1.5 py-0.5 text-xs",
                  channelBadgeClassName(peer.state),
                )}
              >
                {t(`syncStatus.peer.${peer.state}`)}
              </span>
              <Tooltip title={reconnectLabel}>
                <Button
                  aria-label={reconnectLabel}
                  disabled={reconnectDisabled || peer.state === "connecting"}
                  icon={<i className="i-lucide:refresh-cw size-3.5" />}
                  loading={reconnecting}
                  onClick={handleReconnect}
                  shape="circle"
                  size="small"
                  type="text"
                />
              </Tooltip>
              <Tooltip title={t("syncStatus.actions.details")}>
                <Button
                  aria-label={t("syncStatus.actions.details")}
                  icon={<i className="i-lucide:chevron-right size-3.5" />}
                  onClick={onOpenDetails}
                  shape="circle"
                  size="small"
                  type="text"
                />
              </Tooltip>
            </div>
          </div>

          <div className="mt-0.5 flex items-center justify-between gap-2 text-ant-secondary text-xs">
            <span className="truncate">
              {formatPlatformLabel(peer.platform)}
              {isOnline && peer.transport
                ? ` · ${t(`syncStatus.transport.${peer.transport}`)}`
                : ""}
            </span>
            {lastSeenCompact ? (
              <span className="shrink-0 tabular-nums" title={lastSeenFull}>
                {lastSeenCompact}
              </span>
            ) : null}
          </div>

          {connectedAddress ? (
            <div className="mt-1.5">
              <CopyChip value={connectedAddress} />
            </div>
          ) : null}

          {peer.lastError ? (
            <div
              className="mt-1.5 truncate rounded-1 bg-ant-error-bg px-1.5 py-0.5 text-ant-error text-xs"
              title={peer.lastError}
            >
              {peer.lastError}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
};

export default SyncLanDetails;
