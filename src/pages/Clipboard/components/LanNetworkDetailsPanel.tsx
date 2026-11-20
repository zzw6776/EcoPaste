import { Button } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import type { SyncPeerStatus, SyncStatus } from "@/commands";
import Tooltip from "@/components/Tooltip";
import { cn } from "@/utils/cn";
import { CopyChip, channelBadgeClassName } from "./SyncChannelVisual";
import {
  formatPlatformLabel,
  platformIconClass,
  sortLanPeersForDisplay,
} from "./SyncStatusIcons.logic";

interface LanNetworkDetailsContentProps {
  onOpenSettings: () => void;
  onReconnect: (deviceId?: string) => Promise<void>;
  reconnectingKey: string | null;
  status: SyncStatus | null;
}

/** 展示局域网本机节点信息与每台配对设备的完整地址和诊断详情。 */
export const LanNetworkDetailsContent: FC<LanNetworkDetailsContentProps> = (
  props,
) => {
  const { onOpenSettings, onReconnect, reconnectingKey, status } = props;
  const { t, i18n } = useTranslation("clipboard");

  const sortedPeers = sortLanPeersForDisplay(status?.peers ?? []);
  const canOperate = Boolean(
    status?.enabled && status.lanEnabled && status.paired,
  );
  const locale = i18n.resolvedLanguage;

  if (!canOperate) {
    return (
      <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-4 text-sm">
        <p className="m-0 text-ant-secondary text-xs">
          {!status?.enabled || !status?.lanEnabled
            ? t("syncStatus.lan.disabledPrompt")
            : t("syncStatus.lan.unpairedPrompt")}
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
      <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-3">
        <div className="flex items-center justify-between gap-2">
          <span className="font-medium text-ant-text text-sm">
            {t("syncStatus.lan.localNode")}
          </span>
          <span
            className={cn(
              "rounded-full px-2 py-0.5 font-medium text-xs",
              channelBadgeClassName(status?.lan.state ?? "disabled"),
            )}
          >
            {t(`syncStatus.lan.${status?.lan.state ?? "disabled"}`)}
          </span>
        </div>

        <div className="mt-2 flex flex-col gap-1.5 text-ant-secondary text-xs">
          {status?.deviceName ? (
            <span>
              {t("syncStatus.lan.currentDevice", { name: status.deviceName })}
            </span>
          ) : null}
          {status?.endpointId ? (
            <div className="flex flex-col gap-0.5">
              <span>{t("syncStatus.lan.peerId")}</span>
              <CopyChip value={status.endpointId} />
            </div>
          ) : null}
          {status?.lan.lastSuccessAt ? (
            <span className="tabular-nums">
              {t("syncStatus.lastSuccess", {
                time: new Date(status.lan.lastSuccessAt).toLocaleString(locale),
              })}
            </span>
          ) : null}
          {status?.lan.lastError ? (
            <div className="mt-1 break-words rounded-1.5 bg-ant-error-bg px-2 py-1 text-ant-error text-xs">
              {status.lan.lastError}
            </div>
          ) : null}
        </div>
      </div>

      <div className="flex flex-col gap-2">
        <div className="px-0.5 font-medium text-ant-secondary text-xs">
          {t("syncStatus.lan.peersSection")} ({sortedPeers.length})
        </div>
        {sortedPeers.length ? (
          sortedPeers.map((peer) => {
            return (
              <LanPeerFullCard
                key={peer.deviceId}
                locale={locale}
                onReconnect={onReconnect}
                peer={peer}
                reconnectDisabled={
                  !status?.lanEnabled || reconnectingKey !== null
                }
                reconnecting={reconnectingKey === peer.deviceId}
              />
            );
          })
        ) : (
          <div className="flex flex-col items-center justify-center rounded-2.5 border border-ant-border-secondary border-dashed bg-ant-container px-4 py-6 text-center">
            <i className="i-lucide:radar size-5 text-ant-tertiary" />
            <div className="mt-1.5 font-medium text-ant-text text-xs">
              {t("syncStatus.lan.noPeers")}
            </div>
            <div className="mt-0.5 text-ant-secondary text-xs">
              {t("syncStatus.lan.noPeersHint")}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

interface LanPeerFullCardProps {
  locale?: string;
  onReconnect: (deviceId?: string) => Promise<void>;
  peer: SyncPeerStatus;
  reconnectDisabled: boolean;
  reconnecting: boolean;
}

const LanPeerFullCard: FC<LanPeerFullCardProps> = (props) => {
  const { locale, onReconnect, peer, reconnectDisabled, reconnecting } = props;
  const { t } = useTranslation("clipboard");
  const isOnline = peer.state === "online";
  const connectedAddress = isOnline ? peer.connectedAddress : null;
  const candidateAddresses = peer.directAddresses.filter(
    (address, index, addresses) => {
      return (
        address !== connectedAddress && addresses.indexOf(address) === index
      );
    },
  );
  const reconnectLabel = t("syncStatus.lan.reconnectDevice", {
    device: peer.deviceName,
  });

  function handleReconnect() {
    void onReconnect(peer.deviceId);
  }

  return (
    <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-3">
      <div className="flex items-start gap-2.5">
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
            <div className="flex shrink-0 items-center gap-1">
              <span
                className={cn(
                  "rounded-full px-2 py-0.5 text-xs",
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
            </div>
          </div>

          <div className="mt-0.5 flex flex-wrap items-center gap-1.5 text-ant-secondary text-xs">
            <span>{formatPlatformLabel(peer.platform)}</span>
            {isOnline && peer.transport ? (
              <>
                <span className="text-ant-tertiary">·</span>
                <span>{t(`syncStatus.transport.${peer.transport}`)}</span>
              </>
            ) : null}
          </div>

          {connectedAddress ? (
            <div className="mt-2">
              <CopyChip value={connectedAddress} />
            </div>
          ) : null}

          <div className="mt-2 flex flex-col gap-1 text-ant-secondary text-xs tabular-nums">
            {peer.lastSeenAt ? (
              <span>
                {t("syncStatus.lastSeen", {
                  time: new Date(peer.lastSeenAt).toLocaleString(locale),
                })}
              </span>
            ) : null}
            {peer.lastSuccessAt ? (
              <span>
                {t("syncStatus.lastSuccess", {
                  time: new Date(peer.lastSuccessAt).toLocaleString(locale),
                })}
              </span>
            ) : null}
            {peer.lastAttemptAt ? (
              <span>
                {t("syncStatus.lastAttempt", {
                  time: new Date(peer.lastAttemptAt).toLocaleString(locale),
                })}
              </span>
            ) : null}
          </div>

          {peer.endpointId ? (
            <div className="mt-2 flex flex-col gap-0.5">
              <span className="text-ant-tertiary text-xs">
                {t("syncStatus.lan.peerId")}
              </span>
              <CopyChip value={peer.endpointId} />
            </div>
          ) : null}

          {candidateAddresses.length > 0 ? (
            <div className="mt-2 flex flex-col gap-1">
              <span className="text-ant-tertiary text-xs">
                {t("syncStatus.lan.candidateList")} ({candidateAddresses.length}
                )
              </span>
              <div className="flex flex-wrap gap-1">
                {candidateAddresses.map((address) => {
                  return <CopyChip key={address} value={address} />;
                })}
              </div>
            </div>
          ) : null}

          {peer.lastError ? (
            <div className="mt-2 break-words rounded-1.5 bg-ant-error-bg px-2 py-1 text-ant-error text-xs">
              {peer.lastError}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
};
