import { listen } from "@tauri-apps/api/event";
import { useMount, useUnmount } from "ahooks";
import { Button, Popover, Tooltip } from "antd";
import type { FC } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  getSyncStatus,
  reconnectSyncPeer,
  type SyncChannelState,
  type SyncStatus,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { cn } from "@/utils/cn";
import { log } from "@/utils/log";

interface SyncStatusIconsProps {
  compact?: boolean;
}

const SyncStatusIcons: FC<SyncStatusIconsProps> = (props) => {
  const { compact = false } = props;
  const { t } = useTranslation("clipboard");
  const [status, setStatus] = useState<SyncStatus | null>(null);
  const [reconnectingKey, setReconnectingKey] = useState<string | null>(null);
  const mountedRef = useRef(false);
  const unlistenRef = useRef<null | (() => void)>(null);

  async function refresh() {
    try {
      setStatus(await getSyncStatus());
    } catch (error) {
      log.error("load clipboard sync status failed", error);
    }
  }

  async function initialize() {
    try {
      await refresh();
      const unlisten = await listen(TAURI_EVENT.SYNC_UPDATED, refresh);
      if (!mountedRef.current) {
        unlisten();
        return;
      }
      unlistenRef.current = unlisten;
    } catch (error) {
      log.error("initialize clipboard sync status failed", error);
    }
  }

  useMount(() => {
    mountedRef.current = true;
    void initialize();
  });

  useUnmount(() => {
    mountedRef.current = false;
    unlistenRef.current?.();
  });

  async function handleReconnect(deviceId?: string) {
    setReconnectingKey(deviceId ?? "all");
    try {
      setStatus(await reconnectSyncPeer(deviceId));
    } catch {
      // 命令层已统一记录并显示错误。
    } finally {
      setReconnectingKey(null);
    }
  }

  const lanState = status?.lan.state ?? "disabled";
  const cloudState = status?.cloud.state ?? "disabled";
  const buttonClassName = compact ? "size-8" : "size-7";

  return (
    <div className="flex shrink-0 items-center gap-0.5">
      <Popover
        content={
          <LanDetails
            onReconnect={handleReconnect}
            reconnectingKey={reconnectingKey}
            status={status}
          />
        }
        placement="bottomRight"
        trigger="click"
      >
        <Tooltip title={t(`syncStatus.lan.${lanState}`)}>
          <button
            aria-label={t(`syncStatus.lan.${lanState}`)}
            className={cn(
              "flex cursor-pointer items-center justify-center rounded-full transition-colors hover:bg-ant-fill-tertiary",
              buttonClassName,
              stateClassName(lanState),
            )}
            type="button"
          >
            <i
              className={cn(
                "i-lucide:wifi size-4",
                lanState === "connecting" && "animate-pulse",
              )}
            />
          </button>
        </Tooltip>
      </Popover>

      <Popover
        content={
          <CloudDetails
            addresses={[
              ...(status?.cloudDirectAddresses ?? []),
              ...(status?.cloudRelayUrls ?? []),
            ]}
            endpointId={status?.cloudEndpointId ?? ""}
            status={status}
          />
        }
        placement="bottomRight"
        trigger="click"
      >
        <Tooltip title={t(`syncStatus.cloud.${cloudState}`)}>
          <button
            aria-label={t(`syncStatus.cloud.${cloudState}`)}
            className={cn(
              "flex cursor-pointer items-center justify-center rounded-full transition-colors hover:bg-ant-fill-tertiary",
              buttonClassName,
              stateClassName(cloudState),
            )}
            type="button"
          >
            <i
              className={cn(
                "i-lucide:cloud size-4",
                cloudState === "connecting" && "animate-pulse",
              )}
            />
          </button>
        </Tooltip>
      </Popover>
    </div>
  );
};

interface LanDetailsProps {
  onReconnect: (deviceId?: string) => Promise<void>;
  reconnectingKey: string | null;
  status: SyncStatus | null;
}

const LanDetails: FC<LanDetailsProps> = (props) => {
  const { onReconnect, reconnectingKey, status } = props;
  const { t } = useTranslation("clipboard");

  function handleReconnectAll() {
    void onReconnect();
  }

  return (
    <div className="flex w-72 flex-col gap-2 text-sm">
      <div className="flex items-center justify-between gap-2">
        <strong>{t("syncStatus.lan.title")}</strong>
        <Tooltip title={t("syncStatus.lan.reconnectAll")}>
          <Button
            aria-label={t("syncStatus.lan.reconnectAll")}
            disabled={!status?.peers.length || reconnectingKey !== null}
            icon={<i className="i-lucide:refresh-cw size-3.5" />}
            loading={reconnectingKey === "all"}
            onClick={handleReconnectAll}
            size="small"
            type="text"
          />
        </Tooltip>
      </div>
      {status?.peers.length ? (
        status.peers.map((peer) => {
          return (
            <LanPeerDetails
              key={peer.deviceId}
              onReconnect={onReconnect}
              peer={peer}
              reconnectDisabled={reconnectingKey !== null}
              reconnecting={reconnectingKey === peer.deviceId}
            />
          );
        })
      ) : (
        <span className="text-ant-secondary text-xs">
          {t("syncStatus.lan.noPeers")}
        </span>
      )}
    </div>
  );
};

interface LanPeerDetailsProps {
  onReconnect: (deviceId?: string) => Promise<void>;
  peer: SyncStatus["peers"][number];
  reconnectDisabled: boolean;
  reconnecting: boolean;
}

const LanPeerDetails: FC<LanPeerDetailsProps> = (props) => {
  const { onReconnect, peer, reconnectDisabled, reconnecting } = props;
  const { t } = useTranslation("clipboard");
  const addresses = peer.connectedAddress
    ? [peer.connectedAddress]
    : peer.directAddresses;
  const reconnectLabel = t("syncStatus.lan.reconnectDevice", {
    device: peer.deviceName,
  });

  function handleReconnect() {
    void onReconnect(peer.deviceId);
  }

  return (
    <div className="rounded-2 bg-ant-fill-quaternary p-2">
      <div className="flex items-center justify-between gap-2">
        <span className="truncate font-medium">{peer.deviceName}</span>
        <div className="flex shrink-0 items-center gap-1">
          <span className={cn("text-xs", stateClassName(peer.state))}>
            {t(`syncStatus.peer.${peer.state}`)}
          </span>
          <Tooltip title={reconnectLabel}>
            <Button
              aria-label={reconnectLabel}
              disabled={reconnectDisabled || peer.state === "connecting"}
              icon={<i className="i-lucide:refresh-cw size-3.5" />}
              loading={reconnecting}
              onClick={handleReconnect}
              size="small"
              type="text"
            />
          </Tooltip>
        </div>
      </div>
      <div className="mt-1 text-ant-secondary text-xs">
        {peer.platform}
        {peer.transport
          ? ` · ${t(`syncStatus.transport.${peer.transport}`)}`
          : ""}
      </div>
      {addresses.map((address) => {
        return (
          <div
            className="mt-1 break-all font-mono text-ant-secondary text-xs"
            key={address}
          >
            {address}
          </div>
        );
      })}
      {peer.lastSeenAt ? (
        <div className="mt-1 text-ant-secondary text-xs">
          {t("syncStatus.lastSeen", {
            time: new Date(peer.lastSeenAt).toLocaleString(),
          })}
        </div>
      ) : null}
      {peer.lastError ? (
        <div className="mt-1 text-ant-error text-xs">{peer.lastError}</div>
      ) : null}
    </div>
  );
};

interface CloudDetailsProps {
  addresses: readonly string[];
  endpointId: string;
  status: SyncStatus | null;
}

const CloudDetails: FC<CloudDetailsProps> = (props) => {
  const { addresses, endpointId, status } = props;
  const { t } = useTranslation("clipboard");
  const cloud = status?.cloud;

  return (
    <div className="flex w-72 flex-col gap-2 text-sm">
      <strong>{t("syncStatus.cloud.title")}</strong>
      <span
        className={cn("text-xs", stateClassName(cloud?.state ?? "disabled"))}
      >
        {t(`syncStatus.cloud.${cloud?.state ?? "disabled"}`)}
      </span>
      {endpointId ? (
        <div className="break-all font-mono text-ant-secondary text-xs">
          {endpointId}
        </div>
      ) : null}
      {addresses.map((address) => {
        return (
          <div
            className="break-all font-mono text-ant-secondary text-xs"
            key={address}
          >
            {address}
          </div>
        );
      })}
      {cloud?.lastSuccessAt ? (
        <span className="text-ant-secondary text-xs">
          {t("syncStatus.lastSuccess", {
            time: new Date(cloud.lastSuccessAt).toLocaleString(),
          })}
        </span>
      ) : null}
      {cloud?.lastError ? (
        <span className="text-ant-error text-xs">{cloud.lastError}</span>
      ) : null}
      {status?.pendingEvents ? (
        <span className="text-ant-warning text-xs">
          {t("syncStatus.cloud.pending", { count: status.pendingEvents })}
        </span>
      ) : null}
    </div>
  );
};

function stateClassName(state: SyncChannelState) {
  switch (state) {
    case "online":
      return "text-ant-success";
    case "connecting":
      return "text-ant-info";
    case "error":
      return "text-ant-error";
    default:
      return "text-ant-tertiary";
  }
}

export default SyncStatusIcons;
