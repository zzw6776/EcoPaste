import { listen } from "@tauri-apps/api/event";
import { useMount, useUnmount } from "ahooks";
import { Button, Drawer } from "antd";
import type { FC, MouseEventHandler, ReactElement } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  getSyncStatus,
  reconnectSyncPeer,
  type SyncChannelState,
  type SyncStatus,
} from "@/commands";
import Popover from "@/components/Popover";
import Tooltip from "@/components/Tooltip";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useTauriListen } from "@/hooks/useTauriListen";
import { cn } from "@/utils/cn";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";
import CloudRecordsDrawer from "./CloudRecordsDrawer";

interface SyncStatusIconsProps {
  compact?: boolean;
}

interface WindowVisibilityPayload {
  label: string;
  visible: boolean;
}

const SyncStatusIcons: FC<SyncStatusIconsProps> = (props) => {
  const { compact = false } = props;
  const { t } = useTranslation("clipboard");
  const [status, setStatus] = useState<SyncStatus | null>(null);
  const [recordsOpen, setRecordsOpen] = useState(false);
  const [detailsTarget, setDetailsTarget] = useState<"lan" | "cloud" | null>(
    null,
  );
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

  function handleWindowVisibility(event: { payload: WindowVisibilityPayload }) {
    if (event.payload.label !== WINDOW_LABEL.CLIPBOARD) return;

    if (event.payload.visible) {
      void refresh();
      return;
    }

    setDetailsTarget(null);
    setRecordsOpen(false);
  }

  useTauriListen<WindowVisibilityPayload>(
    TAURI_EVENT.WINDOW_VISIBILITY,
    handleWindowVisibility,
  );

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

  function handleOpenRecords() {
    setDetailsTarget(null);
    setRecordsOpen(true);
  }

  function handleCloseRecords() {
    setRecordsOpen(false);
  }

  function handleOpenLanDetails() {
    setDetailsTarget("lan");
  }

  function handleOpenCloudDetails() {
    setDetailsTarget("cloud");
  }

  function handleLanDetailsOpenChange(open: boolean) {
    setDetailsTarget((current) => {
      if (open) return "lan";

      return current === "lan" ? null : current;
    });
  }

  function handleCloudDetailsOpenChange(open: boolean) {
    setDetailsTarget((current) => {
      if (open) return "cloud";

      return current === "cloud" ? null : current;
    });
  }

  function handleCloseDetails() {
    setDetailsTarget(null);
  }

  function handleReconnectAll() {
    void handleReconnect();
  }

  const lanState = status?.lan.state ?? "disabled";
  const cloudState = status?.cloud.state ?? "disabled";
  const buttonClassName = compact ? "size-8" : "size-7";
  const lanLabel = t(`syncStatus.lan.${lanState}`);
  const cloudLabel = t(`syncStatus.cloud.${cloudState}`);
  const cloudDetails = (
    <CloudDetails
      addresses={[...(status?.cloudDirectAddresses ?? [])]}
      endpointId={status?.cloudEndpointId ?? ""}
      onOpenRecords={handleOpenRecords}
      showTitle={!isAndroid}
      status={status}
    />
  );

  return (
    <>
      <div className="flex shrink-0 items-center gap-0.5">
        {isAndroid ? (
          <>
            {renderStatusButton(
              lanLabel,
              "i-lucide:wifi",
              lanState,
              buttonClassName,
              handleOpenLanDetails,
            )}
            {renderStatusButton(
              cloudLabel,
              "i-lucide:cloud",
              cloudState,
              buttonClassName,
              handleOpenCloudDetails,
            )}
          </>
        ) : (
          <>
            <Popover
              content={
                <LanDetails
                  onReconnect={handleReconnect}
                  reconnectingKey={reconnectingKey}
                  status={status}
                />
              }
              onOpenChange={handleLanDetailsOpenChange}
              open={detailsTarget === "lan"}
              placement="bottomRight"
              tooltip={{ mouseEnterDelay: 0.3, title: lanLabel }}
              trigger="click"
            >
              {renderStatusButton(
                lanLabel,
                "i-lucide:wifi",
                lanState,
                buttonClassName,
              )}
            </Popover>

            <Popover
              content={cloudDetails}
              onOpenChange={handleCloudDetailsOpenChange}
              open={detailsTarget === "cloud"}
              placement="bottomRight"
              tooltip={{ mouseEnterDelay: 0.3, title: cloudLabel }}
              trigger="click"
            >
              {renderStatusButton(
                cloudLabel,
                "i-lucide:cloud",
                cloudState,
                buttonClassName,
              )}
            </Popover>
          </>
        )}
      </div>

      {isAndroid ? (
        <Drawer
          destroyOnHidden
          extra={
            detailsTarget === "lan" ? (
              <Button
                aria-label={t("syncStatus.lan.reconnectAll")}
                disabled={
                  !status?.lanEnabled ||
                  !status?.peers.length ||
                  reconnectingKey !== null
                }
                icon={<i className="i-lucide:refresh-cw size-4" />}
                loading={reconnectingKey === "all"}
                onClick={handleReconnectAll}
                type="text"
              />
            ) : null
          }
          onClose={handleCloseDetails}
          open={detailsTarget !== null}
          placement="bottom"
          title={
            detailsTarget === "lan"
              ? t("syncStatus.lan.title")
              : t("syncStatus.cloud.title")
          }
        >
          {detailsTarget === "lan" ? (
            <LanDetails
              onReconnect={handleReconnect}
              reconnectingKey={reconnectingKey}
              showHeader={false}
              status={status}
            />
          ) : (
            cloudDetails
          )}
        </Drawer>
      ) : null}

      <CloudRecordsDrawer onClose={handleCloseRecords} open={recordsOpen} />
    </>
  );
};

/**
 * 渲染局域网与云端状态按钮；移动端直接响应点击，桌面端由 Popover 接管。
 */
function renderStatusButton(
  label: string,
  icon: string,
  state: SyncChannelState,
  buttonClassName: string,
  onClick?: MouseEventHandler<HTMLButtonElement>,
): ReactElement {
  return (
    <button
      aria-label={label}
      className={cn(
        "flex cursor-pointer items-center justify-center rounded-full transition-colors hover:bg-ant-fill-tertiary",
        buttonClassName,
        stateClassName(state),
      )}
      onClick={onClick}
      type="button"
    >
      <i
        className={cn(
          icon,
          "size-4",
          state === "connecting" && "animate-pulse",
        )}
      />
    </button>
  );
}

interface LanDetailsProps {
  onReconnect: (deviceId?: string) => Promise<void>;
  reconnectingKey: string | null;
  showHeader?: boolean;
  status: SyncStatus | null;
}

const LanDetails: FC<LanDetailsProps> = (props) => {
  const { onReconnect, reconnectingKey, showHeader = true, status } = props;
  const { t } = useTranslation("clipboard");

  function handleReconnectAll() {
    void onReconnect();
  }

  return (
    <div
      className={cn(
        "flex flex-col gap-2 text-sm",
        showHeader ? "w-72" : "w-full",
      )}
    >
      {showHeader ? (
        <div className="flex items-center justify-between gap-2">
          <strong>{t("syncStatus.lan.title")}</strong>
          <Tooltip title={t("syncStatus.lan.reconnectAll")}>
            <Button
              aria-label={t("syncStatus.lan.reconnectAll")}
              disabled={
                !status?.lanEnabled ||
                !status?.peers.length ||
                reconnectingKey !== null
              }
              icon={<i className="i-lucide:refresh-cw size-3.5" />}
              loading={reconnectingKey === "all"}
              onClick={handleReconnectAll}
              size="small"
              type="text"
            />
          </Tooltip>
        </div>
      ) : null}
      {status?.peers.length ? (
        status.peers.map((peer) => {
          return (
            <LanPeerDetails
              key={peer.deviceId}
              onReconnect={onReconnect}
              peer={peer}
              reconnectDisabled={!status.lanEnabled || reconnectingKey !== null}
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
  const [candidateAddressesOpen, setCandidateAddressesOpen] = useState(false);
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

  function handleCandidateAddressesToggle() {
    setCandidateAddressesOpen((open) => !open);
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
        {isOnline && peer.transport
          ? ` · ${t(`syncStatus.transport.${peer.transport}`)}`
          : ""}
      </div>
      {connectedAddress ? (
        <div className="mt-1 break-all font-mono text-ant-secondary text-xs">
          {connectedAddress}
        </div>
      ) : null}
      {!isOnline && candidateAddresses.length > 0 ? (
        <>
          <button
            aria-expanded={candidateAddressesOpen}
            className="mt-1 flex cursor-pointer items-center gap-1 border-0 bg-transparent p-0 text-ant-secondary text-xs hover:text-ant-text"
            onClick={handleCandidateAddressesToggle}
            type="button"
          >
            <i
              className={cn(
                "i-lucide:chevron-right size-3 transition-transform",
                candidateAddressesOpen && "rotate-90",
              )}
            />
            <span>
              {t("syncStatus.candidateAddresses", {
                count: candidateAddresses.length,
              })}
            </span>
          </button>
          {candidateAddressesOpen
            ? candidateAddresses.map((address) => {
                return (
                  <div
                    className="mt-1 break-all font-mono text-ant-secondary text-xs"
                    key={address}
                  >
                    {address}
                  </div>
                );
              })
            : null}
        </>
      ) : null}
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
  onOpenRecords: () => void;
  showTitle?: boolean;
  status: SyncStatus | null;
}

const CloudDetails: FC<CloudDetailsProps> = (props) => {
  const {
    addresses,
    endpointId,
    onOpenRecords,
    showTitle = true,
    status,
  } = props;
  const { t } = useTranslation("clipboard");
  const cloud = status?.cloud;

  return (
    <div
      className={cn(
        "flex flex-col gap-2 text-sm",
        showTitle ? "w-72" : "w-full",
      )}
    >
      {showTitle ? <strong>{t("syncStatus.cloud.title")}</strong> : null}
      <span
        className={cn("text-xs", stateClassName(cloud?.state ?? "disabled"))}
      >
        {t(`syncStatus.cloud.${cloud?.state ?? "disabled"}`)}
      </span>
      {status?.cloudEnabled && endpointId ? (
        <div className="break-all font-mono text-ant-secondary text-xs">
          {endpointId}
        </div>
      ) : null}
      {status?.cloudEnabled
        ? addresses.map((address) => {
            return (
              <div
                className="break-all font-mono text-ant-secondary text-xs"
                key={address}
              >
                {address}
              </div>
            );
          })
        : null}
      {status?.cloudEnabled && status.cloudConnectedAddress ? (
        <div className="break-all font-mono text-ant-secondary text-xs">
          {t("syncStatus.cloud.activeRoute", {
            address: status.cloudConnectedAddress,
            transport: t(
              `syncStatus.transport.${status.cloudTransport ?? "unknown"}`,
            ),
          })}
        </div>
      ) : null}
      {status?.cloudEnabled ? (
        <span className="text-ant-secondary text-xs">
          {status.cloudServerVersion
            ? t("syncStatus.cloud.serverVersion", {
                version: status.cloudServerVersion,
              })
            : t("syncStatus.cloud.serverVersionUnknown")}
        </span>
      ) : null}
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
      <Button
        block
        disabled={!status?.enabled || !status.cloudEnabled || !status.paired}
        icon={<i className="i-lucide:cloud-download size-4" />}
        onClick={onOpenRecords}
        size="small"
      >
        {t("syncStatus.records.open")}
      </Button>
    </div>
  );
};

function stateClassName(state: SyncChannelState) {
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

export default SyncStatusIcons;
