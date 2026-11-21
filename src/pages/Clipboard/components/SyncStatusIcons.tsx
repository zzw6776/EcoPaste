import { listen } from "@tauri-apps/api/event";
import { useMount, useUnmount } from "ahooks";
import { Button } from "antd";
import type { FC, MouseEventHandler, ReactElement } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type CloudDeviceDirectory,
  getSyncStatus,
  listCloudDevices,
  reconnectCloud,
  reconnectSyncPeer,
  type SyncChannelState,
  type SyncStatus,
  showWindow,
} from "@/commands";
import Drawer from "@/components/Drawer";
import Popover from "@/components/Popover";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useWindowLifecycle } from "@/hooks/useWindowLifecycle";
import { router } from "@/router";
import {
  isCurrentWindowDormant,
  isCurrentWindowVisible,
} from "@/stores/windowLifecycle";
import { cn } from "@/utils/cn";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";
import CloudDevicesDrawer from "./CloudDevicesDrawer";
import { CloudNetworkDetailsContent } from "./CloudNetworkDetailsPanel";
import CloudRecordsDrawer from "./CloudRecordsDrawer";
import HeaderActionPopoverPanel from "./HeaderActionPopoverPanel";
import { LanNetworkDetailsContent } from "./LanNetworkDetailsPanel";
import { channelTextClassName } from "./SyncChannelVisual";
import SyncCloudDetails from "./SyncCloudDetails";
import SyncLanDetails from "./SyncLanDetails";
import {
  createSyncStatusRefresh,
  summarizeLanPeers,
} from "./SyncStatusIcons.logic";

const HIDDEN_WARM_REFRESH_DEBOUNCE_MS = 120;
const PANEL_MAX_WIDTH = 360;

interface SyncStatusIconsProps {
  compact?: boolean;
}

interface CloudDirectoryState {
  directory: CloudDeviceDirectory | null;
  error: boolean;
  key: string | null;
  loading: boolean;
}

type AndroidSecondaryTarget = "cloud-details" | "lan-details" | null;

const SyncStatusIcons: FC<SyncStatusIconsProps> = (props) => {
  const { compact = false } = props;
  const { t } = useTranslation("clipboard");
  const lifecycle = useWindowLifecycle();
  const [status, setStatus] = useState<SyncStatus | null>(null);
  const [devicesOpen, setDevicesOpen] = useState(false);
  const [recordsOpen, setRecordsOpen] = useState(false);
  const [androidSecondary, setAndroidSecondary] =
    useState<AndroidSecondaryTarget>(null);
  const [detailsTarget, setDetailsTarget] = useState<"lan" | "cloud" | null>(
    null,
  );
  const [reconnectingKey, setReconnectingKey] = useState<string | null>(null);
  const [cloudDirectoryState, setCloudDirectoryState] =
    useState<CloudDirectoryState>({
      directory: null,
      error: false,
      key: null,
      loading: false,
    });

  const warmRefreshTimerRef = useRef<number | null>(null);
  const dirtyWhileDormantRef = useRef(false);
  const hasLoadedStatusRef = useRef(false);
  const mountedRef = useRef(false);
  const unlistenRef = useRef<null | (() => void)>(null);
  const refreshRef = useRef<null | (() => Promise<void> | undefined)>(null);
  const directoryGenerationRef = useRef(0);

  if (!refreshRef.current) {
    refreshRef.current = createSyncStatusRefresh({
      isActive: () => {
        return mountedRef.current && !isCurrentWindowDormant();
      },
      onError: (error) => {
        log.error("load clipboard sync status failed", error);
      },
      read: getSyncStatus,
      write: (nextStatus) => {
        hasLoadedStatusRef.current = true;
        setStatus(nextStatus);
      },
    });
  }
  const refresh = refreshRef.current;

  const cloudSpaceKey =
    status?.enabled &&
    status.cloudEnabled &&
    status.paired &&
    status.cloudEndpointId
      ? `${status.groupId ?? ""}:${status.cloudEndpointId}`
      : null;

  const clearWarmRefreshTimer = useCallback(() => {
    if (warmRefreshTimerRef.current === null) return;

    window.clearTimeout(warmRefreshTimerRef.current);
    warmRefreshTimerRef.current = null;
  }, []);

  const loadCloudDirectory = useCallback(async () => {
    if (!cloudSpaceKey || !mountedRef.current) return;

    const targetKey = cloudSpaceKey;
    const generation = directoryGenerationRef.current + 1;
    directoryGenerationRef.current = generation;
    setCloudDirectoryState((current) => {
      return {
        directory: current.key === targetKey ? current.directory : null,
        error: false,
        key: targetKey,
        loading: true,
      };
    });
    try {
      const directory = await listCloudDevices({ silent: true });
      if (
        !mountedRef.current ||
        directoryGenerationRef.current !== generation
      ) {
        return;
      }
      setCloudDirectoryState({
        directory,
        error: false,
        key: targetKey,
        loading: false,
      });
    } catch {
      if (
        !mountedRef.current ||
        directoryGenerationRef.current !== generation
      ) {
        return;
      }
      setCloudDirectoryState((current) => {
        return {
          directory: current.key === targetKey ? current.directory : null,
          error: true,
          key: targetKey,
          loading: false,
        };
      });
    }
  }, [cloudSpaceKey]);

  async function initialize() {
    try {
      const unlisten = await listen(
        TAURI_EVENT.SYNC_UPDATED,
        handleSyncUpdated,
      );
      if (!mountedRef.current) {
        unlisten();
        return;
      }
      unlistenRef.current = unlisten;
      if (isCurrentWindowDormant()) {
        dirtyWhileDormantRef.current = true;
        return;
      }
      void refresh();
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
    directoryGenerationRef.current += 1;
    clearWarmRefreshTimer();
    unlistenRef.current?.();
  });

  function handleSyncUpdated() {
    if (!mountedRef.current) return;
    if (isCurrentWindowDormant()) {
      dirtyWhileDormantRef.current = true;
      clearWarmRefreshTimer();
      return;
    }
    if (isCurrentWindowVisible()) {
      dirtyWhileDormantRef.current = false;
      clearWarmRefreshTimer();
      void refresh();
      return;
    }

    clearWarmRefreshTimer();
    warmRefreshTimerRef.current = window.setTimeout(() => {
      warmRefreshTimerRef.current = null;
      if (!mountedRef.current || isCurrentWindowDormant()) {
        dirtyWhileDormantRef.current = true;
        return;
      }
      void refresh();
    }, HIDDEN_WARM_REFRESH_DEBOUNCE_MS);
  }

  useEffect(() => {
    if (!lifecycle.initialized) return;

    if (!lifecycle.visible) {
      setDetailsTarget(null);
      setDevicesOpen(false);
      setRecordsOpen(false);
      setAndroidSecondary(null);
      if (lifecycle.phase === "dormant") {
        clearWarmRefreshTimer();
      }
      return;
    }

    clearWarmRefreshTimer();
    if (dirtyWhileDormantRef.current || !hasLoadedStatusRef.current) {
      dirtyWhileDormantRef.current = false;
      void refresh();
    }
  }, [
    clearWarmRefreshTimer,
    lifecycle.initialized,
    lifecycle.phase,
    lifecycle.visible,
    refresh,
  ]);

  useEffect(() => {
    if (detailsTarget !== "cloud" || !cloudSpaceKey) return;

    void loadCloudDirectory();
  }, [cloudSpaceKey, detailsTarget, loadCloudDirectory]);

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

  async function handleReconnectCloud() {
    setReconnectingKey("cloud");
    try {
      setStatus(await reconnectCloud());
      if (cloudSpaceKey) {
        void loadCloudDirectory();
      }
    } catch {
      // 命令层已统一记录并显示错误。
    } finally {
      setReconnectingKey(null);
    }
  }

  function handleRefreshCloudDirectory() {
    void loadCloudDirectory();
  }

  function openSecondaryWindow(label: string) {
    setDetailsTarget(null);
    void showWindow(label);
  }

  function handleOpenRecords() {
    if (!isAndroid) {
      openSecondaryWindow(WINDOW_LABEL.CLOUD_RECORDS);
      return;
    }
    setRecordsOpen(true);
  }

  function handleOpenDevices() {
    if (!isAndroid) {
      openSecondaryWindow(WINDOW_LABEL.CLOUD_DEVICES);
      return;
    }
    setDevicesOpen(true);
  }

  function handleOpenCloudDetailsWindow() {
    if (!isAndroid) {
      openSecondaryWindow(WINDOW_LABEL.CLOUD_DETAILS);
      return;
    }
    setAndroidSecondary("cloud-details");
  }

  function handleOpenLanDetailsWindow() {
    if (!isAndroid) {
      openSecondaryWindow(WINDOW_LABEL.LAN_DETAILS);
      return;
    }
    setAndroidSecondary("lan-details");
  }

  function handleOpenSettings() {
    setDetailsTarget(null);
    setAndroidSecondary(null);
    if (isAndroid) {
      void router.navigate("/preference");
      return;
    }
    void showWindow(WINDOW_LABEL.PREFERENCE);
  }

  function handleCloseDevices() {
    setDevicesOpen(false);
  }

  function handleCloseRecords() {
    setRecordsOpen(false);
  }

  function handleCloseAndroidSecondary() {
    setAndroidSecondary(null);
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
  const lanSummary = summarizeLanPeers(status?.peers ?? []);

  const activeDirectory =
    cloudSpaceKey && cloudDirectoryState.key === cloudSpaceKey
      ? cloudDirectoryState
      : null;

  const lanDetails = (
    <SyncLanDetails
      onOpenDetails={handleOpenLanDetailsWindow}
      onOpenSettings={handleOpenSettings}
      onReconnect={handleReconnect}
      reconnectingKey={reconnectingKey}
      showHeader={!isAndroid}
      status={status}
    />
  );

  const cloudDetails = (
    <SyncCloudDetails
      directory={activeDirectory?.directory ?? null}
      directoryError={activeDirectory?.error ?? false}
      directoryLoading={activeDirectory?.loading ?? false}
      onOpenDetails={handleOpenCloudDetailsWindow}
      onOpenDevices={handleOpenDevices}
      onOpenRecords={handleOpenRecords}
      onOpenSettings={handleOpenSettings}
      onReconnect={handleReconnectCloud}
      onRefreshDirectory={handleRefreshCloudDirectory}
      reconnecting={reconnectingKey === "cloud"}
      showHeader={!isAndroid}
      status={status}
    />
  );

  const lanBadgeCount =
    status?.enabled && status.lanEnabled && lanSummary.onlineCount > 0
      ? lanSummary.onlineCount
      : 0;
  const cloudBadgeCount = 0;

  return (
    <>
      <div className="flex shrink-0 items-center gap-0.5">
        {isAndroid ? (
          <>
            {renderStatusButton({
              badgeCount: lanBadgeCount,
              badgeTone: "success",
              buttonClassName,
              icon:
                lanState === "disabled" ? "i-lucide:wifi-off" : "i-lucide:wifi",
              label: lanLabel,
              onClick: handleOpenLanDetails,
              state: lanState,
            })}
            {renderStatusButton({
              badgeCount: cloudBadgeCount,
              badgeTone: "warning",
              buttonClassName,
              icon:
                cloudState === "disabled"
                  ? "i-lucide:cloud-off"
                  : "i-lucide:cloud",
              label: cloudLabel,
              onClick: handleOpenCloudDetails,
              state: cloudState,
            })}
          </>
        ) : (
          <>
            <Popover
              content={
                <HeaderActionPopoverPanel maxWidth={PANEL_MAX_WIDTH}>
                  {lanDetails}
                </HeaderActionPopoverPanel>
              }
              onOpenChange={handleLanDetailsOpenChange}
              open={detailsTarget === "lan"}
              placement="bottomRight"
              tooltip={{ mouseEnterDelay: 0.3, title: lanLabel }}
              trigger="click"
            >
              {renderStatusButton({
                badgeCount: lanBadgeCount,
                badgeTone: "success",
                buttonClassName,
                icon:
                  lanState === "disabled"
                    ? "i-lucide:wifi-off"
                    : "i-lucide:wifi",
                label: lanLabel,
                state: lanState,
              })}
            </Popover>

            <Popover
              content={
                <HeaderActionPopoverPanel maxWidth={PANEL_MAX_WIDTH}>
                  {cloudDetails}
                </HeaderActionPopoverPanel>
              }
              onOpenChange={handleCloudDetailsOpenChange}
              open={detailsTarget === "cloud"}
              placement="bottomRight"
              tooltip={{ mouseEnterDelay: 0.3, title: cloudLabel }}
              trigger="click"
            >
              {renderStatusButton({
                badgeCount: cloudBadgeCount,
                badgeTone: "warning",
                buttonClassName,
                icon:
                  cloudState === "disabled"
                    ? "i-lucide:cloud-off"
                    : "i-lucide:cloud",
                label: cloudLabel,
                state: cloudState,
              })}
            </Popover>
          </>
        )}
      </div>

      {isAndroid ? (
        <>
          <Drawer
            destroyOnHidden
            extra={
              <div className="flex items-center gap-0.5">
                {detailsTarget === "lan" ? (
                  <>
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
                    <Button
                      aria-label={t("syncStatus.lan.detailsTitle")}
                      disabled={!status?.lanEnabled || !status?.paired}
                      icon={<i className="i-lucide:info size-4" />}
                      onClick={handleOpenLanDetailsWindow}
                      type="text"
                    />
                  </>
                ) : (
                  <Button
                    aria-label={t("syncStatus.cloud.reconnect")}
                    disabled={
                      !cloudSpaceKey ||
                      cloudState === "connecting" ||
                      reconnectingKey !== null
                    }
                    icon={<i className="i-lucide:refresh-cw size-4" />}
                    loading={reconnectingKey === "cloud"}
                    onClick={handleReconnectCloud}
                    type="text"
                  />
                )}
                <Button
                  aria-label={t("syncStatus.actions.openSettings")}
                  icon={<i className="i-lucide:settings size-4" />}
                  onClick={handleOpenSettings}
                  type="text"
                />
              </div>
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
            {detailsTarget === "lan" ? lanDetails : cloudDetails}
          </Drawer>

          <CloudDevicesDrawer onClose={handleCloseDevices} open={devicesOpen} />
          <CloudRecordsDrawer onClose={handleCloseRecords} open={recordsOpen} />

          <Drawer
            destroyOnHidden
            extra={
              androidSecondary === "lan-details" ? (
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
              ) : (
                <Button
                  aria-label={t("syncStatus.cloud.reconnect")}
                  disabled={
                    !cloudSpaceKey ||
                    cloudState === "connecting" ||
                    reconnectingKey !== null
                  }
                  icon={<i className="i-lucide:refresh-cw size-4" />}
                  loading={reconnectingKey === "cloud"}
                  onClick={handleReconnectCloud}
                  type="text"
                />
              )
            }
            onClose={handleCloseAndroidSecondary}
            open={androidSecondary !== null}
            placement="right"
            size="large"
            title={
              androidSecondary === "lan-details"
                ? t("syncStatus.lan.detailsTitle")
                : t("syncStatus.actions.moreInfo")
            }
          >
            {androidSecondary === "lan-details" ? (
              <LanNetworkDetailsContent
                onOpenSettings={handleOpenSettings}
                onReconnect={handleReconnect}
                reconnectingKey={reconnectingKey}
                status={status}
              />
            ) : (
              <CloudNetworkDetailsContent
                onOpenSettings={handleOpenSettings}
                status={status}
              />
            )}
          </Drawer>
        </>
      ) : null}
    </>
  );
};

interface StatusButtonOptions {
  badgeCount: number;
  badgeTone: "success" | "warning";
  buttonClassName: string;
  icon: string;
  label: string;
  onClick?: MouseEventHandler<HTMLButtonElement>;
  state: SyncChannelState;
}

/** 渲染局域网与云端状态按钮，附带轻量角标。 */
function renderStatusButton(options: StatusButtonOptions): ReactElement {
  const {
    badgeCount,
    badgeTone,
    buttonClassName,
    icon,
    label,
    onClick,
    state,
  } = options;
  const showErrorDot = state === "error" || state === "degraded";

  return (
    <button
      aria-label={label}
      className={cn(
        "relative flex cursor-pointer items-center justify-center rounded-full border-0 bg-transparent transition-colors hover:bg-ant-fill-tertiary",
        buttonClassName,
        channelTextClassName(state),
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
      {badgeCount > 0 ? (
        <span
          className={cn(
            "pointer-events-none absolute top-0.5 right-0.5 origin-top-right scale-75 font-semibold text-xs tabular-nums leading-none",
            badgeTone === "success" ? "text-ant-success" : "text-ant-warning",
          )}
        >
          {Math.min(badgeCount, 9)}
        </span>
      ) : showErrorDot ? (
        <span
          className={cn(
            "absolute top-1 right-1 size-1.5 rounded-full",
            state === "error" ? "bg-ant-error" : "bg-ant-warning",
          )}
        />
      ) : null}
    </button>
  );
}

export default SyncStatusIcons;
