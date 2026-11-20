import { listen } from "@tauri-apps/api/event";
import { useEventListener, useMount, useUnmount } from "ahooks";
import { Button } from "antd";
import type { FC, ReactNode } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  getSyncStatus,
  hideWindow,
  reconnectCloud,
  reconnectSyncPeer,
  type SyncStatus,
  showWindow,
} from "@/commands";
import ScrollArea from "@/components/ScrollArea";
import Tooltip from "@/components/Tooltip";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useWindowLifecycle } from "@/hooks/useWindowLifecycle";
import { useWindowReady } from "@/hooks/useWindowReady";
import {
  CloudDevicesContent,
  useCloudDevicesData,
} from "@/pages/Clipboard/components/CloudDevicesPanel";
import { CloudNetworkDetailsContent } from "@/pages/Clipboard/components/CloudNetworkDetailsPanel";
import {
  CloudRecordsContent,
  useCloudRecordsData,
} from "@/pages/Clipboard/components/CloudRecordsPanel";
import { LanNetworkDetailsContent } from "@/pages/Clipboard/components/LanNetworkDetailsPanel";
import {
  createSyncStatusRefresh,
  summarizeLanPeers,
} from "@/pages/Clipboard/components/SyncStatusIcons.logic";
import { isCurrentWindowDormant } from "@/stores/windowLifecycle";
import { cn } from "@/utils/cn";
import { isMac } from "@/utils/is";
import { log } from "@/utils/log";

interface SyncPopupShellProps {
  children: ReactNode;
  extra?: ReactNode;
  icon: string;
  iconClassName?: string;
  scrollable?: boolean;
  subtitle?: ReactNode;
  title: string;
  windowLabel: string;
}

/** 桌面端二级同步弹窗统一外壳：负责标题栏拖拽、安全区适配、Esc 关闭与内容滚区。 */
const SyncPopupShell: FC<SyncPopupShellProps> = (props) => {
  const {
    children,
    extra,
    icon,
    iconClassName,
    scrollable = true,
    subtitle,
    title,
    windowLabel,
  } = props;
  useWindowReady();

  function handleKeyDown(event: KeyboardEvent) {
    if (event.key !== "Escape") return;

    void hideWindow(windowLabel);
  }

  useEventListener("keydown", handleKeyDown);

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-ant-layout text-ant-text">
      <header
        className={cn(
          "flex shrink-0 items-center justify-between gap-3 border-ant-border-secondary border-b bg-ant-container",
          isMac ? "pt-3 pr-4 pb-3 pl-20" : "px-4 py-3",
        )}
        data-tauri-drag-region
      >
        <div className="flex min-w-0 items-center gap-2" data-tauri-drag-region>
          <i className={cn("size-4 shrink-0", icon, iconClassName)} />
          <span
            className="truncate font-semibold text-ant-text text-sm"
            data-tauri-drag-region
          >
            {title}
          </span>
          {subtitle ? (
            <span className="shrink-0 rounded-full bg-ant-fill-tertiary px-2 py-0.5 text-ant-secondary text-xs tabular-nums">
              {subtitle}
            </span>
          ) : null}
        </div>
        {extra ? (
          <div className="flex shrink-0 items-center gap-1">{extra}</div>
        ) : null}
      </header>

      {scrollable ? (
        <ScrollArea
          className="min-h-0 flex-1"
          contentClassName="flex flex-col gap-3 p-4"
        >
          {children}
        </ScrollArea>
      ) : (
        <div className="min-h-0 flex-1 p-4">{children}</div>
      )}
    </div>
  );
};

/** 独立桌面窗口持续订阅实时同步状态并随显隐自动刷新。 */
function useLiveSyncStatus() {
  const lifecycle = useWindowLifecycle();
  const [status, setStatus] = useState<SyncStatus | null>(null);
  const mountedRef = useRef(false);
  const loadedRef = useRef(false);
  const dirtyRef = useRef(false);
  const unlistenRef = useRef<null | (() => void)>(null);
  const refreshRef = useRef<null | (() => Promise<void> | undefined)>(null);

  if (!refreshRef.current) {
    refreshRef.current = createSyncStatusRefresh({
      isActive: () => {
        return mountedRef.current && !isCurrentWindowDormant();
      },
      onError: (error) => {
        log.error("load sync status failed in popup", error);
      },
      read: getSyncStatus,
      write: (nextStatus) => {
        loadedRef.current = true;
        setStatus(nextStatus);
      },
    });
  }
  const refresh = refreshRef.current;

  const initialize = useCallback(async () => {
    try {
      const unlisten = await listen(TAURI_EVENT.SYNC_UPDATED, () => {
        if (!mountedRef.current || isCurrentWindowDormant()) {
          dirtyRef.current = true;
          return;
        }
        void refresh();
      });
      if (!mountedRef.current) {
        unlisten();
        return;
      }
      unlistenRef.current = unlisten;
      void refresh();
    } catch (error) {
      log.error("subscribe sync status failed in popup", error);
    }
  }, [refresh]);

  useMount(() => {
    mountedRef.current = true;
    void initialize();
  });

  useUnmount(() => {
    mountedRef.current = false;
    unlistenRef.current?.();
  });

  useEffect(() => {
    if (!lifecycle.initialized || !lifecycle.visible) return;
    if (!dirtyRef.current && loadedRef.current) return;

    dirtyRef.current = false;
    void refresh();
  }, [lifecycle.initialized, lifecycle.visible, refresh]);

  return {
    setStatus,
    status,
  };
}

/** 打开偏好设置窗口。 */
function handleOpenPreferenceWindow() {
  void showWindow(WINDOW_LABEL.PREFERENCE);
}

/** 云端设备管理独立弹窗页面。 */
export const CloudDevicesPage: FC = () => {
  const { t } = useTranslation("clipboard");
  const lifecycle = useWindowLifecycle();
  const { devices, hubStatus, load, loading, onlineCount } =
    useCloudDevicesData();

  useEffect(() => {
    if (!lifecycle.initialized || !lifecycle.visible) return;

    void load();
  }, [lifecycle.initialized, lifecycle.visible, load]);

  function handleRefresh() {
    void load();
  }

  return (
    <SyncPopupShell
      extra={
        <Tooltip title={t("syncStatus.devices.refresh")}>
          <Button
            aria-label={t("syncStatus.devices.refresh")}
            icon={<i className="i-lucide:refresh-cw size-4" />}
            loading={loading}
            onClick={handleRefresh}
            shape="circle"
            size="small"
            type="text"
          />
        </Tooltip>
      }
      icon="i-lucide:monitor-smartphone"
      iconClassName="text-ant-info"
      subtitle={t("syncStatus.devices.summary", {
        online: onlineCount,
        total: devices.length,
      })}
      title={t("syncStatus.devices.title")}
      windowLabel={WINDOW_LABEL.CLOUD_DEVICES}
    >
      <CloudDevicesContent
        devices={devices}
        hubStatus={hubStatus}
        loading={loading}
      />
    </SyncPopupShell>
  );
};

/** 云端剪贴板记录独立弹窗页面。 */
export const CloudRecordsPage: FC = () => {
  const { t } = useTranslation("clipboard");
  const lifecycle = useWindowLifecycle();
  const { load, loadMore, loading, nextBeforeCursor, records, total } =
    useCloudRecordsData();

  useEffect(() => {
    if (!lifecycle.initialized || !lifecycle.visible) return;

    void load(true);
  }, [lifecycle.initialized, lifecycle.visible, load]);

  function handleRefresh() {
    void load(true);
  }

  return (
    <SyncPopupShell
      extra={
        <Tooltip title={t("syncStatus.records.refresh")}>
          <Button
            aria-label={t("syncStatus.records.refresh")}
            icon={<i className="i-lucide:refresh-cw size-4" />}
            loading={loading}
            onClick={handleRefresh}
            shape="circle"
            size="small"
            type="text"
          />
        </Tooltip>
      }
      icon="i-lucide:cloud"
      iconClassName="text-ant-primary"
      scrollable={false}
      subtitle={t("syncStatus.records.total", { count: total })}
      title={t("syncStatus.records.title")}
      windowLabel={WINDOW_LABEL.CLOUD_RECORDS}
    >
      <CloudRecordsContent
        loading={loading}
        nextBeforeCursor={nextBeforeCursor}
        onEndReached={loadMore}
        records={records}
      />
    </SyncPopupShell>
  );
};

/** 云端节点与双通道网络详情独立弹窗页面。 */
export const CloudDetailsPage: FC = () => {
  const { t } = useTranslation("clipboard");
  const { setStatus, status } = useLiveSyncStatus();
  const [reconnecting, setReconnecting] = useState(false);

  const isConfigured = Boolean(
    status?.enabled &&
      status.cloudEnabled &&
      status.paired &&
      status.cloudEndpointId,
  );

  async function handleReconnect() {
    setReconnecting(true);
    try {
      setStatus(await reconnectCloud());
    } catch {
      // 命令层已统一记录并显示错误。
    } finally {
      setReconnecting(false);
    }
  }

  return (
    <SyncPopupShell
      extra={
        <>
          <Tooltip title={t("syncStatus.cloud.reconnect")}>
            <Button
              aria-label={t("syncStatus.cloud.reconnect")}
              disabled={!isConfigured || status?.cloud.state === "connecting"}
              icon={<i className="i-lucide:refresh-cw size-4" />}
              loading={reconnecting}
              onClick={handleReconnect}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
          <Tooltip title={t("syncStatus.actions.openSettings")}>
            <Button
              aria-label={t("syncStatus.actions.openSettings")}
              icon={<i className="i-lucide:settings size-4" />}
              onClick={handleOpenPreferenceWindow}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
        </>
      }
      icon="i-lucide:server"
      iconClassName="text-ant-info"
      subtitle={
        status?.cloudEnabled && status.cloudServerVersion
          ? t("syncStatus.cloud.serverVersion", {
              version: status.cloudServerVersion,
            })
          : void 0
      }
      title={t("syncStatus.actions.moreInfo")}
      windowLabel={WINDOW_LABEL.CLOUD_DETAILS}
    >
      <CloudNetworkDetailsContent
        onOpenSettings={handleOpenPreferenceWindow}
        status={status}
      />
    </SyncPopupShell>
  );
};

/** 局域网节点与设备完整详情独立弹窗页面。 */
export const LanDetailsPage: FC = () => {
  const { t } = useTranslation("clipboard");
  const { setStatus, status } = useLiveSyncStatus();
  const [reconnectingKey, setReconnectingKey] = useState<string | null>(null);

  const summary = summarizeLanPeers(status?.peers ?? []);
  const canOperate = Boolean(
    status?.enabled && status.lanEnabled && status.paired,
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

  function handleReconnectAll() {
    void handleReconnect();
  }

  return (
    <SyncPopupShell
      extra={
        <>
          <Tooltip title={t("syncStatus.lan.reconnectAll")}>
            <Button
              aria-label={t("syncStatus.lan.reconnectAll")}
              disabled={
                !canOperate ||
                summary.totalCount === 0 ||
                reconnectingKey !== null
              }
              icon={<i className="i-lucide:refresh-cw size-4" />}
              loading={reconnectingKey === "all"}
              onClick={handleReconnectAll}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
          <Tooltip title={t("syncStatus.actions.openSettings")}>
            <Button
              aria-label={t("syncStatus.actions.openSettings")}
              icon={<i className="i-lucide:settings size-4" />}
              onClick={handleOpenPreferenceWindow}
              shape="circle"
              size="small"
              type="text"
            />
          </Tooltip>
        </>
      }
      icon="i-lucide:wifi"
      iconClassName="text-ant-success"
      subtitle={
        canOperate && summary.totalCount > 0
          ? t("syncStatus.lan.onlineRatio", {
              online: summary.onlineCount,
              total: summary.totalCount,
            })
          : void 0
      }
      title={t("syncStatus.lan.detailsTitle")}
      windowLabel={WINDOW_LABEL.LAN_DETAILS}
    >
      <LanNetworkDetailsContent
        onOpenSettings={handleOpenPreferenceWindow}
        onReconnect={handleReconnect}
        reconnectingKey={reconnectingKey}
        status={status}
      />
    </SyncPopupShell>
  );
};
