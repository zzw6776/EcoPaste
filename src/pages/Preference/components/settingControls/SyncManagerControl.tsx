import { listen } from "@tauri-apps/api/event";
import { useMount, useUnmount } from "ahooks";
import {
  Button,
  Input,
  Radio,
  type RadioChangeEvent,
  Space,
  Switch,
  Typography,
} from "antd";
import { QRCodeSVG } from "qrcode.react";
import type { ChangeEvent, FC } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  createSyncGroup,
  discoverNearbySyncSpaces,
  exportSyncPairingCode,
  getSyncStatus,
  inspectSyncPairingCode,
  joinSyncGroup,
  leaveSyncGroup,
  type NearbyJoinAttempt,
  type NearbySyncDevice,
  type NearbySyncSpace,
  reconnectSyncPeer,
  removeSyncPeer,
  requestNearbySyncJoin,
  type SyncPeerStatus,
  type SyncStatus,
  setCloudRelayAuthToken,
  setSyncDeviceName,
  syncNow,
} from "@/commands";
import Modal from "@/components/Modal";
import { TAURI_EVENT } from "@/constants/events";
import { useAndroidBack } from "@/hooks/useAndroidBack";
import CloudRecordsDrawer from "@/pages/Clipboard/components/CloudRecordsDrawer";
import { updateSettings } from "@/stores/settings";
import type { CloudRelayMode } from "@/types/settings";
import { cn } from "@/utils/cn";
import { getModalApi } from "@/utils/feedback";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";
import SyncQrScanner, { type SyncQrScannerHandle } from "./SyncQrScanner";

interface SyncManagerControlProps {
  disabled: boolean;
}

const SyncManagerControl: FC<SyncManagerControlProps> = (props) => {
  const { disabled } = props;
  const { t } = useTranslation("preferences");
  const [status, setStatus] = useState<SyncStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [reconnectingKey, setReconnectingKey] = useState<string | null>(null);
  const [removingKey, setRemovingKey] = useState<string | null>(null);
  const [pairingCode, setPairingCode] = useState("");
  const [pairingCodeOpen, setPairingCodeOpen] = useState(false);
  const [joinOpen, setJoinOpen] = useState(false);
  const [scannerOpen, setScannerOpen] = useState(false);
  const [joinCode, setJoinCode] = useState("");
  const [deviceName, setDeviceName] = useState("");
  const [cloudEndpointId, setCloudEndpointId] = useState("");
  const [cloudDirectAddresses, setCloudDirectAddresses] = useState("");
  const [cloudRelayUrls, setCloudRelayUrls] = useState("");
  const [cloudRelayMode, setCloudRelayMode] = useState<CloudRelayMode>("off");
  const [cloudRelayToken, setCloudRelayToken] = useState("");
  const [recordsOpen, setRecordsOpen] = useState(false);
  const [nearbySpaces, setNearbySpaces] = useState<NearbySyncSpace[]>([]);
  const [nearbyLoading, setNearbyLoading] = useState(false);
  const [joinAttempt, setJoinAttempt] = useState<NearbyJoinAttempt | null>(
    null,
  );
  const [cloudConfigOpen, setCloudConfigOpen] = useState(false);
  const deviceNameDirtyRef = useRef(false);
  const cloudConfigDirtyRef = useRef(false);
  const cloudRelayTokenDirtyRef = useRef(false);
  const mountedRef = useRef(false);
  const qrScannerRef = useRef<SyncQrScannerHandle | null>(null);
  const unlistenRef = useRef<null | (() => void)>(null);
  const joinUnlistenRef = useRef<null | (() => void)>(null);

  async function refresh() {
    try {
      const next = await getSyncStatus();
      setStatus(next);
      if (!deviceNameDirtyRef.current) {
        setDeviceName(next.deviceName);
      }
      if (!cloudConfigDirtyRef.current) {
        setCloudEndpointId(next.cloudEndpointId);
        setCloudDirectAddresses(next.cloudDirectAddresses.join("\n"));
        setCloudRelayUrls(next.cloudRelayUrls.join("\n"));
        setCloudRelayMode(next.cloudRelayMode);
        if (!cloudRelayTokenDirtyRef.current) {
          setCloudRelayToken("");
        }
      }
    } catch (error) {
      log.error("load sync settings status failed", error);
    }
  }

  async function initialize() {
    try {
      await refresh();
      const unlisten = await listen(TAURI_EVENT.SYNC_UPDATED, refresh);
      const joinUnlisten = await listen<NearbyJoinAttempt>(
        TAURI_EVENT.SYNC_JOIN_ATTEMPT_UPDATED,
        (event) => {
          setJoinAttempt((current) => {
            if (current?.requestId !== event.payload.requestId) return current;

            return event.payload;
          });
        },
      );
      if (!mountedRef.current) {
        unlisten();
        joinUnlisten();
        return;
      }
      unlistenRef.current = unlisten;
      joinUnlistenRef.current = joinUnlisten;
    } catch (error) {
      log.error("initialize sync settings failed", error);
    }
  }

  useMount(() => {
    mountedRef.current = true;
    void initialize();
  });

  useUnmount(() => {
    mountedRef.current = false;
    unlistenRef.current?.();
    joinUnlistenRef.current?.();
  });

  async function run(action: () => Promise<unknown>) {
    setLoading(true);
    try {
      await action();
      await refresh();
    } catch {
      // 命令层已统一记录并显示错误。
    } finally {
      setLoading(false);
    }
  }

  async function handleCreate() {
    await run(async () => {
      const code = await createSyncGroup();
      setPairingCode(code);
      setPairingCodeOpen(true);
    });
  }

  async function handleEnable() {
    await run(async () => {
      await updateSettings({ sync: { enabled: true } });
    });
  }

  async function handleExport() {
    await run(async () => {
      const code = await exportSyncPairingCode();
      setPairingCode(code);
      setPairingCodeOpen(true);
    });
  }

  async function joinWithPairingCode(
    pairingCode: string,
    replaceExisting: boolean,
  ) {
    await run(async () => {
      await joinSyncGroup(pairingCode, replaceExisting);
      setJoinCode("");
      setJoinOpen(false);
      setJoinAttempt(null);
    });
  }

  async function processPairingCode(pairingCode: string) {
    const code = pairingCode.trim();
    setJoinCode(code);
    closeScanner();
    const preview = await inspectSyncPairingCode(code);
    if (!status?.paired || preview.sameGroup) {
      await joinWithPairingCode(code, false);
      return;
    }

    setJoinOpen(false);
    getModalApi().confirm({
      cancelText: t("sync.conflict.keepLocal"),
      centered: true,
      content: t("sync.conflict.warning", {
        device: preview.inviterDeviceName,
      }),
      okText: t("sync.conflict.joinRemote", {
        device: preview.inviterDeviceName,
      }),
      onCancel: async () => {
        await handleExport();
        setJoinAttempt(null);
      },
      onOk: async () => {
        await joinWithPairingCode(code, true);
      },
      title: t("sync.conflict.title"),
    });
  }

  async function handleJoin() {
    try {
      await processPairingCode(joinCode);
    } catch {
      // 命令层已统一记录并显示错误。
    }
  }

  async function joinScannedDevice(pairingCode: string) {
    try {
      await processPairingCode(pairingCode);
    } catch {
      // 命令层已统一显示错误，保留配对码输入界面供用户重试。
    }
  }

  function handleScanDetected(pairingCode: string) {
    void joinScannedDevice(pairingCode);
  }

  function handleLeave() {
    getModalApi().confirm({
      cancelText: t("sync.actions.cancel"),
      centered: true,
      content: t("sync.leaveWarning"),
      okButtonProps: { danger: true },
      okText: t("sync.actions.leave"),
      onOk: async () => {
        await run(leaveSyncGroup);
      },
      title: t("sync.leaveTitle"),
    });
  }

  async function handleSaveName() {
    await run(async () => {
      await setSyncDeviceName(deviceName);
      deviceNameDirtyRef.current = false;
    });
  }

  function handleDeviceNameChange(event: ChangeEvent<HTMLInputElement>) {
    deviceNameDirtyRef.current = true;
    setDeviceName(event.target.value);
  }

  function handleSyncNow() {
    void run(syncNow);
  }

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

  async function handleRemovePeer(deviceId: string) {
    setRemovingKey(deviceId);
    try {
      setStatus(await removeSyncPeer(deviceId));
    } catch {
      // 命令层已统一记录并显示错误。
    } finally {
      setRemovingKey(null);
    }
  }

  function handleRemovePeerRequest(peer: SyncPeerStatus) {
    getModalApi().confirm({
      cancelText: t("sync.actions.cancel"),
      centered: true,
      content: t("sync.removeWarning", { device: peer.deviceName }),
      okButtonProps: { danger: true },
      okText: t("sync.actions.removeDevice"),
      onOk: async () => {
        await handleRemovePeer(peer.deviceId);
      },
      title: t("sync.removeTitle", { device: peer.deviceName }),
    });
  }

  async function handleLanEnabledChange(lanEnabled: boolean) {
    await run(async () => {
      await updateSettings({ sync: { lanEnabled } });
    });
  }

  function handleLanSwitch(checked: boolean) {
    void handleLanEnabledChange(checked);
  }

  async function handleCloudEnabledChange(cloudEnabled: boolean) {
    if (!cloudEnabled) {
      setCloudConfigOpen(false);
    }
    await run(async () => {
      await updateSettings({ sync: { cloudEnabled } });
    });
  }

  function handleCloudSwitch(checked: boolean) {
    void handleCloudEnabledChange(checked);
  }

  function handleCloudEndpointIdChange(event: ChangeEvent<HTMLInputElement>) {
    cloudConfigDirtyRef.current = true;
    setCloudEndpointId(event.target.value);
  }

  function handleCloudDirectAddressesChange(
    event: ChangeEvent<HTMLTextAreaElement>,
  ) {
    cloudConfigDirtyRef.current = true;
    setCloudDirectAddresses(event.target.value);
  }

  function handleCloudRelayUrlsChange(event: ChangeEvent<HTMLTextAreaElement>) {
    cloudConfigDirtyRef.current = true;
    setCloudRelayUrls(event.target.value);
  }

  function handleCloudRelayEnabledChange(checked: boolean) {
    cloudConfigDirtyRef.current = true;
    setCloudRelayMode(checked ? "public" : "off");
  }

  function handleCloudRelayModeChange(event: RadioChangeEvent) {
    cloudConfigDirtyRef.current = true;
    setCloudRelayMode(event.target.value as CloudRelayMode);
  }

  function handleCloudRelayTokenChange(event: ChangeEvent<HTMLInputElement>) {
    cloudConfigDirtyRef.current = true;
    cloudRelayTokenDirtyRef.current = true;
    setCloudRelayToken(event.target.value);
  }

  async function handleSaveCloudConfig() {
    await run(async () => {
      await updateSettings({
        sync: {
          cloudRelayMode,
          serverDirectAddresses: splitAddressLines(cloudDirectAddresses),
          serverEndpointId: cloudEndpointId.trim(),
          serverRelayUrls: splitAddressLines(cloudRelayUrls),
        },
      });
      if (cloudRelayTokenDirtyRef.current) {
        await setCloudRelayAuthToken(cloudRelayToken.trim() || null);
        cloudRelayTokenDirtyRef.current = false;
        setCloudRelayToken("");
      }
      cloudConfigDirtyRef.current = false;
    });
  }

  function handleOpenRecords() {
    setRecordsOpen(true);
  }

  function handleCloseRecords() {
    setRecordsOpen(false);
  }

  function handleCloudConfigToggle() {
    setCloudConfigOpen((open) => {
      return !open;
    });
  }

  function closeCloudConfig() {
    setCloudConfigOpen(false);
  }

  const busy = loading || reconnectingKey !== null || removingKey !== null;

  async function scanNearby() {
    setNearbyLoading(true);
    try {
      setNearbySpaces(await discoverNearbySyncSpaces());
    } catch {
      setNearbySpaces([]);
    } finally {
      setNearbyLoading(false);
    }
  }

  async function handleRequestNearbyJoin(endpointId: string) {
    try {
      setJoinAttempt(await requestNearbySyncJoin(endpointId));
    } catch {
      // 命令层已统一记录并显示错误。
    }
  }

  function requestNearbyJoin(endpointId: string) {
    void handleRequestNearbyJoin(endpointId);
  }

  function handleRefreshNearby() {
    void scanNearby();
  }

  async function handleUseApprovedJoin() {
    if (!joinAttempt?.pairingCode) return;

    await processPairingCode(joinAttempt.pairingCode);
  }

  function handleApprovedJoin() {
    void handleUseApprovedJoin();
  }

  function handleOpenJoin() {
    setScannerOpen(false);
    setNearbySpaces([]);
    setJoinOpen(true);
    void scanNearby();
  }

  function handleOpenScanner() {
    setScannerOpen(true);
  }

  function closeScanner() {
    qrScannerRef.current?.stop();
    setScannerOpen(false);
  }

  function handleUsePairingCode() {
    closeScanner();
  }

  function handlePairingCodeCancel() {
    setPairingCodeOpen(false);
    setPairingCode("");
  }

  function handleJoinCancel() {
    closeScanner();
    setJoinOpen(false);
    setNearbySpaces([]);
  }

  function handleJoinCodeChange(event: ChangeEvent<HTMLTextAreaElement>) {
    setJoinCode(event.target.value);
  }

  useAndroidBack(scannerOpen, closeScanner);
  useAndroidBack(cloudConfigOpen, closeCloudConfig);

  return (
    <div className="flex w-full flex-col gap-2">
      <div className="rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-3">
        <div className="flex items-start gap-2">
          <div className="flex size-8 shrink-0 items-center justify-center rounded-2 bg-ant-container text-ant-primary">
            <i className="i-lucide:smartphone size-4" />
          </div>
          <div className="min-w-0">
            <div className="font-medium text-sm">
              {t("sync.deviceName.title")}
            </div>
            <div className="text-ant-secondary text-xs">
              {t("sync.deviceName.description")}
            </div>
          </div>
        </div>
        <div className="mt-3 flex w-full gap-2">
          <Input
            aria-label={t("sync.deviceName.title")}
            className="min-w-0 flex-1"
            disabled={disabled || busy}
            onChange={handleDeviceNameChange}
            placeholder={t("sync.deviceName.placeholder")}
            value={deviceName}
          />
          <Button
            className="shrink-0"
            disabled={disabled || busy}
            onClick={handleSaveName}
          >
            {t("sync.actions.saveName")}
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 items-start gap-2 md:grid-cols-2">
        <div className="rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-3">
          <div className="flex items-center gap-2">
            <i
              className={cn(
                "i-lucide:wifi size-4",
                channelStateClassName(status?.lan.state),
              )}
            />
            <strong className="text-sm">{t("sync.channels.lan")}</strong>
            <span
              className={cn(
                "text-xs",
                channelStateClassName(status?.lan.state),
              )}
            >
              {t(`sync.states.${status?.lan.state ?? "disabled"}`)}
            </span>
            <Switch
              aria-label={t("sync.lan.enabled")}
              checked={status?.lanEnabled ?? false}
              className="ml-auto"
              disabled={disabled || busy || !status?.enabled}
              onChange={handleLanSwitch}
              size="small"
            />
            <Button
              aria-label={t("sync.actions.reconnectAll")}
              disabled={
                disabled || busy || !status?.lanEnabled || !status?.peers.length
              }
              icon={<i className="i-lucide:refresh-cw size-3.5" />}
              loading={reconnectingKey === "all"}
              onClick={handleReconnectAll}
              size="small"
              title={t("sync.actions.reconnectAll")}
              type="text"
            />
          </div>
          <div className="mt-2 flex flex-col gap-2">
            {status?.peers.length ? (
              status.peers.map((peer) => {
                return (
                  <SyncPeerSetting
                    disabled={disabled || busy}
                    key={peer.deviceId}
                    onReconnect={handleReconnect}
                    onRemove={handleRemovePeerRequest}
                    peer={peer}
                    reconnectDisabled={!status.lanEnabled}
                    reconnecting={reconnectingKey === peer.deviceId}
                    removing={removingKey === peer.deviceId}
                  />
                );
              })
            ) : (
              <span className="text-ant-secondary text-xs">
                {t("sync.noPeers")}
              </span>
            )}
          </div>
        </div>

        <div className="rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-3">
          <div className="flex items-center gap-2">
            <i
              className={cn(
                "i-lucide:cloud size-4",
                channelStateClassName(status?.cloud.state),
              )}
            />
            <strong className="text-sm">{t("sync.channels.cloud")}</strong>
            <span
              className={cn(
                "text-xs",
                channelStateClassName(status?.cloud.state),
              )}
            >
              {t(`sync.states.${status?.cloud.state ?? "disabled"}`)}
            </span>
            <Switch
              aria-label={t("sync.cloud.enabled")}
              checked={status?.cloudEnabled ?? false}
              className="ml-auto"
              disabled={disabled || busy || !status?.enabled}
              onChange={handleCloudSwitch}
              size="small"
            />
          </div>
          {!isAndroid && status?.cloudEnabled && status.cloudEndpointId ? (
            <div className="mt-2 break-all font-mono text-ant-secondary text-xs">
              {status.cloudEndpointId}
            </div>
          ) : null}
          {!isAndroid && status?.cloudEnabled
            ? status.cloudDirectAddresses.map((address) => {
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
          {status?.cloudEnabled && status.cloudConnectedAddress ? (
            <div className="mt-1 break-all font-mono text-ant-secondary text-xs">
              {t("sync.cloud.activeRoute", {
                address: status.cloudConnectedAddress,
                transport: t(
                  `sync.transport.${status.cloudTransport ?? "unknown"}`,
                ),
              })}
            </div>
          ) : null}
          {status?.cloudEnabled ? (
            <div className="mt-1 text-ant-secondary text-xs">
              {status.cloudServerVersion
                ? t("sync.cloud.serverVersion", {
                    version: status.cloudServerVersion,
                  })
                : t("sync.cloud.serverVersionUnknown")}
            </div>
          ) : null}
          {status?.cloudEnabled && status.cloud.lastSuccessAt ? (
            <div className="mt-2 text-ant-secondary text-xs">
              {t("sync.lastSuccess", {
                time: new Date(status.cloud.lastSuccessAt).toLocaleString(),
              })}
            </div>
          ) : null}
          {status?.cloudEnabled && status.cloud.lastError ? (
            <div className="mt-2 text-ant-error text-xs">
              {status.cloud.lastError}
            </div>
          ) : null}
          {status?.cloudEnabled ? (
            <Button
              className="mt-2"
              disabled={
                disabled || busy || !status.paired || !status.cloudEndpointId
              }
              icon={<i className="i-lucide:cloud-download size-4" />}
              onClick={handleOpenRecords}
              size="small"
            >
              {t("sync.cloud.records")}
            </Button>
          ) : (
            <div className="mt-2 text-ant-secondary text-xs">
              {t("sync.cloud.disabledHint")}
            </div>
          )}
        </div>
      </div>

      {status?.cloudEnabled ? (
        <div className="overflow-hidden rounded-2 border border-ant-border-secondary bg-ant-container">
          <button
            aria-expanded={cloudConfigOpen}
            className="flex w-full items-center gap-2 border-0 bg-transparent p-3 text-left text-ant-text"
            onClick={handleCloudConfigToggle}
            type="button"
          >
            <div className="flex size-8 items-center justify-center rounded-2 bg-ant-fill-quaternary text-ant-info">
              <i className="i-lucide:cloud-cog size-4" />
            </div>
            <div className="min-w-0">
              <div className="font-medium text-sm">{t("sync.cloud.title")}</div>
              <div className="text-ant-secondary text-xs">
                {t("sync.cloud.description")}
              </div>
            </div>
            <i
              className={cn(
                "i-lucide:chevron-down ml-auto size-4 transition-transform",
                {
                  "rotate-180": cloudConfigOpen,
                },
              )}
            />
          </button>
          {cloudConfigOpen ? (
            <div className="border-ant-border-secondary border-t p-3">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                <label
                  className="flex flex-col gap-1 md:col-span-2"
                  htmlFor="sync-cloud-endpoint-id"
                >
                  <span className="text-ant-secondary text-xs">
                    {t("sync.cloud.endpointId")}
                  </span>
                  <Input
                    disabled={disabled || busy}
                    id="sync-cloud-endpoint-id"
                    onChange={handleCloudEndpointIdChange}
                    placeholder={t(
                      "settings.sync.serverEndpointId.placeholder",
                    )}
                    value={cloudEndpointId}
                  />
                </label>
                <label
                  className="flex flex-col gap-1"
                  htmlFor="sync-cloud-direct-addresses"
                >
                  <span className="text-ant-secondary text-xs">
                    {t("sync.cloud.directAddresses")}
                  </span>
                  <Input.TextArea
                    autoSize={{ maxRows: 5, minRows: 3 }}
                    disabled={disabled || busy}
                    id="sync-cloud-direct-addresses"
                    onChange={handleCloudDirectAddressesChange}
                    placeholder={t(
                      "settings.sync.serverDirectAddresses.placeholder",
                    )}
                    value={cloudDirectAddresses}
                  />
                </label>
                <div className="flex flex-col gap-2 rounded-2 border border-ant-border-secondary p-3 md:col-span-2">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="font-medium text-sm">
                        {t("sync.cloud.relay.title")}
                      </div>
                      <div className="text-ant-secondary text-xs">
                        {t("sync.cloud.relay.description")}
                      </div>
                    </div>
                    <Switch
                      aria-label={t("sync.cloud.relay.enabled")}
                      checked={cloudRelayMode !== "off"}
                      disabled={disabled || busy}
                      onChange={handleCloudRelayEnabledChange}
                      size="small"
                    />
                  </div>
                  {cloudRelayMode !== "off" ? (
                    <Radio.Group
                      disabled={disabled || busy}
                      onChange={handleCloudRelayModeChange}
                      options={[
                        {
                          label: t("sync.cloud.relay.public"),
                          value: "public",
                        },
                        {
                          label: t("sync.cloud.relay.custom"),
                          value: "custom",
                        },
                      ]}
                      value={cloudRelayMode}
                    />
                  ) : null}
                  {cloudRelayMode === "public" ? (
                    <div className="text-ant-secondary text-xs">
                      {t("sync.cloud.relay.publicHint")}
                    </div>
                  ) : null}
                  {cloudRelayMode === "custom" ? (
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                      <label
                        className="flex flex-col gap-1"
                        htmlFor="sync-cloud-relay-urls"
                      >
                        <span className="text-ant-secondary text-xs">
                          {t("sync.cloud.relayUrls")}
                        </span>
                        <Input.TextArea
                          autoSize={{ maxRows: 5, minRows: 3 }}
                          disabled={disabled || busy}
                          id="sync-cloud-relay-urls"
                          onChange={handleCloudRelayUrlsChange}
                          placeholder={t(
                            "settings.sync.serverRelayUrls.placeholder",
                          )}
                          value={cloudRelayUrls}
                        />
                      </label>
                      <label
                        className="flex flex-col gap-1"
                        htmlFor="sync-cloud-relay-token"
                      >
                        <span className="text-ant-secondary text-xs">
                          {t("sync.cloud.relay.token")}
                        </span>
                        <Input.Password
                          autoComplete="off"
                          disabled={disabled || busy}
                          id="sync-cloud-relay-token"
                          onChange={handleCloudRelayTokenChange}
                          placeholder={
                            status?.cloudRelayAuthConfigured
                              ? t("sync.cloud.relay.tokenConfigured")
                              : t("sync.cloud.relay.tokenPlaceholder")
                          }
                          value={cloudRelayToken}
                        />
                        <span className="text-ant-secondary text-xs">
                          {t("sync.cloud.relay.tokenHint")}
                        </span>
                      </label>
                    </div>
                  ) : null}
                </div>
              </div>
              <div className="mt-3 flex items-center justify-between gap-3">
                <span className="text-ant-secondary text-xs">
                  {t("sync.cloud.domainHint")}
                </span>
                <Button
                  disabled={disabled || busy || !cloudConfigDirtyRef.current}
                  loading={loading}
                  onClick={handleSaveCloudConfig}
                  type="primary"
                >
                  {t("sync.cloud.save")}
                </Button>
              </div>
            </div>
          ) : null}
        </div>
      ) : null}

      <Space wrap>
        {status?.paired ? (
          <>
            {!status.enabled ? (
              <Button
                disabled={disabled || busy}
                onClick={handleEnable}
                type="primary"
              >
                {t("sync.actions.enable")}
              </Button>
            ) : null}
            <Button disabled={disabled || busy} onClick={handleExport}>
              {t("sync.actions.pairDevice")}
            </Button>
            <Button disabled={disabled || busy} onClick={handleOpenJoin}>
              {t("sync.actions.connectOther")}
            </Button>
            <Button
              disabled={disabled || busy}
              loading={loading}
              onClick={handleSyncNow}
            >
              {t("sync.actions.syncNow")}
            </Button>
            <Button danger disabled={disabled || busy} onClick={handleLeave}>
              {t("sync.actions.leave")}
            </Button>
          </>
        ) : (
          <>
            <Button
              disabled={disabled || busy}
              onClick={handleCreate}
              type="primary"
            >
              {t("sync.actions.create")}
            </Button>
            <Button disabled={disabled || busy} onClick={handleOpenJoin}>
              {t("sync.actions.join")}
            </Button>
          </>
        )}
      </Space>

      <Modal
        centered
        footer={null}
        onCancel={handlePairingCodeCancel}
        open={pairingCodeOpen}
        title={t("sync.pairingCodeTitle")}
      >
        <Typography.Paragraph>{t("sync.pairingCodeHint")}</Typography.Paragraph>
        <div className="mb-3 flex justify-center overflow-auto rounded-3 bg-ant-container p-3">
          <QRCodeSVG
            className="h-auto w-96 max-w-full"
            level="L"
            marginSize={4}
            size={384}
            title={t("sync.pairingCodeTitle")}
            value={pairingCode}
          />
        </div>
        <Typography.Text className="text-xs" type="secondary">
          {t("sync.pairingCodeFallback")}
        </Typography.Text>
        <Input.TextArea
          autoSize={{ maxRows: 8, minRows: 4 }}
          readOnly
          value={pairingCode}
        />
      </Modal>

      <Modal
        centered
        footer={scannerOpen ? null : void 0}
        okButtonProps={{ disabled: joinCode.trim().length === 0, loading }}
        okText={t("sync.actions.join")}
        onCancel={handleJoinCancel}
        onOk={handleJoin}
        open={joinOpen}
        title={t(scannerOpen ? "sync.scanner.title" : "sync.joinTitle")}
      >
        <div className="flex flex-col gap-3">
          <div className="rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-3">
            <div className="flex items-center gap-2">
              <div>
                <div className="font-medium text-sm">
                  {t("sync.nearby.title")}
                </div>
                <div className="text-ant-secondary text-xs">
                  {t("sync.nearby.description")}
                </div>
              </div>
              <Button
                className="ml-auto shrink-0"
                icon={<i className="i-lucide:refresh-cw size-4" />}
                loading={nearbyLoading}
                onClick={handleRefreshNearby}
                size="small"
                type="text"
              />
            </div>
            <div className="mt-3 flex flex-col gap-2">
              {nearbySpaces.length ? (
                nearbySpaces.map((space) => {
                  return (
                    <div
                      className="rounded-2 bg-ant-container p-2"
                      key={space.spaceId}
                    >
                      {space.devices.map((device) => {
                        return (
                          <NearbyDeviceSetting
                            currentSpace={space.sameGroup}
                            device={device}
                            disabled={
                              status?.peers.some((peer) => {
                                return peer.endpointId === device.endpointId;
                              }) || joinAttempt?.state === "pending"
                            }
                            key={device.endpointId}
                            onRequest={requestNearbyJoin}
                          />
                        );
                      })}
                    </div>
                  );
                })
              ) : (
                <div className="py-2 text-center text-ant-secondary text-xs">
                  {nearbyLoading
                    ? t("sync.nearby.scanning")
                    : t("sync.nearby.empty")}
                </div>
              )}
            </div>
          </div>

          {joinAttempt ? (
            <div className="rounded-2 border border-ant-border-secondary p-3 text-center">
              <div className="text-ant-secondary text-xs">
                {t("sync.nearby.comparisonCodeHint", {
                  device: joinAttempt.targetDeviceName,
                })}
              </div>
              <div className="mt-1 font-mono font-semibold text-2xl tracking-widest">
                {joinAttempt.comparisonCode}
              </div>
              <div className="mt-2 text-ant-secondary text-xs">
                {t(`sync.nearby.states.${joinAttempt.state}`)}
              </div>
              {joinAttempt.lastError ? (
                <div className="mt-1 text-ant-error text-xs">
                  {joinAttempt.lastError}
                </div>
              ) : null}
              {joinAttempt.state === "approved" && joinAttempt.pairingCode ? (
                <Button
                  className="mt-3"
                  onClick={handleApprovedJoin}
                  type="primary"
                >
                  {t("sync.nearby.joinApproved")}
                </Button>
              ) : null}
            </div>
          ) : null}

          {scannerOpen ? (
            <>
              <SyncQrScanner
                onDetected={handleScanDetected}
                ref={qrScannerRef}
              />
              <Button block onClick={handleUsePairingCode}>
                {t("sync.actions.usePairingCode")}
              </Button>
            </>
          ) : (
            <>
              <Input.TextArea
                autoSize={{ maxRows: 8, minRows: 4 }}
                onChange={handleJoinCodeChange}
                placeholder={t("sync.joinPlaceholder")}
                value={joinCode}
              />
              {isAndroid ? (
                <Button block onClick={handleOpenScanner}>
                  {t("sync.actions.scanQrCode")}
                </Button>
              ) : null}
            </>
          )}
        </div>
      </Modal>
      <CloudRecordsDrawer onClose={handleCloseRecords} open={recordsOpen} />
    </div>
  );
};

export default SyncManagerControl;

interface NearbyDeviceSettingProps {
  currentSpace: boolean;
  device: NearbySyncDevice;
  disabled: boolean;
  onRequest: (endpointId: string) => void;
}

const NearbyDeviceSetting: FC<NearbyDeviceSettingProps> = (props) => {
  const { currentSpace, device, disabled, onRequest } = props;
  const { t } = useTranslation("preferences");

  function handleRequest() {
    onRequest(device.endpointId);
  }

  return (
    <div className="flex items-center gap-2">
      <i className="i-lucide:monitor-smartphone size-4 text-ant-primary" />
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm">{device.deviceName}</div>
        <div className="text-ant-secondary text-xs">
          {device.platform}
          {currentSpace ? ` · ${t("sync.nearby.currentSpace")}` : ""}
        </div>
      </div>
      <Button
        disabled={disabled}
        onClick={handleRequest}
        size="small"
        type="primary"
      >
        {t("sync.nearby.requestJoin")}
      </Button>
    </div>
  );
};

interface SyncPeerSettingProps {
  disabled: boolean;
  onRemove: (peer: SyncPeerStatus) => void;
  onReconnect: (deviceId?: string) => Promise<void>;
  peer: SyncPeerStatus;
  reconnectDisabled: boolean;
  reconnecting: boolean;
  removing: boolean;
}

const SyncPeerSetting: FC<SyncPeerSettingProps> = (props) => {
  const {
    disabled,
    onRemove,
    onReconnect,
    peer,
    reconnectDisabled,
    reconnecting,
    removing,
  } = props;
  const { t } = useTranslation("preferences");
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
  const reconnectLabel = t("sync.actions.reconnectDevice", {
    device: peer.deviceName,
  });
  const removeLabel = t("sync.actions.removeDeviceNamed", {
    device: peer.deviceName,
  });

  function handleReconnect() {
    void onReconnect(peer.deviceId);
  }

  function handleRemove() {
    onRemove(peer);
  }

  function handleCandidateAddressesToggle() {
    setCandidateAddressesOpen((open) => !open);
  }

  function closeCandidateAddresses() {
    setCandidateAddressesOpen(false);
  }

  useAndroidBack(candidateAddressesOpen, closeCandidateAddresses);

  return (
    <div className="text-xs">
      <div className="flex items-center gap-1 font-medium">
        <span className="min-w-0 truncate">
          {peer.deviceName} · {t(`sync.states.${peer.state}`)}
        </span>
        <Button
          aria-label={reconnectLabel}
          className="ml-auto shrink-0"
          disabled={
            disabled || reconnectDisabled || peer.state === "connecting"
          }
          icon={<i className="i-lucide:refresh-cw size-3.5" />}
          loading={reconnecting}
          onClick={handleReconnect}
          size="small"
          title={reconnectLabel}
          type="text"
        />
        <Button
          aria-label={removeLabel}
          className="shrink-0"
          danger
          disabled={disabled}
          icon={<i className="i-lucide:trash-2 size-3.5" />}
          loading={removing}
          onClick={handleRemove}
          size="small"
          title={removeLabel}
          type="text"
        />
      </div>
      <div className="text-ant-secondary">
        {peer.platform}
        {isOnline && peer.transport
          ? ` · ${t(`sync.transport.${peer.transport}`)}`
          : ""}
      </div>
      {connectedAddress ? (
        <div className="break-all font-mono text-ant-secondary">
          {connectedAddress}
        </div>
      ) : null}
      {!isOnline && candidateAddresses.length > 0 ? (
        <>
          <button
            aria-expanded={candidateAddressesOpen}
            className="flex cursor-pointer items-center gap-1 border-0 bg-transparent p-0 text-ant-secondary text-xs hover:text-ant-text"
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
              {t("sync.candidateAddresses", {
                count: candidateAddresses.length,
              })}
            </span>
          </button>
          {candidateAddressesOpen
            ? candidateAddresses.map((address) => {
                return (
                  <div
                    className="break-all font-mono text-ant-secondary"
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
        <div className="text-ant-secondary">
          {t("sync.lastSeen", {
            time: new Date(peer.lastSeenAt).toLocaleString(),
          })}
        </div>
      ) : null}
      {peer.lastError ? (
        <div className="text-ant-error">{peer.lastError}</div>
      ) : null}
    </div>
  );
};

function channelStateClassName(state?: SyncStatus["lan"]["state"]) {
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

function splitAddressLines(value: string) {
  return value
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}
