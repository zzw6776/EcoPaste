import { listen } from "@tauri-apps/api/event";
import { useMount, useUnmount } from "ahooks";
import { Button, Input, Modal, Space, Typography } from "antd";
import { QRCodeSVG } from "qrcode.react";
import type { ChangeEvent, FC } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  createSyncGroup,
  exportSyncPairingCode,
  getSyncStatus,
  inspectSyncPairingCode,
  joinSyncGroup,
  leaveSyncGroup,
  reconnectSyncPeer,
  type SyncPeerStatus,
  type SyncStatus,
  setSyncDeviceName,
  syncNow,
} from "@/commands";
import { updateSettings } from "@/stores/settings";
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
  const [pairingCode, setPairingCode] = useState("");
  const [pairingCodeOpen, setPairingCodeOpen] = useState(false);
  const [joinOpen, setJoinOpen] = useState(false);
  const [scannerOpen, setScannerOpen] = useState(false);
  const [joinCode, setJoinCode] = useState("");
  const [deviceName, setDeviceName] = useState("");
  const deviceNameDirtyRef = useRef(false);
  const mountedRef = useRef(false);
  const qrScannerRef = useRef<SyncQrScannerHandle | null>(null);
  const unlistenRef = useRef<null | (() => void)>(null);

  async function refresh() {
    try {
      const next = await getSyncStatus();
      setStatus(next);
      if (!deviceNameDirtyRef.current) {
        setDeviceName(next.deviceName);
      }
    } catch (error) {
      log.error("load sync settings status failed", error);
    }
  }

  async function initialize() {
    try {
      await refresh();
      const unlisten = await listen("sync://updated", refresh);
      if (!mountedRef.current) {
        unlisten();
        return;
      }
      unlistenRef.current = unlisten;
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

  const busy = loading || reconnectingKey !== null;

  function handleOpenJoin() {
    setScannerOpen(isAndroid);
    setJoinOpen(true);
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
  }

  function handleJoinCodeChange(event: ChangeEvent<HTMLTextAreaElement>) {
    setJoinCode(event.target.value);
  }

  return (
    <div className="flex w-full flex-col gap-2">
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
            <Button
              aria-label={t("sync.actions.reconnectAll")}
              className="ml-auto"
              disabled={disabled || busy || !status?.peers.length}
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
                    peer={peer}
                    reconnecting={reconnectingKey === peer.deviceId}
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
          </div>
          {status?.cloudEndpointId ? (
            <div className="mt-2 break-all font-mono text-ant-secondary text-xs">
              {status.cloudEndpointId}
            </div>
          ) : null}
          {[
            ...(status?.cloudDirectAddresses ?? []),
            ...(status?.cloudRelayUrls ?? []),
          ].map((address) => {
            return (
              <div
                className="mt-1 break-all font-mono text-ant-secondary text-xs"
                key={address}
              >
                {address}
              </div>
            );
          })}
          {status?.cloud.lastSuccessAt ? (
            <div className="mt-2 text-ant-secondary text-xs">
              {t("sync.lastSuccess", {
                time: new Date(status.cloud.lastSuccessAt).toLocaleString(),
              })}
            </div>
          ) : null}
          {status?.cloud.lastError ? (
            <div className="mt-2 text-ant-error text-xs">
              {status.cloud.lastError}
            </div>
          ) : null}
        </div>
      </div>

      <Space.Compact className="w-full">
        <Input
          disabled={disabled || busy}
          onChange={handleDeviceNameChange}
          value={deviceName}
        />
        <Button disabled={disabled || busy} onClick={handleSaveName}>
          {t("sync.actions.saveName")}
        </Button>
      </Space.Compact>

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
        {scannerOpen ? (
          <div className="flex flex-col gap-3">
            <SyncQrScanner onDetected={handleScanDetected} ref={qrScannerRef} />
            <Button block onClick={handleUsePairingCode}>
              {t("sync.actions.usePairingCode")}
            </Button>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
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
          </div>
        )}
      </Modal>
    </div>
  );
};

export default SyncManagerControl;

interface SyncPeerSettingProps {
  disabled: boolean;
  onReconnect: (deviceId?: string) => Promise<void>;
  peer: SyncPeerStatus;
  reconnecting: boolean;
}

const SyncPeerSetting: FC<SyncPeerSettingProps> = (props) => {
  const { disabled, onReconnect, peer, reconnecting } = props;
  const { t } = useTranslation("preferences");
  const addresses = peer.connectedAddress
    ? [peer.connectedAddress]
    : peer.directAddresses;
  const reconnectLabel = t("sync.actions.reconnectDevice", {
    device: peer.deviceName,
  });

  function handleReconnect() {
    void onReconnect(peer.deviceId);
  }

  return (
    <div className="text-xs">
      <div className="flex items-center gap-1 font-medium">
        <span className="min-w-0 truncate">
          {peer.deviceName} · {t(`sync.states.${peer.state}`)}
        </span>
        <Button
          aria-label={reconnectLabel}
          className="ml-auto shrink-0"
          disabled={disabled || peer.state === "connecting"}
          icon={<i className="i-lucide:refresh-cw size-3.5" />}
          loading={reconnecting}
          onClick={handleReconnect}
          size="small"
          title={reconnectLabel}
          type="text"
        />
      </div>
      <div className="text-ant-secondary">
        {peer.platform}
        {peer.transport ? ` · ${t(`sync.transport.${peer.transport}`)}` : ""}
      </div>
      {addresses.map((address) => {
        return (
          <div className="break-all font-mono text-ant-secondary" key={address}>
            {address}
          </div>
        );
      })}
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
    case "error":
      return "text-ant-error";
    default:
      return "text-ant-tertiary";
  }
}
