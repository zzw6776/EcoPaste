import { Button, Empty, Progress, Spin, Tag, Tooltip } from "antd";
import type { FC } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type CloudDeviceStatus,
  type CloudHubStatus,
  listCloudDevices,
} from "@/commands";
import Drawer from "@/components/Drawer";
import { cn } from "@/utils/cn";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";

interface CloudDevicesDrawerProps {
  onClose: () => void;
  open: boolean;
}

const CloudDevicesDrawer: FC<CloudDevicesDrawerProps> = (props) => {
  const { onClose, open } = props;
  const { t } = useTranslation("clipboard");
  const [devices, setDevices] = useState<CloudDeviceStatus[]>([]);
  const [hubStatus, setHubStatus] = useState<CloudHubStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const loadGenerationRef = useRef(0);

  async function load() {
    const generation = loadGenerationRef.current + 1;
    loadGenerationRef.current = generation;
    setLoading(true);
    try {
      const directory = await listCloudDevices();
      if (loadGenerationRef.current !== generation) return;

      setDevices(directory.devices);
      setHubStatus(directory.hubStatus);
    } catch (error) {
      log.error("load cloud devices failed", error);
    } finally {
      if (loadGenerationRef.current === generation) {
        setLoading(false);
      }
    }
  }

  function handleOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      void load();
      return;
    }

    loadGenerationRef.current += 1;
    setDevices([]);
    setHubStatus(null);
    setLoading(false);
  }

  function handleRefresh() {
    void load();
  }

  function handleClose() {
    onClose();
  }

  const onlineCount = devices.filter((device) => device.online).length;

  return (
    <Drawer
      afterOpenChange={handleOpenChange}
      destroyOnHidden
      extra={
        <Tooltip title={t("syncStatus.devices.refresh")}>
          <Button
            aria-label={t("syncStatus.devices.refresh")}
            icon={<i className="i-lucide:refresh-cw size-4" />}
            loading={loading}
            onClick={handleRefresh}
            type="text"
          />
        </Tooltip>
      }
      onClose={handleClose}
      open={open}
      placement="right"
      size={isAndroid ? "large" : "default"}
      title={
        <div className="flex items-center gap-2">
          <i className="i-lucide:monitor-smartphone size-4 text-ant-info" />
          <span>{t("syncStatus.devices.title")}</span>
          <span className="font-normal text-ant-secondary text-xs">
            {t("syncStatus.devices.summary", {
              online: onlineCount,
              total: devices.length,
            })}
          </span>
        </div>
      }
    >
      <Spin spinning={loading && devices.length === 0 && !hubStatus}>
        <div className="flex flex-col gap-3">
          {hubStatus ? <HubStatusCard status={hubStatus} /> : null}
          {devices.length ? (
            <div className="flex flex-col gap-2">
              {devices.map((device) => {
                return (
                  <CloudDeviceCard device={device} key={device.deviceId} />
                );
              })}
            </div>
          ) : loading ? (
            <div className="h-24" />
          ) : (
            <Empty description={t("syncStatus.devices.empty")} />
          )}
        </div>
      </Spin>
    </Drawer>
  );
};

interface HubStatusCardProps {
  status: CloudHubStatus;
}

const HubStatusCard: FC<HubStatusCardProps> = (props) => {
  const { status } = props;
  const { t } = useTranslation("clipboard");
  const percentage = status.maxBytes
    ? Math.min(100, Math.round((status.usedBytes / status.maxBytes) * 100))
    : 0;

  return (
    <div className="rounded-2 border border-ant-border-secondary bg-ant-container p-3">
      <div className="flex items-center justify-between gap-3 text-sm">
        <span className="font-medium">{t("syncStatus.devices.storage")}</span>
        <span className="text-ant-secondary text-xs">
          {status.maxBytes
            ? t("syncStatus.devices.storageUsage", {
                max: formatBytes(status.maxBytes),
                used: formatBytes(status.usedBytes),
              })
            : formatBytes(status.usedBytes)}
        </span>
      </div>
      {status.maxBytes ? (
        <Progress
          className="mt-1"
          percent={percentage}
          showInfo={false}
          size="small"
        />
      ) : null}
      <div className="mt-1 text-ant-secondary text-xs">
        {t("syncStatus.devices.retention", {
          days: status.retentionDays
            ? t("syncStatus.devices.days", { count: status.retentionDays })
            : t("syncStatus.devices.unlimited"),
          events: status.eventCount.toLocaleString(),
          maxEvents: status.maxEvents
            ? status.maxEvents.toLocaleString()
            : t("syncStatus.devices.unlimited"),
        })}
      </div>
      <div className="mt-1 text-ant-secondary text-xs">
        {status.oldestEventAt
          ? t("syncStatus.devices.oldestEvent", {
              time: new Date(status.oldestEventAt).toLocaleString(),
            })
          : t("syncStatus.devices.noRetainedEvents")}
      </div>
    </div>
  );
};

interface CloudDeviceCardProps {
  device: CloudDeviceStatus;
}

const CloudDeviceCard: FC<CloudDeviceCardProps> = (props) => {
  const { device } = props;
  const { t } = useTranslation("clipboard");

  return (
    <div className="rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-3">
      <div className="flex items-start gap-3">
        <div className="flex size-9 shrink-0 items-center justify-center rounded-2 bg-ant-container text-ant-secondary">
          <i className={cn("size-4", platformIcon(device.platform))} />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="truncate font-medium text-sm">
              {device.deviceName}
            </span>
            {device.current ? (
              <Tag className="m-0">{t("syncStatus.devices.current")}</Tag>
            ) : null}
            <Tag className="m-0" color={device.online ? "success" : "default"}>
              {t(
                device.online
                  ? "syncStatus.devices.online"
                  : "syncStatus.devices.offline",
              )}
            </Tag>
          </div>
          <div className="mt-1 text-ant-secondary text-xs">
            {formatPlatform(device.platform)} ·{" "}
            {device.appVersion
              ? t("syncStatus.devices.version", {
                  version: device.appVersion,
                })
              : t("syncStatus.devices.versionUnknown")}
          </div>
          <div className="mt-1 text-ant-secondary text-xs">
            {t("syncStatus.devices.lastSeen", {
              time: new Date(device.lastSeenAt).toLocaleString(),
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

function platformIcon(platform: string) {
  return platform.toLowerCase() === "android"
    ? "i-lucide:smartphone"
    : "i-lucide:monitor";
}

function formatPlatform(platform: string) {
  switch (platform.toLowerCase()) {
    case "macos":
      return "macOS";
    case "windows":
      return "Windows";
    case "android":
      return "Android";
    default:
      return platform;
  }
}

function formatBytes(bytes: number) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  const digits = value >= 10 || unitIndex === 0 ? 0 : 1;
  return `${value.toFixed(digits)} ${units[unitIndex]}`;
}

export default CloudDevicesDrawer;
