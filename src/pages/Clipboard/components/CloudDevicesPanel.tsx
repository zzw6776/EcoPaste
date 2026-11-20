import { Empty, Spin, Tag } from "antd";
import type { FC } from "react";
import { useCallback, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type CloudDeviceStatus,
  type CloudHubStatus,
  listCloudDevices,
} from "@/commands";
import { cn } from "@/utils/cn";
import { log } from "@/utils/log";
import CloudQuotaCard from "./CloudQuotaCard";
import {
  formatPlatformLabel,
  platformIconClass,
} from "./SyncStatusIcons.logic";

/** 管理云端设备与 Hub 配额数据的拉取与代际竞争保护。 */
export function useCloudDevicesData() {
  const [devices, setDevices] = useState<CloudDeviceStatus[]>([]);
  const [hubStatus, setHubStatus] = useState<CloudHubStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const loadGenerationRef = useRef(0);

  const load = useCallback(async () => {
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
  }, []);

  const reset = useCallback(() => {
    loadGenerationRef.current += 1;
    setDevices([]);
    setHubStatus(null);
    setLoading(false);
  }, []);

  const onlineCount = devices.filter((device) => {
    return device.online;
  }).length;

  return {
    devices,
    hubStatus,
    load,
    loading,
    onlineCount,
    reset,
  };
}

interface CloudDevicesContentProps {
  devices: CloudDeviceStatus[];
  hubStatus: CloudHubStatus | null;
  loading: boolean;
}

/** 展示云端 Hub 配额及全部已注册云端设备列表。 */
export const CloudDevicesContent: FC<CloudDevicesContentProps> = (props) => {
  const { devices, hubStatus, loading } = props;
  const { t } = useTranslation("clipboard");

  return (
    <Spin spinning={loading && devices.length === 0 && !hubStatus}>
      <div className="flex flex-col gap-3">
        {hubStatus ? <CloudQuotaCard status={hubStatus} /> : null}
        {devices.length ? (
          <div className="flex flex-col gap-2">
            {devices.map((device) => {
              return <CloudDeviceCard device={device} key={device.deviceId} />;
            })}
          </div>
        ) : loading ? (
          <div className="h-24" />
        ) : (
          <Empty description={t("syncStatus.devices.empty")} />
        )}
      </div>
    </Spin>
  );
};

interface CloudDeviceCardProps {
  device: CloudDeviceStatus;
}

const CloudDeviceCard: FC<CloudDeviceCardProps> = (props) => {
  const { device } = props;
  const { t } = useTranslation("clipboard");

  return (
    <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-3">
      <div className="flex items-start gap-3">
        <div className="flex size-9 shrink-0 items-center justify-center rounded-2 bg-ant-fill-quaternary text-ant-secondary">
          <i className={cn("size-4", platformIconClass(device.platform))} />
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
            {formatPlatformLabel(device.platform)} ·{" "}
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
