import { Button } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import Drawer from "@/components/Drawer";
import Tooltip from "@/components/Tooltip";
import { isAndroid } from "@/utils/is";
import { CloudDevicesContent, useCloudDevicesData } from "./CloudDevicesPanel";

interface CloudDevicesDrawerProps {
  onClose: () => void;
  open: boolean;
}

/** 移动端 / 偏好页使用的云端设备抽屉容器。 */
const CloudDevicesDrawer: FC<CloudDevicesDrawerProps> = (props) => {
  const { onClose, open } = props;
  const { t } = useTranslation("clipboard");
  const { devices, hubStatus, load, loading, onlineCount, reset } =
    useCloudDevicesData();

  function handleOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      void load();
      return;
    }

    reset();
  }

  function handleRefresh() {
    void load();
  }

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
      onClose={onClose}
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
      <CloudDevicesContent
        devices={devices}
        hubStatus={hubStatus}
        loading={loading}
      />
    </Drawer>
  );
};

export default CloudDevicesDrawer;
