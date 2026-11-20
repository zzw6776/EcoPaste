import { Button } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import Drawer from "@/components/Drawer";
import Tooltip from "@/components/Tooltip";
import { isAndroid } from "@/utils/is";
import { CloudRecordsContent, useCloudRecordsData } from "./CloudRecordsPanel";

interface CloudRecordsDrawerProps {
  onClose: () => void;
  open: boolean;
}

/** 移动端 / 偏好页使用的云端剪贴板记录抽屉容器。 */
const CloudRecordsDrawer: FC<CloudRecordsDrawerProps> = (props) => {
  const { onClose, open } = props;
  const { t } = useTranslation("clipboard");
  const { load, loadMore, loading, nextBeforeCursor, records, reset, total } =
    useCloudRecordsData();

  function handleOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      reset();
      void load(true);
      return;
    }

    reset();
  }

  function handleRefresh() {
    void load(true);
  }

  return (
    <Drawer
      afterOpenChange={handleOpenChange}
      destroyOnHidden
      extra={
        <Tooltip title={t("syncStatus.records.refresh")}>
          <Button
            aria-label={t("syncStatus.records.refresh")}
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
      styles={{ body: { overflow: "hidden" } }}
      title={
        <div className="flex items-center gap-2">
          <i className="i-lucide:cloud size-4 text-ant-info" />
          <span>{t("syncStatus.records.title")}</span>
          <span className="font-normal text-ant-secondary text-xs">
            {t("syncStatus.records.total", { count: total })}
          </span>
        </div>
      }
    >
      <CloudRecordsContent
        loading={loading}
        nextBeforeCursor={nextBeforeCursor}
        onEndReached={loadMore}
        records={records}
      />
    </Drawer>
  );
};

export default CloudRecordsDrawer;
