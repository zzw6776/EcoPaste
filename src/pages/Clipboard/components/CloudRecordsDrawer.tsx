import { Button, Drawer, Empty, Spin, Tag, Tooltip } from "antd";
import type { FC } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type CloudRecord,
  type CloudRecordPage,
  listCloudRecords,
} from "@/commands";
import { cn } from "@/utils/cn";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";

interface CloudRecordsDrawerProps {
  onClose: () => void;
  open: boolean;
}

const CloudRecordsDrawer: FC<CloudRecordsDrawerProps> = (props) => {
  const { onClose, open } = props;
  const { t } = useTranslation("clipboard");
  const [records, setRecords] = useState<CloudRecord[]>([]);
  const [nextCursor, setNextCursor] = useState<number | null>(null);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);

  async function load(beforeCursor?: number) {
    setLoading(true);
    try {
      const page = await listCloudRecords(beforeCursor);
      applyPage(page, beforeCursor !== void 0);
    } catch (error) {
      log.error("load cloud records failed", error);
    } finally {
      setLoading(false);
    }
  }

  function applyPage(page: CloudRecordPage, append: boolean) {
    setRecords((current) => {
      return append ? [...current, ...page.records] : page.records;
    });
    setNextCursor(page.nextBeforeCursor);
    setTotal(page.total);
  }

  function handleOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      void load();
    }
  }

  function handleRefresh() {
    void load();
  }

  function handleLoadMore() {
    if (nextCursor !== null) {
      void load(nextCursor);
    }
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
      <Spin spinning={loading && records.length === 0}>
        {records.length ? (
          <div className="flex flex-col gap-2">
            {records.map((record) => {
              return <CloudRecordCard key={record.eventId} record={record} />;
            })}
            {nextCursor !== null ? (
              <Button block loading={loading} onClick={handleLoadMore}>
                {t("syncStatus.records.loadMore")}
              </Button>
            ) : (
              <div className="py-2 text-center text-ant-tertiary text-xs">
                {t("syncStatus.records.end")}
              </div>
            )}
          </div>
        ) : loading ? (
          <div className="h-24" />
        ) : (
          <Empty description={t("syncStatus.records.empty")} />
        )}
      </Spin>
    </Drawer>
  );
};

interface CloudRecordCardProps {
  record: CloudRecord;
}

const CloudRecordCard: FC<CloudRecordCardProps> = (props) => {
  const { record } = props;
  const { t } = useTranslation("clipboard");
  const sizeLabel = record.totalSize > 0 ? formatBytes(record.totalSize) : null;
  const preview = record.isSensitive
    ? t("syncStatus.records.sensitivePreview")
    : record.preview ||
      t(`types.${record.kind}`, { defaultValue: record.kind });

  return (
    <div className="rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-3">
      <div className="flex items-start gap-2">
        <div className="flex size-8 shrink-0 items-center justify-center rounded-2 bg-ant-container text-ant-secondary">
          <i className={cn("size-4", recordIcon(record.kind))} />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-1.5">
            <span className="truncate font-medium text-sm">
              {record.deviceName}
            </span>
            {record.isSensitive ? (
              <Tag className="m-0" color="warning">
                {t("syncStatus.records.sensitive")}
              </Tag>
            ) : null}
          </div>
          <div className="mt-0.5 text-ant-secondary text-xs">
            {new Date(record.createdAt).toLocaleString()}
            {record.fileCount > 0
              ? ` · ${t("syncStatus.records.files", { count: record.fileCount })}`
              : ""}
            {sizeLabel ? ` · ${sizeLabel}` : ""}
          </div>
        </div>
      </div>
      <div className="mt-2 whitespace-pre-wrap break-words text-sm leading-relaxed">
        {preview}
      </div>
    </div>
  );
};

function recordIcon(kind: string) {
  switch (kind) {
    case "image":
      return "i-lucide:image";
    case "files":
      return "i-lucide:files";
    default:
      return "i-lucide:clipboard-type";
  }
}

function formatBytes(bytes: number) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  const digits = unit === 0 || size >= 10 ? 0 : 1;
  return `${size.toFixed(digits)} ${units[unit]}`;
}

export default CloudRecordsDrawer;
