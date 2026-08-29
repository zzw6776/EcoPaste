import { Button, Drawer, Empty, Image, Spin, Tag, Tooltip } from "antd";
import type { FC } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Virtuoso } from "react-virtuoso";
import { type CloudRecord, listCloudRecords } from "@/commands";
import { toAssetUrl } from "@/components/AssetImage";
import { cn } from "@/utils/cn";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";

interface CloudRecordsDrawerProps {
  onClose: () => void;
  open: boolean;
}

const CLOUD_RECORD_PAGE_SIZE = 30;

const CloudRecordsDrawer: FC<CloudRecordsDrawerProps> = (props) => {
  const { onClose, open } = props;
  const { t } = useTranslation("clipboard");
  const [records, setRecords] = useState<CloudRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [nextBeforeCursor, setNextBeforeCursor] = useState<number | null>(null);
  const loadGenerationRef = useRef(0);
  const loadingRef = useRef(false);
  const navigationEntryRef = useRef(false);
  const onCloseRef = useRef(onClose);

  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useEffect(() => {
    if (!isAndroid || !open || navigationEntryRef.current) return;

    window.history.pushState(
      { ...window.history.state, ecopasteLayer: "cloud-records" },
      "",
    );
    navigationEntryRef.current = true;
    const handlePopState = () => {
      if (!navigationEntryRef.current) return;

      navigationEntryRef.current = false;
      onCloseRef.current();
    };
    window.addEventListener("popstate", handlePopState);

    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, [open]);

  async function load(reset: boolean) {
    if (loadingRef.current && !reset) return;

    const generation = loadGenerationRef.current + 1;
    loadGenerationRef.current = generation;
    loadingRef.current = true;
    setLoading(true);
    try {
      const beforeCursor = reset ? void 0 : (nextBeforeCursor ?? void 0);
      const page = await listCloudRecords(beforeCursor, CLOUD_RECORD_PAGE_SIZE);
      if (loadGenerationRef.current !== generation) return;

      setRecords((current) => {
        return reset ? page.records : [...current, ...page.records];
      });
      setTotal(page.total);
      setNextBeforeCursor(page.nextBeforeCursor);
    } catch (error) {
      log.error("load cloud records failed", error);
    } finally {
      if (loadGenerationRef.current === generation) {
        loadingRef.current = false;
        setLoading(false);
      }
    }
  }

  function handleOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      setRecords([]);
      setNextBeforeCursor(null);
      void load(true);
    } else {
      loadGenerationRef.current += 1;
      loadingRef.current = false;
    }
  }

  function handleRefresh() {
    void load(true);
  }

  function handleEndReached() {
    if (nextBeforeCursor !== null) {
      void load(false);
    }
  }

  function handleClose() {
    if (isAndroid && navigationEntryRef.current) {
      navigationEntryRef.current = false;
      window.history.back();
    }
    onClose();
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
      onClose={handleClose}
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
      <Spin
        className="h-full"
        classNames={{ container: "h-full" }}
        spinning={loading && records.length === 0}
      >
        {records.length ? (
          <Virtuoso
            className="h-full"
            components={{
              Footer: () => {
                if (loading) {
                  return <div className="h-12" />;
                }
                if (nextBeforeCursor !== null) return null;

                return (
                  <div className="py-2 text-center text-ant-tertiary text-xs">
                    {t("syncStatus.records.end")}
                  </div>
                );
              },
            }}
            data={records}
            endReached={handleEndReached}
            itemContent={(_index, record) => {
              return (
                <div className="pb-2">
                  <CloudRecordCard record={record} />
                </div>
              );
            }}
          />
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
  const preview =
    record.preview || t(`types.${record.kind}`, { defaultValue: record.kind });

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
      {record.imagePath ? (
        <div className="mt-2 overflow-hidden rounded-2 bg-ant-container">
          <Image
            alt={preview}
            className="max-h-72 w-full object-contain"
            preview
            src={toAssetUrl(record.imagePath)}
          />
        </div>
      ) : null}
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
