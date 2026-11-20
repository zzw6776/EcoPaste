import { Empty, Spin, Tag } from "antd";
import type { FC } from "react";
import { useCallback, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Virtuoso } from "react-virtuoso";
import { type CloudRecord, listCloudRecords } from "@/commands";
import { toAssetUrl } from "@/components/AssetImage";
import Image from "@/components/Image";
import { formatBytes } from "@/pages/Preference/utils/storageUsage";
import { cn } from "@/utils/cn";
import { log } from "@/utils/log";

const CLOUD_RECORD_PAGE_SIZE = 30;

/** 管理云端剪贴板历史记录分页加载与代际竞争保护。 */
export function useCloudRecordsData() {
  const [records, setRecords] = useState<CloudRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [nextBeforeCursor, setNextBeforeCursor] = useState<number | null>(null);
  const loadGenerationRef = useRef(0);
  const loadingRef = useRef(false);
  const nextCursorRef = useRef<number | null>(null);
  nextCursorRef.current = nextBeforeCursor;

  const load = useCallback(async (reset: boolean) => {
    if (loadingRef.current && !reset) return;

    const generation = loadGenerationRef.current + 1;
    loadGenerationRef.current = generation;
    loadingRef.current = true;
    setLoading(true);
    try {
      const beforeCursor = reset ? void 0 : (nextCursorRef.current ?? void 0);
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
  }, []);

  const loadMore = useCallback(() => {
    if (nextCursorRef.current === null) return;

    void load(false);
  }, [load]);

  const reset = useCallback(() => {
    loadGenerationRef.current += 1;
    loadingRef.current = false;
    setRecords([]);
    setTotal(0);
    setLoading(false);
    setNextBeforeCursor(null);
  }, []);

  return {
    load,
    loading,
    loadMore,
    nextBeforeCursor,
    records,
    reset,
    total,
  };
}

interface CloudRecordsContentProps {
  loading: boolean;
  nextBeforeCursor: number | null;
  onEndReached: () => void;
  records: CloudRecord[];
}

/** 展示虚拟滚动分页的云端剪贴板记录列表。 */
export const CloudRecordsContent: FC<CloudRecordsContentProps> = (props) => {
  const { loading, nextBeforeCursor, onEndReached, records } = props;
  const { t } = useTranslation("clipboard");

  return (
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
          endReached={onEndReached}
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
    <div className="rounded-2.5 border border-ant-border-secondary bg-ant-container p-3">
      <div className="flex items-start gap-2">
        <div className="flex size-8 shrink-0 items-center justify-center rounded-2 bg-ant-fill-quaternary text-ant-secondary">
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
        <div className="mt-2 overflow-hidden rounded-2 bg-ant-fill-quaternary">
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
