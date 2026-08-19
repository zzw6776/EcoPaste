import { Empty } from "antd";
import type { FC } from "react";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Virtuoso } from "react-virtuoso";
import {
  type ClipboardPreviewFileEntry,
  type ClipboardPreviewPayload,
  closeClipboardPreview,
  writeToClipboard,
} from "@/commands";
import AssetImage from "@/components/AssetImage";
import VirtuosoScroller, {
  type VirtuosoScrollerChildrenProps,
} from "@/components/VirtuosoScroller";
import { getAppTheme, parseUrlInfo } from "@/utils/appIcons";
import { cn } from "@/utils/cn";
import { PREVIEW_TEXT_SOFT_WRAP_CHARS } from "../constants";

export interface PreviewContentProps {
  payload: ClipboardPreviewPayload | null;
}

export interface PreviewHeaderProps {
  payload: ClipboardPreviewPayload | null;
}

interface PayloadViewerProps {
  payload: ClipboardPreviewPayload;
}

interface FilePreviewRowProps {
  file: ClipboardPreviewFileEntry;
}

const TEXT_VIRTUOSO_COMPONENTS = {
  Footer: PreviewTextPadding,
  Header: PreviewTextPadding,
};

const FILES_VIRTUOSO_COMPONENTS = {
  Footer: PreviewFilesPadding,
  Header: PreviewFilesPadding,
};

/**
 * 1:1 Paste 官方 Quick Look 顶部栏：
 * 左侧 `(x)` 关闭 + 类型标题，右侧来源 App 真实图标 + 复制/编辑操作。
 */
export const PreviewHeader: FC<PreviewHeaderProps> = (props) => {
  const { payload } = props;
  const { t } = useTranslation(["preview", "clipboard"]);

  if (!payload) {
    return (
      <div className="flex h-10 shrink-0 items-center bg-transparent px-4 font-medium text-neutral-400 text-xs">
        {t("title.loading")}
      </div>
    );
  }

  const theme = getAppTheme(
    payload.sourceAppName ?? void 0,
    void 0,
    payload.kind,
    payload.subKind,
    payload.text,
  );

  const handleClose = () => {
    void closeClipboardPreview();
  };

  const handleCopy = () => {
    if (payload.id) {
      void writeToClipboard(payload.id, false);
    }
  };

  return (
    <div className="relative flex h-10 shrink-0 select-none items-center justify-between bg-transparent px-3.5">
      {/* 左侧：(x) 关闭按钮 + 标题 */}
      <div className="flex items-center gap-2">
        <button
          className="flex size-[20px] cursor-pointer items-center justify-center rounded-full bg-black/[0.08] text-neutral-600 transition-colors hover:bg-black/15 dark:bg-white/10 dark:text-neutral-300 dark:hover:bg-white/20"
          onClick={handleClose}
          title="关闭预览 (Esc)"
          type="button"
        >
          <i className="i-lucide:x size-2.5 stroke-[2.5]" />
        </button>
        <span className="font-bold text-[13.5px] text-neutral-800 dark:text-neutral-100">
          {theme.tag}
        </span>
      </div>

      {/* 右侧：来源 App 真实图标 + 复制操作 */}
      <div className="flex items-center gap-2">
        {/* 真实 App 来源图标（macOS 原生圆角，完全对齐下方卡片） */}
        {(payload.sourceAppIconPath || theme.iconUrl) && (
          <div className="flex size-[20px] shrink-0 items-center justify-center overflow-hidden rounded-[4.5px]">
            <AssetImage
              alt={payload.sourceAppName ?? "app"}
              className="pointer-events-none size-full object-contain"
              fallbackSrc={theme.iconUrl}
              src={payload.sourceAppIconPath || theme.iconUrl}
            />
          </div>
        )}

        {/* 复制操作 */}
        <button
          className="flex cursor-pointer items-center gap-1 rounded-[6px] bg-black/[0.05] px-2.5 py-0.5 font-medium text-neutral-700 text-xs transition-colors hover:bg-black/10 dark:bg-white/10 dark:text-neutral-200 dark:hover:bg-white/15"
          onClick={handleCopy}
          type="button"
        >
          <i className="i-lucide:copy size-3" />
          <span>复制</span>
        </button>
      </div>
    </div>
  );
};

/**
 * 底部元信息栏（字符数、单词数、行数 / 尺寸等，1:1 对齐 Paste）
 */
export const PreviewFooter: FC<PreviewContentProps> = (props) => {
  const { payload } = props;
  if (!payload) return null;

  let metaText = "";
  if (payload.kind === "text") {
    const text = payload.text ?? "";
    const chars = text.length;
    const lines = text.split(/\r\n|\r|\n/).length;

    // 检查是否包含中文 / CJK 字符
    const isCjk = /[\u4e00-\u9fa5\u3040-\u30ff\u3400-\u4dbf]/.test(text);
    const latinWords = text.match(/[a-zA-Z0-9]+(?:'[a-zA-Z0-9]+)?/g) ?? [];
    const wordCount = latinWords.length;

    const parts: string[] = [`${chars} 个字符`];

    // 纯英文文本且包含多个单词时展示单词数
    if (!isCjk && wordCount > 1) {
      parts.push(`${wordCount} 个单词`);
    }

    // 超过 1 行时才展示行数（单行不冗余展示“1 行”）
    if (lines > 1) {
      parts.push(`${lines} 行`);
    }

    metaText = parts.join(" · ");
  } else if (payload.kind === "image") {
    const w = payload.imageWidth ?? 0;
    const h = payload.imageHeight ?? 0;
    metaText = w && h ? `${w} × ${h} 像素` : "图片";
  } else if (payload.kind === "files") {
    metaText = `${payload.totalFiles} 个项目`;
  }

  return (
    <div className="flex h-7 shrink-0 select-none items-center justify-between bg-transparent px-4 font-medium text-[11.5px] text-neutral-400 dark:text-neutral-500">
      <span>{metaText}</span>
    </div>
  );
};

/**
 * 按 payload kind 分发到基础 viewer。
 */
export const PreviewContent: FC<PreviewContentProps> = (props) => {
  const { payload } = props;
  const { t } = useTranslation("preview");

  if (!payload) {
    return (
      <div className="flex min-h-24 items-center justify-center">
        <Empty
          description={t("empty.content")}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  if (payload.subKind === "url") {
    return <UrlViewer payload={payload} />;
  }

  if (payload.kind === "image") return <ImageViewer payload={payload} />;

  if (payload.kind === "files") return <FilesViewer payload={payload} />;

  return <TextViewer payload={payload} />;
};

/**
 * 链接大预览
 */
const UrlViewer: FC<PayloadViewerProps> = (props) => {
  const { payload } = props;
  const urlInfo = parseUrlInfo(payload.text);

  return (
    <div className="flex min-h-48 flex-col items-center justify-center gap-3 bg-white p-6 text-center dark:bg-[#1E1E1E]">
      <img
        alt="logo"
        className="size-16 object-contain"
        src={urlInfo?.iconUrl}
      />
      <div className="font-bold text-base text-neutral-800 dark:text-neutral-100">
        {urlInfo?.name}
      </div>
      <div className="select-text break-all px-4 font-mono text-blue-600 text-xs dark:text-blue-400">
        {payload.text}
      </div>
    </div>
  );
};

/**
 * 文本预览：纯文本虚拟行展示，舒适行高。
 */
const TextViewer: FC<PayloadViewerProps> = (props) => {
  const { payload } = props;
  const { t } = useTranslation("preview");
  const text = payload.text ?? "";
  const rows = useMemo(() => {
    return buildTextPreviewRows(text);
  }, [text]);

  if (text.length === 0) {
    return (
      <div className="flex min-h-24 items-center justify-center">
        <Empty
          description={t("empty.text")}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  return (
    <div className="h-full w-full bg-transparent px-1 pt-1">
      <VirtuosoScroller>{renderTextVirtuoso}</VirtuosoScroller>
    </div>
  );

  function renderTextVirtuoso(props: VirtuosoScrollerChildrenProps) {
    const { scrollerRef } = props;

    return (
      <Virtuoso
        components={TEXT_VIRTUOSO_COMPONENTS}
        computeItemKey={computeTextRowKey}
        itemContent={renderTextRow}
        scrollerRef={scrollerRef}
        totalCount={rows.length}
      />
    );
  }

  function computeTextRowKey(index: number) {
    return index;
  }

  function renderTextRow(index: number) {
    const row = rows[index] ?? "";

    return (
      <div className="min-h-[28px] select-text whitespace-pre px-4.5 font-normal font-sans text-[16.5px] text-neutral-900 leading-[28px] tracking-tight dark:text-neutral-100">
        {row.length === 0 ? " " : row}
      </div>
    );
  }
};

/**
 * 图片预览：使用原图路径渲染。
 */
const ImageViewer: FC<PayloadViewerProps> = (props) => {
  const { payload } = props;
  const { t } = useTranslation("preview");
  const imageWidth = payload.imageWidth ?? void 0;
  const imageHeight = payload.imageHeight ?? void 0;

  if (!payload.imagePath || !payload.imageExists) {
    return (
      <div className="flex min-h-24 items-center justify-center">
        <Empty
          description={t("empty.imageMissing")}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 items-center justify-center bg-[radial-gradient(#e2e8f0_1px,transparent_1px)] bg-[size:10px_10px] bg-white p-4 dark:bg-[#1E1E1E]">
      <AssetImage
        alt={t("image.alt")}
        className="h-auto max-h-[360px] max-w-full rounded-lg object-contain shadow-md"
        draggable={false}
        height={imageHeight}
        src={payload.imagePath}
        width={imageWidth}
      />
    </div>
  );
};

/**
 * 文件预览：虚拟列表展示。
 */
const FilesViewer: FC<PayloadViewerProps> = (props) => {
  const { payload } = props;
  const { t } = useTranslation("preview");

  if (payload.files.length === 0) {
    return (
      <div className="flex min-h-24 items-center justify-center">
        <Empty
          description={t("empty.files")}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  return (
    <div className="h-full w-full bg-white dark:bg-[#1E1E1E]">
      <VirtuosoScroller>{renderFilesVirtuoso}</VirtuosoScroller>
    </div>
  );

  function renderFilesVirtuoso(props: VirtuosoScrollerChildrenProps) {
    const { scrollerRef } = props;
    const components =
      payload.totalFiles > payload.files.length
        ? {
            Footer: renderFilesFooter,
            Header: PreviewFilesPadding,
          }
        : FILES_VIRTUOSO_COMPONENTS;

    return (
      <Virtuoso
        components={components}
        computeItemKey={computeFileRowKey}
        itemContent={renderFileRow}
        scrollerRef={scrollerRef}
        totalCount={payload.files.length}
      />
    );
  }

  function computeFileRowKey(index: number) {
    const file = payload.files[index];

    return file ? `${file.path}:${index}` : index;
  }

  function renderFileRow(index: number) {
    const file = payload.files[index];
    if (!file) return null;

    return <FilePreviewRow file={file} />;
  }

  function renderFilesFooter() {
    const remaining = payload.totalFiles - payload.files.length;

    return (
      <div className="px-4 py-2 text-center text-neutral-400 text-xs">
        {t("meta.filesRemaining", { count: remaining })}
      </div>
    );
  }
};

function PreviewTextPadding() {
  return <div className="h-3" />;
}

function PreviewFilesPadding() {
  return <div className="h-2" />;
}

const FilePreviewRow: FC<FilePreviewRowProps> = (props) => {
  const { file } = props;
  const { t } = useTranslation("preview");
  const kindLabel = file.isDir ? t("file.folder") : t("file.item");
  const sizeLabel = file.size === null ? kindLabel : formatBytes(file.size);

  return (
    <div
      className={cn(
        "flex min-h-10 items-center gap-2 rounded-lg px-3 py-1.5 transition-colors hover:bg-neutral-50 dark:hover:bg-neutral-800",
        { "opacity-50": !file.exists },
      )}
      title={file.path}
    >
      {file.iconPath ? (
        <AssetImage className="size-6 shrink-0" src={file.iconPath} />
      ) : (
        <i
          aria-hidden
          className="i-lucide:file size-5 shrink-0 text-neutral-400"
        />
      )}

      <div className="min-w-0 flex-1">
        <div
          className={cn(
            "truncate font-medium text-neutral-800 text-xs dark:text-neutral-200",
            {
              "line-through": !file.exists,
            },
          )}
        >
          {file.name}
        </div>
        <div className="truncate text-[11px] text-neutral-400">
          {file.exists ? file.path : t("file.missingPath")}
        </div>
      </div>

      <span className="shrink-0 font-mono text-neutral-400 text-xs">
        {sizeLabel}
      </span>
    </div>
  );
};

function buildTextPreviewRows(text: string) {
  const rows: string[] = [];

  for (const line of text.split("\n")) {
    if (line.length === 0) {
      rows.push("");
      continue;
    }

    for (
      let start = 0;
      start < line.length;
      start += PREVIEW_TEXT_SOFT_WRAP_CHARS
    ) {
      rows.push(line.slice(start, start + PREVIEW_TEXT_SOFT_WRAP_CHARS));
    }
  }

  return rows;
}

function formatBytes(value: number) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }

  const fractionDigits = unitIndex === 0 || size >= 10 ? 0 : 1;

  return `${size.toFixed(fractionDigits)} ${units[unitIndex]}`;
}
