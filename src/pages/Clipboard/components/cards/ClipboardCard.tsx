import type { DragEvent, FC, MouseEvent, PointerEvent, Ref } from "react";
import { useState } from "react";
import { popupClipboardItemMenu, startDragClipboardItem } from "@/commands";
import AssetImage from "@/components/AssetImage";
import type { ItemActionLabels } from "@/constants/itemActions";
import type { ClipboardAction, ClipboardItem } from "@/types/clipboard";
import type { ItemAction } from "@/types/settings";
import {
  formatRelativeTime,
  getAppTheme,
  parseUrlInfo,
} from "@/utils/appIcons";
import { cn } from "@/utils/cn";
import { useDynamicHeaderBg } from "@/utils/dominantColor";
import { isMobile } from "@/utils/is";
import ClipboardQuickActions from "./ClipboardQuickActions";
import FilesCard from "./FilesCard";
import ImageCard from "./ImageCard";
import NoteContentSwitcher from "./NoteContentSwitcher";
import TextCard from "./TextCard";

interface ClipboardCardProps {
  item: ClipboardItem;
  isSelected?: boolean;
  hintKey?: string;
  onQuickPaste?: () => void;
  isLinkActive?: boolean;
  onOpenLink?: () => void;
  onPointerEnter?: (event: PointerEvent<HTMLDivElement>) => void;
  onPointerLeave?: () => void;
  onPointerMove?: (event: PointerEvent<HTMLDivElement>) => void;
  onMouseDown?: (event: MouseEvent<HTMLDivElement>) => void;
  onAuxClick?: (event: MouseEvent<HTMLDivElement>) => void;
  onDoubleClick?: (event: MouseEvent<HTMLDivElement>) => void;
  availableActions?: ClipboardAction[];
  quickActions?: ItemAction[];
  quickActionLabels?: ItemActionLabels;
  onQuickAction?: (action: ItemAction) => Promise<void> | void;
  showOriginalOnHover?: boolean;
  rootRef?: Ref<HTMLDivElement>;
}

/**
 * 1:1 像素级复刻官方 Paste 大卡片：
 * 1. 1:1 正方形大卡片 (aspect-square)；
 * 2. 42px 精致彩色顶栏 (h-[42px])；
 * 3. 34px 跨界圆形 App 图标 (半截在顶栏内，半截在白底上)；
 * 4. 2.5px 纯正苹果天蓝独立顶层聚焦框 (z-30)；
 * 5. 极淡 8px 灰白方块微弱棋盘格背景 (图片卡片)；
 * 6. 纯白舒展多行文字排版 (文本卡片)；
 * 7. 底部极简字符/尺寸与 ≡ 序号。
 */
const ClipboardCard: FC<ClipboardCardProps> = (props) => {
  const {
    item,
    isSelected,
    hintKey,
    isLinkActive,
    onOpenLink,
    onPointerEnter,
    onPointerLeave,
    onPointerMove,
    onMouseDown,
    onAuxClick,
    onDoubleClick,
    availableActions,
    quickActions = [],
    quickActionLabels,
    onQuickAction,
    showOriginalOnHover = true,
    rootRef,
  } = props;
  const {
    kind,
    subKind,
    sourceAppIconPath,
    sourceAppName,
    sourceAppId,
    createdAt,
    displayCreatedAt,
    width,
    height,
    size,
    summary,
  } = item;
  const [hovered, setHovered] = useState(false);

  const theme = getAppTheme(
    sourceAppName,
    sourceAppId,
    kind,
    subKind,
    item.summary,
  );
  const timeText = displayCreatedAt || formatRelativeTime(createdAt);
  const headerBg = useDynamicHeaderBg(sourceAppIconPath || theme.iconUrl);

  const handleDragStart = async (event: DragEvent) => {
    event.preventDefault();
    await startDragClipboardItem(item.id);
  };

  const handleContextMenu = async (event: MouseEvent) => {
    event.preventDefault();
    const actions = availableActions ?? item.availableActions ?? [];
    const { isFavorite, isPinned, note } = item;
    if (actions.length === 0) return;

    await popupClipboardItemMenu(
      item.id,
      [...actions],
      item.groupId,
      isFavorite,
      isPinned,
      Boolean(note),
    );
  };

  const handlePointerEnter = (event: PointerEvent<HTMLDivElement>) => {
    setHovered(true);
    onPointerEnter?.(event);
  };

  const handlePointerLeave = () => {
    setHovered(false);
    onPointerLeave?.();
  };

  const handlePointerMove = (event: PointerEvent<HTMLDivElement>) => {
    onPointerMove?.(event);
  };

  const handleMobileActionMouseDown = (
    event: MouseEvent<HTMLButtonElement>,
  ) => {
    event.stopPropagation();
  };

  const handleMobileFavorite = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    void onQuickAction?.("star");
  };

  const handleMobileCopy = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    void onQuickAction?.("copy");
  };

  const handleMobileDelete = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    void onQuickAction?.("delete");
  };

  const isImageCard = kind === "image";
  const isUrlCard = subKind === "url";
  const urlInfo = isUrlCard ? parseUrlInfo(summary || item.content) : null;

  // 底部元信息
  const metaText = isImageCard
    ? width && height
      ? `${width} × ${height}`
      : "图片"
    : isUrlCard
      ? urlInfo?.name || "网页"
      : `${summary?.length ?? size ?? 0} 个字符`;

  const isMob = isMobile();

  return (
    <div
      aria-selected={isSelected}
      className={cn(
        "relative flex w-full min-w-0 cursor-pointer select-none flex-col overflow-hidden rounded-[16px] border-0 bg-white transition-all duration-150 dark:bg-neutral-800",
        isMob
          ? "mb-1 min-h-[105px] shadow-[0_2px_8px_rgba(0,0,0,0.04)] active:scale-[0.985]"
          : "h-full",
        isSelected && !isMob ? "z-10" : "hover:shadow-md",
      )}
      draggable
      onAuxClick={onAuxClick}
      onContextMenu={handleContextMenu}
      onDoubleClick={onDoubleClick}
      onDragStart={handleDragStart}
      onMouseDown={onMouseDown}
      onPointerEnter={handlePointerEnter}
      onPointerLeave={handlePointerLeave}
      onPointerMove={handlePointerMove}
      ref={rootRef}
      role="option"
      style={
        isSelected && !isMob
          ? {
              boxShadow:
                "0 0 0 2.5px #007AFF, 0 4px 16px rgba(0, 122, 255, 0.25)",
            }
          : void 0
      }
      tabIndex={0}
    >
      {/* 1. 顶部色块横幅 */}
      <div
        className={cn(
          "relative flex w-full shrink-0 select-none items-center justify-between overflow-hidden pr-2.5 pl-3.5 text-white",
          isMob ? "h-[36px]" : "h-[46px]",
        )}
        style={{ background: headerBg }}
      >
        {/* 左侧：类型标签 + 时间 / 置顶 / 收藏 */}
        <div className="flex min-w-0 flex-col justify-center pr-2">
          <div className="flex items-center gap-1">
            <span className="font-bold text-[12px] text-white leading-none tracking-tight drop-shadow-2xs">
              {theme.tag}
            </span>
            {item.isPinned && (
              <i
                className="i-lucide:pin size-3 rotate-45 text-white/90"
                title="已置顶"
              />
            )}
            {item.isFavorite && (
              <i
                className="i-lucide:star size-3 fill-amber-300 text-amber-300"
                title="已收藏"
              />
            )}
          </div>
          <span className="mt-1 truncate font-medium text-[10px] text-white/85 leading-none">
            {timeText}
          </span>
        </div>

        {/* 右侧：App 图标 */}
        <div
          className={cn(
            "flex shrink-0 items-center justify-center",
            isMob ? "size-[28px]" : "size-[38px]",
          )}
        >
          <AssetImage
            alt={sourceAppName ?? "app"}
            className="pointer-events-none size-full object-contain"
            fallbackSrc={theme.iconUrl}
            src={sourceAppIconPath || theme.iconUrl}
          />
        </div>
      </div>

      {/* 2. 下半段内容区 */}
      <div
        className="relative flex min-w-0 flex-1 flex-col justify-between overflow-hidden bg-white dark:bg-neutral-800"
        style={
          isImageCard
            ? {
                backgroundImage:
                  "conic-gradient(#f4f4f5 90deg, #ffffff 90deg 180deg, #f4f4f5 180deg 270deg, #ffffff 270deg)",
                backgroundSize: "16px 16px",
              }
            : void 0
        }
      >
        {/* 内容主体 */}
        <div
          className={cn(
            "relative min-w-0 flex-1 overflow-hidden",
            isMob ? "min-h-[44px] p-2.5" : "p-2.5",
          )}
        >
          {isUrlCard && urlInfo ? (
            <div className="flex size-full flex-col justify-between">
              <div className="flex flex-1 items-center justify-center">
                <img
                  alt={urlInfo.name}
                  className="size-10 object-contain"
                  src={urlInfo.iconUrl}
                />
              </div>
              <div className="flex flex-col gap-0.5">
                <span className="line-clamp-1 font-bold text-[11.5px] text-neutral-800 dark:text-neutral-100">
                  {urlInfo.name}
                </span>
                <span className="line-clamp-1 font-mono text-[9.5px] text-neutral-400">
                  {urlInfo.host}
                </span>
              </div>
            </div>
          ) : item.note ? (
            <NoteContentSwitcher
              note={item.note}
              showOriginal={hovered && showOriginalOnHover}
            >
              {kind === "text" && (
                <TextCard
                  {...item}
                  isLinkActive={isLinkActive}
                  onOpenLink={onOpenLink}
                />
              )}
              {kind === "image" && <ImageCard {...item} />}
              {kind === "files" && <FilesCard {...item} />}
            </NoteContentSwitcher>
          ) : (
            <>
              {kind === "text" && (
                <TextCard
                  {...item}
                  isLinkActive={isLinkActive}
                  onOpenLink={onOpenLink}
                />
              )}
              {kind === "image" && <ImageCard {...item} />}
              {kind === "files" && <FilesCard {...item} />}
            </>
          )}
        </div>

        {/* 3. 底部极简元信息与快捷操作 */}
        <div className="flex h-7 shrink-0 select-none items-center justify-between px-3 pt-0 pb-1 font-medium text-[11px] text-neutral-400">
          <span className="truncate">{metaText}</span>

          {/* 右侧：移动端常驻操作按钮，桌面端 hover 时展示 */}
          <div className="flex items-center">
            {isMob ? (
              <div className="flex items-center gap-1">
                <button
                  className={cn(
                    "flex size-6.5 cursor-pointer items-center justify-center rounded-full text-neutral-400 transition-colors active:bg-neutral-100 dark:active:bg-neutral-700",
                    item.isFavorite && "text-amber-500",
                  )}
                  onClick={handleMobileFavorite}
                  onMouseDown={handleMobileActionMouseDown}
                  title="收藏"
                  type="button"
                >
                  <i
                    className={cn(
                      "i-lucide:star size-3.5",
                      item.isFavorite && "fill-current",
                    )}
                  />
                </button>
                <button
                  className="flex size-6.5 cursor-pointer items-center justify-center rounded-full text-neutral-400 transition-colors active:bg-neutral-100 dark:active:bg-neutral-700"
                  onClick={handleMobileCopy}
                  onMouseDown={handleMobileActionMouseDown}
                  title="复制"
                  type="button"
                >
                  <i className="i-lucide:copy size-3.5" />
                </button>
                <button
                  className="flex size-6.5 cursor-pointer items-center justify-center rounded-full text-neutral-400 transition-colors hover:text-red-500 active:bg-neutral-100 dark:active:bg-neutral-700"
                  onClick={handleMobileDelete}
                  onMouseDown={handleMobileActionMouseDown}
                  title="删除"
                  type="button"
                >
                  <i className="i-lucide:trash-2 size-3.5" />
                </button>
              </div>
            ) : hovered && quickActions.length > 0 ? (
              <ClipboardQuickActions
                item={item}
                labels={quickActionLabels}
                onQuickAction={onQuickAction}
                quickActions={quickActions}
                visible={hovered}
              />
            ) : hintKey ? (
              <span className="flex items-center gap-1 font-mono text-[10.5px] text-neutral-400 tracking-tight">
                <span className="text-[9px] opacity-75">≡</span>
                <span>{hintKey}</span>
              </span>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ClipboardCard;
