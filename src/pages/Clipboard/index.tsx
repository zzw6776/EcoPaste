import type { PointerEvent as ReactPointerEvent } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useSnapshot } from "valtio";
import { resizeClipboardWindow } from "@/commands";
import { minimizeAndroidApp } from "@/commands/android";
import { useClipboardWindowEditableFocus } from "@/hooks/useClipboardWindowEditableFocus";
import { clipboardViewState } from "@/stores/clipboardView";
import type { ClipboardCategory, ClipboardRange } from "@/types/clipboard";
import { cn } from "@/utils/cn";
import { isAndroid, isMobile, isTauri, isWin } from "@/utils/is";
import Header from "./components/Header";
import List, { type ClipboardListView } from "./components/List";

interface BuiltInFilter {
  category: ClipboardCategory | null;
  range: ClipboardRange;
}

const BUILT_IN_FILTERS: BuiltInFilter[] = [
  { category: null, range: "all" },
  { category: null, range: "favorite" },
  { category: "text", range: "all" },
  { category: "image", range: "all" },
  { category: "files", range: "all" },
];
const CATEGORY_DRAG_DIRECTION_THRESHOLD = 6;
const CATEGORY_DRAG_DISTANCE_RATIO = 0.2;
const CATEGORY_DRAG_MIN_FLING_DISTANCE = 12;
const CATEGORY_DRAG_VELOCITY_THRESHOLD = 0.45;
const CATEGORY_SNAP_TRANSITION =
  "transform 220ms cubic-bezier(0.22, 1, 0.36, 1)";

interface CategoryDragState {
  anchorX: number;
  lastAt: number;
  lastX: number;
  mode: "pending" | "horizontal" | "vertical";
  pointerId: number;
  startOffset: number;
  startX: number;
  startY: number;
}

/**
 * 剪贴板主窗口：
 * - 桌面端 (Mac/Win)：Paste 官方 1:1 通透毛玻璃底栏抽屉，支持上下拉伸与高度记忆；
 * - 移动端 (Android/iOS)：顺滑 Bottom Sheet 抽屉，支持左右底角上滑呼出与手势收起。
 */
const Clipboard = () => {
  useClipboardWindowEditableFocus();
  const clipboardSnapshot = useSnapshot(clipboardViewState);

  const isDraggingRef = useRef(false);
  const startYRef = useRef(0);
  const startHeightRef = useRef(340);
  const popupDragStartYRef = useRef(0);
  const categoryPagerRef = useRef<HTMLDivElement>(null);
  const categoryPagerTrackRef = useRef<HTMLDivElement>(null);
  const categoryPagerIndexRef = useRef(0);
  const categoryPagerReadyRef = useRef(false);
  const categoryDragRef = useRef<CategoryDragState | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // 移动端/桌面端视口自适应与预览状态
  const [mobileMode, setMobileMode] = useState(() => isMobile());
  const [androidPopupMode, setAndroidPopupMode] = useState(() => {
    return isAndroid && window.innerHeight < window.screen.height * 0.95;
  });
  const [sheetOpen, setSheetOpen] = useState(true);
  const builtInFilterIndex = Math.max(
    0,
    getCurrentBuiltInFilterIndex(
      clipboardSnapshot.range,
      clipboardSnapshot.category,
      clipboardSnapshot.groupId,
    ),
  );

  const positionCategoryPager = useCallback(
    (index: number, animated: boolean) => {
      const pager = categoryPagerRef.current;
      const track = categoryPagerTrackRef.current;
      if (!pager || !track) return;

      categoryPagerIndexRef.current = index;
      track.style.transition = animated ? CATEGORY_SNAP_TRANSITION : "none";
      track.style.transform = `translate3d(${-pager.clientWidth * index}px, 0, 0)`;
    },
    [],
  );

  useEffect(() => {
    const handleResize = () => {
      if (!isTauri) {
        setMobileMode(window.innerWidth < 768);
      }
      if (isAndroid) {
        setAndroidPopupMode(window.innerHeight < window.screen.height * 0.95);
      }
    };
    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (!mobileMode) return;

    const pager = categoryPagerRef.current;
    if (!pager || !categoryPagerTrackRef.current) return;

    const frame = requestAnimationFrame(() => {
      positionCategoryPager(builtInFilterIndex, categoryPagerReadyRef.current);
      categoryPagerReadyRef.current = true;
    });
    const observer = new ResizeObserver(() => {
      if (categoryDragRef.current?.mode === "horizontal") return;

      positionCategoryPager(categoryPagerIndexRef.current, false);
    });
    observer.observe(pager);

    return () => {
      cancelAnimationFrame(frame);
      observer.disconnect();
    };
  }, [builtInFilterIndex, mobileMode, positionCategoryPager]);

  const handleResizeStart = (event: ReactPointerEvent<HTMLDivElement>) => {
    isDraggingRef.current = true;
    startYRef.current = event.screenY;
    startHeightRef.current = containerRef.current?.clientHeight || 340;
    event.currentTarget.setPointerCapture(event.pointerId);
  };

  const handleResizeMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (!isDraggingRef.current) return;
    const delta = startYRef.current - event.screenY;
    const newHeight = Math.min(
      680,
      Math.max(250, startHeightRef.current + delta),
    );

    if (isTauri) {
      void resizeClipboardWindow(newHeight);
    } else if (containerRef.current) {
      containerRef.current.style.height = `${newHeight}px`;
    }
  };

  const handleResizeEnd = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (!isDraggingRef.current) return;
    isDraggingRef.current = false;
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // 忽略 release 异常
    }
  };

  const handlePopupDragStart = (
    event: ReactPointerEvent<HTMLButtonElement>,
  ) => {
    popupDragStartYRef.current = event.screenY;
    event.currentTarget.setPointerCapture(event.pointerId);
  };

  const handlePopupDragEnd = (event: ReactPointerEvent<HTMLButtonElement>) => {
    const delta = event.screenY - popupDragStartYRef.current;
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // 指针已被系统取消时无需重复释放。
    }
    if (delta < 64) return;

    void minimizeAndroidApp();
  };

  const handlePopupDragCancel = (
    event: ReactPointerEvent<HTMLButtonElement>,
  ) => {
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // 系统取消手势时指针可能已经释放。
    }
  };

  const handleCategoryPagerPointerDown = (
    event: ReactPointerEvent<HTMLDivElement>,
  ) => {
    if (event.button !== 0) return;

    categoryDragRef.current = {
      anchorX: event.clientX,
      lastAt: event.timeStamp,
      lastX: event.clientX,
      mode: "pending",
      pointerId: event.pointerId,
      startOffset: 0,
      startX: event.clientX,
      startY: event.clientY,
    };
  };

  const handleCategoryPagerPointerMove = (
    event: ReactPointerEvent<HTMLDivElement>,
  ) => {
    const drag = categoryDragRef.current;
    const pager = categoryPagerRef.current;
    const track = categoryPagerTrackRef.current;
    if (!drag || drag.pointerId !== event.pointerId || !pager || !track) return;

    const deltaX = event.clientX - drag.startX;
    const deltaY = event.clientY - drag.startY;
    if (drag.mode === "pending") {
      if (
        Math.max(Math.abs(deltaX), Math.abs(deltaY)) <
        CATEGORY_DRAG_DIRECTION_THRESHOLD
      ) {
        return;
      }

      if (Math.abs(deltaY) >= Math.abs(deltaX)) {
        drag.mode = "vertical";
        return;
      }

      drag.mode = "horizontal";
      drag.anchorX = event.clientX;
      drag.startOffset = getRenderedTranslateX(track);
      track.style.transition = "none";
      track.style.transform = `translate3d(${drag.startOffset}px, 0, 0)`;
      event.currentTarget.setPointerCapture(event.pointerId);
    }

    if (drag.mode !== "horizontal") return;

    event.preventDefault();
    const pointerDelta = event.clientX - drag.anchorX;
    const rawOffset = drag.startOffset + pointerDelta;
    const minOffset = -pager.clientWidth * (BUILT_IN_FILTERS.length - 1);
    const resistedOffset = resistCategoryPagerEdge(rawOffset, minOffset);
    track.style.transform = `translate3d(${resistedOffset}px, 0, 0)`;
    drag.lastAt = event.timeStamp;
    drag.lastX = event.clientX;
  };

  const handleCategoryPagerPointerEnd = (
    event: ReactPointerEvent<HTMLDivElement>,
  ) => {
    const drag = categoryDragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;

    categoryDragRef.current = null;
    if (drag.mode !== "horizontal") return;

    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // WebView 取消手势时可能已经释放指针。
    }

    const pager = categoryPagerRef.current;
    if (!pager || pager.clientWidth <= 0) return;

    const deltaX = event.clientX - drag.startX;
    const elapsed = Math.max(event.timeStamp - drag.lastAt, 1);
    const velocity = (event.clientX - drag.lastX) / elapsed;
    const crossedDistance =
      Math.abs(deltaX) >= pager.clientWidth * CATEGORY_DRAG_DISTANCE_RATIO;
    const flung =
      Math.abs(deltaX) >= CATEGORY_DRAG_MIN_FLING_DISTANCE &&
      Math.abs(velocity) >= CATEGORY_DRAG_VELOCITY_THRESHOLD;
    const direction = crossedDistance || flung ? (deltaX < 0 ? 1 : -1) : 0;
    const nextIndex = Math.max(
      0,
      Math.min(
        BUILT_IN_FILTERS.length - 1,
        categoryPagerIndexRef.current + direction,
      ),
    );

    positionCategoryPager(nextIndex, true);
    const filter = BUILT_IN_FILTERS[nextIndex];
    if (filter) applyBuiltInFilter(filter);
  };

  const handleCategoryPagerPointerCancel = (
    event: ReactPointerEvent<HTMLDivElement>,
  ) => {
    const drag = categoryDragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;

    categoryDragRef.current = null;
    if (drag.mode === "horizontal") {
      positionCategoryPager(categoryPagerIndexRef.current, true);
    }
  };

  const content = (
    <div
      className={cn(
        "relative flex h-full w-full select-none flex-col overflow-hidden text-neutral-800 dark:text-white",
        mobileMode
          ? cn(
              "bg-[#F2F2F7] dark:bg-[#121212]",
              androidPopupMode ? "rounded-t-4" : "mobile-safe-area-top",
            )
          : cn({
              "bg-white/0 dark:bg-black/0": isWin,
              "rounded-[26px] border border-white/50 bg-white/0 dark:border-white/15 dark:bg-black/0":
                !isWin,
            }),
      )}
      data-tauri-drag-region
    >
      {!mobileMode ? (
        /* 桌面端顶部上下拉伸调节隐形热区 */
        <div
          className="absolute top-0 right-0 left-0 z-50 h-2.5 cursor-ns-resize touch-none"
          onPointerCancel={handleResizeEnd}
          onPointerDown={handleResizeStart}
          onPointerMove={handleResizeMove}
          onPointerUp={handleResizeEnd}
        />
      ) : null}

      {androidPopupMode ? (
        <button
          aria-label="下滑收起剪贴板"
          className="flex h-5 shrink-0 cursor-ns-resize touch-none items-center justify-center border-0 bg-transparent p-0"
          onPointerCancel={handlePopupDragCancel}
          onPointerDown={handlePopupDragStart}
          onPointerUp={handlePopupDragEnd}
          type="button"
        >
          <span className="h-1 w-10 rounded-full bg-black/20 dark:bg-white/25" />
        </button>
      ) : null}

      <Header />

      {mobileMode ? (
        <div
          className="relative min-h-0 w-full flex-1 touch-pan-y overflow-hidden"
          onPointerCancel={handleCategoryPagerPointerCancel}
          onPointerDown={handleCategoryPagerPointerDown}
          onPointerMove={handleCategoryPagerPointerMove}
          onPointerUp={handleCategoryPagerPointerEnd}
          ref={categoryPagerRef}
        >
          <div
            className="flex size-full will-change-transform"
            ref={categoryPagerTrackRef}
          >
            {BUILT_IN_FILTERS.map((filter, index) => {
              const view = getBuiltInListView(
                filter,
                index,
                clipboardSnapshot.range,
                clipboardSnapshot.category,
                clipboardSnapshot.groupId,
              );
              const preloaded = Math.abs(index - builtInFilterIndex) <= 1;

              return (
                <section
                  className="h-full w-full shrink-0 overflow-hidden"
                  key={`${filter.range}:${filter.category ?? "all"}`}
                >
                  {preloaded ? (
                    <List active={index === builtInFilterIndex} view={view} />
                  ) : null}
                </section>
              );
            })}
          </div>
        </div>
      ) : (
        <div className="min-h-0 w-full flex-1 overflow-hidden">
          <List />
        </div>
      )}

      {isWin && !mobileMode ? (
        <div
          aria-hidden
          className="pointer-events-none absolute inset-0 z-50 border border-black/15 dark:border-white/18"
        />
      ) : null}
    </div>
  );

  // 非 Tauri 环境（Web 预览模式）：支持自由切换桌面底栏与手机版底角上滑演示
  if (!isTauri) {
    return (
      <div
        className="relative flex h-screen w-screen select-none items-end justify-center overflow-hidden"
        style={{
          background: `
            radial-gradient(at 0% 0%, rgba(255, 182, 193, 0.7) 0px, transparent 50%),
            radial-gradient(at 100% 0%, rgba(165, 243, 252, 0.7) 0px, transparent 50%),
            radial-gradient(at 50% 100%, rgba(196, 181, 253, 0.8) 0px, transparent 50%),
            radial-gradient(at 85% 85%, rgba(254, 215, 170, 0.7) 0px, transparent 50%),
            linear-gradient(135deg, #f0fdf4 0%, #e0e7ff 40%, #fae8ff 70%, #fef3c7 100%)
          `,
        }}
      >
        {/* 顶部预览模式切换栏 */}
        <div className="absolute top-4 z-50 flex items-center gap-2 rounded-full border border-white/40 bg-white/60 px-3.5 py-1.5 shadow-lg backdrop-blur-md dark:border-white/10 dark:bg-black/50">
          <button
            className={cn(
              "cursor-pointer rounded-full px-3 py-1 font-semibold text-xs transition-all",
              !mobileMode
                ? "bg-[#007AFF] text-white shadow-xs"
                : "text-neutral-600 hover:text-neutral-900 dark:text-neutral-300",
            )}
            onClick={() => {
              setMobileMode(false);
              setSheetOpen(true);
            }}
            type="button"
          >
            🖥️ Mac/Win 桌面底栏
          </button>
          <button
            className={cn(
              "cursor-pointer rounded-full px-3 py-1 font-semibold text-xs transition-all",
              mobileMode
                ? "bg-[#007AFF] text-white shadow-xs"
                : "text-neutral-600 hover:text-neutral-900 dark:text-neutral-300",
            )}
            onClick={() => {
              setMobileMode(true);
              setSheetOpen(true);
            }}
            type="button"
          >
            📱 安卓/移动端手势抽屉
          </button>
        </div>

        {/* 移动端手势演示模式 */}
        {mobileMode ? (
          <div className="relative flex h-full w-full max-w-sm flex-col items-center justify-end overflow-hidden pb-1 sm:h-[780px] sm:rounded-[44px] sm:border-[8px] sm:border-neutral-800 sm:bg-black/90 sm:shadow-[0_25px_60px_rgba(0,0,0,0.5)]">
            {/* 手机状态栏模拟 */}
            <div className="absolute top-3 right-0 left-0 hidden items-center justify-between px-7 font-semibold text-neutral-800 text-xs sm:flex dark:text-white/80">
              <span>09:41</span>
              <div className="h-4 w-20 rounded-full bg-black/40 dark:bg-white/20" />
              <span>5G 100%</span>
            </div>

            {/* 左右底角上滑手势触发区提示 */}
            {!sheetOpen ? (
              <div className="absolute inset-x-0 bottom-0 z-40 flex h-24 items-end justify-between px-3 pb-3">
                <button
                  className="flex h-14 w-28 animate-bounce cursor-pointer flex-col items-center justify-center rounded-2xl border border-[#007AFF]/40 bg-[#007AFF]/20 font-medium text-[#007AFF] text-[11px] shadow-lg backdrop-blur-md"
                  onClick={() => setSheetOpen(true)}
                  type="button"
                >
                  <span>↖ 左底角上滑</span>
                  <span className="text-[9px] opacity-75">呼出剪贴板</span>
                </button>
                <div className="text-[10px] text-neutral-400">系统桌面区</div>
                <button
                  className="flex h-14 w-28 animate-bounce cursor-pointer flex-col items-center justify-center rounded-2xl border-[#007AFF]/40 bg-[#007AFF]/20 font-medium text-[#007AFF] text-[11px] shadow-lg backdrop-blur-md"
                  onClick={() => setSheetOpen(true)}
                  type="button"
                >
                  <span>↗ 右底角上滑</span>
                  <span className="text-[9px] opacity-75">呼出剪贴板</span>
                </button>
              </div>
            ) : null}

            {/* 移动端 Bottom Sheet 抽屉卡片 */}
            <div
              className={cn(
                "w-full drop-shadow-2xl transition-transform duration-300 ease-out",
                sheetOpen
                  ? "h-[380px] translate-y-0"
                  : "pointer-events-none h-[380px] translate-y-full",
              )}
              ref={containerRef}
            >
              {content}
            </div>
          </div>
        ) : (
          /* 桌面端 1:1 毛玻璃底栏 */
          <div
            className="flex h-[340px] w-full max-w-full flex-col px-4 pb-4 drop-shadow-2xl"
            ref={containerRef}
          >
            {content}
          </div>
        )}
      </div>
    );
  }

  return (
    <div
      className="flex size-screen flex-col overflow-hidden bg-transparent"
      ref={containerRef}
    >
      {content}
    </div>
  );
};

function getCurrentBuiltInFilterIndex(
  range: ClipboardRange,
  category: ClipboardCategory | null,
  groupId: string | null,
) {
  if (groupId !== null) return -1;
  if (range === "favorite") return 1;
  if (category === null) return 0;

  return BUILT_IN_FILTERS.findIndex((filter) => {
    return filter.category === category;
  });
}

function applyBuiltInFilter(filter: BuiltInFilter) {
  clipboardViewState.groupId = null;
  clipboardViewState.range = filter.range;
  clipboardViewState.category = filter.category;
}

/** 读取当前合成后的横向位移，允许用户在分页回弹动画中途继续拖动。 */
function getRenderedTranslateX(element: HTMLElement) {
  const transform = window.getComputedStyle(element).transform;
  if (transform === "none") return 0;

  return new DOMMatrixReadOnly(transform).m41;
}

/** 首尾页保留轻微阻尼，避免轨道被直接拖出可视区域。 */
function resistCategoryPagerEdge(offset: number, minOffset: number) {
  if (offset > 0) return offset * 0.2;
  if (offset < minOffset) return minOffset + (offset - minOffset) * 0.2;

  return offset;
}

/** 自定义分组占用首个分页；滑离后再恢复为“全部历史”。 */
function getBuiltInListView(
  filter: BuiltInFilter,
  index: number,
  range: ClipboardRange,
  category: ClipboardCategory | null,
  groupId: string | null,
): ClipboardListView {
  if (index === 0 && groupId !== null) {
    return { category, groupId, range };
  }

  return {
    category: filter.category,
    groupId: null,
    range: filter.range,
  };
}

export default Clipboard;
