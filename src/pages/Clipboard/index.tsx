import type { PointerEvent as ReactPointerEvent } from "react";
import { useRef } from "react";
import { resizeClipboardWindow } from "@/commands";
import { useClipboardWindowEditableFocus } from "@/hooks/useClipboardWindowEditableFocus";
import { isTauri } from "@/utils/is";
import Header from "./components/Header";
import List from "./components/List";

/**
 * 剪贴板主窗口：Paste 官方 1:1 通透毛玻璃全景托盘。
 * 支持鼠标按住顶部上边框上下拖动，动态平滑放大/缩小弹出框高度。
 */
const Clipboard = () => {
  useClipboardWindowEditableFocus();

  const isDraggingRef = useRef(false);
  const startYRef = useRef(0);
  const startHeightRef = useRef(340);
  const containerRef = useRef<HTMLDivElement>(null);

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

  const content = (
    <div
      className="relative flex h-full w-full select-none flex-col overflow-hidden rounded-[26px] border border-white/50 bg-white/70 text-neutral-800 shadow-[0_20px_50px_rgba(0,0,0,0.2)] dark:border-white/15 dark:bg-black/60 dark:text-white"
      data-tauri-drag-region
    >
      {/* 顶部上下拉伸调节隐形热区（完全不显示小横条） */}
      <div
        className="absolute top-0 right-0 left-0 z-50 h-2.5 cursor-ns-resize touch-none"
        onPointerCancel={handleResizeEnd}
        onPointerDown={handleResizeStart}
        onPointerMove={handleResizeMove}
        onPointerUp={handleResizeEnd}
      />

      <Header />

      <div className="min-h-0 w-full flex-1 overflow-hidden">
        <List />
      </div>
    </div>
  );

  if (!isTauri) {
    return (
      <div
        className="relative flex h-screen w-screen select-none items-end justify-center overflow-hidden px-4 pb-4"
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
        <div
          className="flex h-[340px] w-full max-w-full flex-col rounded-[24px] border border-white/30 bg-white/20 drop-shadow-2xl backdrop-blur-2xl"
          ref={containerRef}
        >
          {content}
        </div>
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

export default Clipboard;
