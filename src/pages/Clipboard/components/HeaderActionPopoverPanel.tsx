import { useEventListener } from "ahooks";
import type { CSSProperties, FC, ReactNode } from "react";
import { useState } from "react";
import { cn } from "@/utils/cn";

interface HeaderActionPopoverPanelProps {
  children: ReactNode;
  className?: string;
}

const MAX_WIDTH = 320;
const MAX_HEIGHT = 360;
const HORIZONTAL_RESERVED_SPACE = 48;
const VERTICAL_RESERVED_SPACE = 96;

interface PanelSize extends CSSProperties {
  height: number;
  width: number;
}

/**
 * 让顶部三个状态入口共享同一弹窗内容尺寸，并随主窗口可用区域变化。
 */
const HeaderActionPopoverPanel: FC<HeaderActionPopoverPanelProps> = (props) => {
  const { children, className } = props;
  const [size, setSize] = useState(resolvePanelSize);

  function handleResize() {
    const next = resolvePanelSize();
    setSize((current) => {
      if (current.width === next.width && current.height === next.height) {
        return current;
      }
      return next;
    });
  }

  useEventListener("resize", handleResize, { target: window });

  return (
    <div
      className={cn("flex min-h-0 flex-col overflow-x-hidden", className)}
      style={size}
    >
      {children}
    </div>
  );
};

/**
 * 为 antd 内边距、箭头和顶部触发区预留空间，确保弹窗始终留在主窗口内。
 */
function resolvePanelSize(): PanelSize {
  return {
    height: Math.min(
      MAX_HEIGHT,
      Math.max(1, window.innerHeight - VERTICAL_RESERVED_SPACE),
    ),
    width: Math.min(
      MAX_WIDTH,
      Math.max(1, window.innerWidth - HORIZONTAL_RESERVED_SPACE),
    ),
  };
}

export default HeaderActionPopoverPanel;
