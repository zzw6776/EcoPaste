import { Button, Input, type InputRef, Popover } from "antd";
import type { FC, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type {
  ClipboardGroupInput,
  ClipboardGroupRecord,
} from "@/types/clipboard";
import { cn } from "@/utils/cn";
import ClipboardGroupIcon from "../ClipboardGroupIcon";

export const PRESET_GROUP_COLORS = [
  "#8B5CF6", // 紫
  "#EC4899", // 粉
  "#EF4444", // 红
  "#F97316", // 橙
  "#F59E0B", // 黄
  "#10B981", // 绿
  "#06B6D4", // 青
  "#3B82F6", // 蓝
  "#64748B", // 灰
];

export const PRESET_GROUP_ICONS = [
  "", // 无图标 (默认)
  "i-lets-icons:folder",
  "i-lets-icons:star",
  "i-lets-icons:bookmark",
  "i-lets-icons:box",
  "i-lets-icons:code",
  "i-lets-icons:link",
  "i-lets-icons:bell",
  "i-lets-icons:setting-line",
];

export interface ParsedGroupIcon {
  color: string;
  icon: string;
}

export function parseGroupIcon(rawIcon?: string): ParsedGroupIcon {
  if (!rawIcon) {
    return { color: "#8B5CF6", icon: "" };
  }

  if (rawIcon.startsWith("#")) {
    const splitIndex = rawIcon.indexOf(":");
    if (splitIndex > 0) {
      const color = rawIcon.slice(0, splitIndex);
      const icon = rawIcon.slice(splitIndex + 1);
      return { color, icon: icon === "none" ? "" : icon };
    }
    return { color: rawIcon, icon: "" };
  }

  if (rawIcon.startsWith("none:")) {
    return { color: "", icon: rawIcon.slice(5) };
  }

  if (rawIcon === "none") {
    return { color: "", icon: "" };
  }

  // 纯图标字符串（无颜色前缀）
  return { color: "#8B5CF6", icon: rawIcon };
}

export function encodeGroupIcon(color: string, icon: string): string {
  const c = color || "none";
  const i = icon || "none";
  return `${c}:${i}`;
}

interface ClipboardGroupPopoverProps {
  children: ReactNode;
  group: ClipboardGroupRecord | null;
  mode: "create" | "edit";
  onClose: () => void;
  onSubmit: (input: ClipboardGroupInput) => Promise<void>;
  open: boolean;
}

/**
 * 1:1 对标 Paste 的顶部栏紧凑画板编辑/新建气泡下拉框
 */
export const ClipboardGroupPopover: FC<ClipboardGroupPopoverProps> = (
  props,
) => {
  const { children, group, mode, onClose, onSubmit, open } = props;
  const { t } = useTranslation(["clipboard", "common"]);

  const [name, setName] = useState("");
  const [color, setColor] = useState("#8B5CF6");
  const [icon, setIcon] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const inputRef = useRef<InputRef>(null);

  useEffect(() => {
    if (!open) return;

    if (group) {
      const parsed = parseGroupIcon(group.icon);
      setName(group.name);
      setColor(parsed.color);
      setIcon(parsed.icon);
    } else {
      setName("");
      setColor("#8B5CF6");
      setIcon("");
    }

    const timer = setTimeout(() => {
      inputRef.current?.focus({ cursor: "end" });
    }, 60);

    return () => clearTimeout(timer);
  }, [group, open]);

  const handleSave = async () => {
    const trimmed = name.trim();
    if (!trimmed) {
      inputRef.current?.focus();
      return;
    }

    setSubmitting(true);
    try {
      await onSubmit({
        icon: encodeGroupIcon(color, icon),
        isHidden: group?.isHidden ?? false,
        name: trimmed,
      });
      onClose();
    } finally {
      setSubmitting(false);
    }
  };

  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void handleSave();
    } else if (event.key === "Escape") {
      event.preventDefault();
      onClose();
    }
  };

  const popoverContent = (
    <div
      className="flex w-[290px] select-none flex-col gap-3 p-3.5 text-neutral-800 dark:text-neutral-200"
      onKeyDown={handleKeyDown}
      role="dialog"
    >
      {/* 标题 */}
      <div className="flex items-center justify-between">
        <span className="font-bold text-[12.5px] text-neutral-700 dark:text-neutral-200">
          {mode === "create" ? "新建画板" : "编辑画板"}
        </span>
      </div>

      {/* 1. 画板名称输入框（前置实时预览小圆点与图标） */}
      <div className="flex items-center gap-2 rounded-xl bg-neutral-100 px-3 py-1.5 focus-within:ring-2 focus-within:ring-[#007AFF] dark:bg-white/10">
        {color ? (
          <span
            className="size-3 shrink-0 rounded-full transition-colors"
            style={{ backgroundColor: color }}
          />
        ) : (
          <span className="size-3 shrink-0 rounded-full border border-neutral-400 border-dashed" />
        )}
        {icon ? (
          <ClipboardGroupIcon
            className="size-3.5 shrink-0 text-current"
            icon={icon}
          />
        ) : null}
        <Input
          className="border-0 bg-transparent p-0 font-medium text-[13px] placeholder:text-neutral-400 focus:shadow-none"
          maxLength={24}
          onChange={(e) => setName(e.target.value)}
          placeholder="画板名称"
          ref={inputRef}
          value={name}
          variant="borderless"
        />
      </div>

      {/* 2. 颜色选择器（横向色块，支持取消选择颜色） */}
      <div className="flex flex-col gap-1.5">
        <span className="font-medium text-[11px] text-neutral-400">
          标识颜色
        </span>
        <div className="flex items-center justify-between">
          {/* 无颜色选项 */}
          <button
            aria-label="无颜色"
            className={cn(
              "flex size-5.5 cursor-pointer items-center justify-center rounded-full border border-neutral-300 border-dashed transition-all hover:scale-110 dark:border-white/20",
              !color &&
                "border-transparent ring-2 ring-[#007AFF] ring-offset-1",
            )}
            onClick={() => setColor("")}
            title="无颜色"
            type="button"
          >
            <i className="i-lucide:ban size-3 text-neutral-400" />
          </button>

          {PRESET_GROUP_COLORS.map((presetColor) => {
            const selected = color === presetColor;
            return (
              <button
                aria-label={presetColor}
                className={cn(
                  "flex size-5.5 cursor-pointer items-center justify-center rounded-full shadow-2xs transition-transform hover:scale-110",
                  selected && "scale-105 ring-2 ring-[#007AFF] ring-offset-1",
                )}
                key={presetColor}
                onClick={() => setColor(presetColor)}
                style={{ backgroundColor: presetColor }}
                type="button"
              >
                {selected && (
                  <i className="i-lucide:check size-3 stroke-[2.5] text-white" />
                )}
              </button>
            );
          })}
        </div>
      </div>

      {/* 3. 图标选择器（可选，默认无图标） */}
      <div className="flex flex-col gap-1.5">
        <span className="font-medium text-[11px] text-neutral-400">
          图标（可选）
        </span>
        <div className="grid grid-cols-9 items-center gap-1">
          {PRESET_GROUP_ICONS.map((presetIcon) => {
            const selected = icon === presetIcon;
            return (
              <button
                className={cn(
                  "flex size-6 cursor-pointer items-center justify-center rounded-lg border border-transparent text-xs transition-colors",
                  selected
                    ? "bg-[#007AFF] font-bold text-white"
                    : "text-neutral-500 hover:bg-black/5 dark:text-neutral-400 dark:hover:bg-white/10",
                )}
                key={presetIcon || "none"}
                onClick={() => setIcon(presetIcon)}
                title={presetIcon ? undefined : "无图标"}
                type="button"
              >
                {presetIcon ? (
                  <ClipboardGroupIcon
                    icon={presetIcon}
                    inheritColor={selected}
                  />
                ) : (
                  <i className="i-lucide:slash size-3 opacity-60" />
                )}
              </button>
            );
          })}
        </div>
      </div>

      {/* 4. 底部动作按钮 */}
      <div className="mt-1 flex items-center justify-end gap-2 border-neutral-100 border-t pt-1 dark:border-white/10">
        <Button
          className="h-7 px-3 text-xs"
          onClick={onClose}
          size="small"
          type="text"
        >
          {t("common:actions.cancel", { defaultValue: "取消" })}
        </Button>
        <Button
          className="h-7 border-0 bg-[#007AFF] px-3.5 text-xs hover:bg-[#0062CC]"
          disabled={!name.trim()}
          loading={submitting}
          onClick={() => void handleSave()}
          size="small"
          type="primary"
        >
          {mode === "create" ? "创建" : "完成"}
        </Button>
      </div>
    </div>
  );

  return (
    <Popover
      arrow={false}
      content={popoverContent}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) onClose();
      }}
      open={open}
      overlayClassName="clipboard-group-popover"
      overlayInnerStyle={{
        backdropFilter: "blur(20px)",
        backgroundColor: "rgba(255, 255, 255, 0.96)",
        borderRadius: 16,
        boxShadow:
          "0 16px 36px -4px rgba(0, 0, 0, 0.22), 0 0 1px rgba(0, 0, 0, 0.15)",
        padding: 0,
      }}
      placement="bottom"
      trigger="click"
    >
      {children}
    </Popover>
  );
};

export default ClipboardGroupPopover;
