import { open as openFileDialog } from "@tauri-apps/plugin-dialog";
import { Form, type GetRef, Input, Modal } from "antd";
import type { FC, MouseEvent } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  importClipboardGroupSvg,
  setClipboardWindowAutoHideSuspended,
} from "@/commands";
import CustomIconButton from "@/components/CustomIconButton";
import type {
  ClipboardGroupIcon as ClipboardGroupIconValue,
  ClipboardGroupInput,
  ClipboardGroupRecord,
} from "@/types/clipboard";
import { cn } from "@/utils/cn";
import ClipboardGroupIcon from "../ClipboardGroupIcon";

export const DEFAULT_GROUP_ICON = "i-lets-icons:folder";
export const DEFAULT_GROUP_COLOR = "#8B5CF6";

export const PRESET_GROUP_COLORS = [
  "#8B5CF6", // 紫 (默认)
  "#EC4899", // 粉
  "#EF4444", // 红
  "#F97316", // 橙
  "#F59E0B", // 黄
  "#10B981", // 绿
  "#06B6D4", // 青
  "#3B82F6", // 蓝
  "#64748B", // 灰
];

export const PRESET_GROUP_ICONS: ClipboardGroupIconValue[] = [
  DEFAULT_GROUP_ICON,
  "i-lets-icons:star",
  "i-lets-icons:book",
  "i-lets-icons:bookmark",
  "i-lets-icons:box",
  "i-lets-icons:database",
  "i-lets-icons:code",
  "i-lets-icons:link",
  "i-lets-icons:notebook",
  "i-lets-icons:calendar",
  "i-lets-icons:bell",
  "i-lets-icons:setting-line",
];

export interface ParsedGroupIcon {
  color: string;
  icon: string;
}

export function parseGroupIcon(rawIcon?: string): ParsedGroupIcon {
  if (!rawIcon) {
    return { color: DEFAULT_GROUP_COLOR, icon: DEFAULT_GROUP_ICON };
  }

  if (rawIcon.startsWith("#")) {
    const splitIndex = rawIcon.indexOf(":");
    if (splitIndex > 0) {
      return {
        color: rawIcon.slice(0, splitIndex),
        icon: rawIcon.slice(splitIndex + 1) || DEFAULT_GROUP_ICON,
      };
    }
    return { color: rawIcon, icon: DEFAULT_GROUP_ICON };
  }

  return { color: DEFAULT_GROUP_COLOR, icon: rawIcon };
}

export function encodeGroupIcon(color: string, icon: string): string {
  return `${color}:${icon}`;
}

type GroupModalMode = "create" | "edit";
type InputRef = GetRef<typeof Input>;

interface ClipboardGroupFormValues {
  color: string;
  icon: ClipboardGroupIconValue;
  name: string;
}

interface ClipboardGroupModalProps {
  group: ClipboardGroupRecord | null;
  mode: GroupModalMode;
  onCancel: () => void;
  onSubmit: (input: ClipboardGroupInput) => Promise<void>;
  open: boolean;
}

/**
 * 判断图标值是否为自定义 SVG。
 */
const isCustomSvgIcon = (icon: ClipboardGroupIconValue) => {
  return icon.trimStart().startsWith("<svg");
};

/**
 * 生成分组弹框的初始表单值。
 */
const buildInitialValues = (
  group: ClipboardGroupRecord | null,
): ClipboardGroupFormValues => {
  const parsed = parseGroupIcon(group?.icon);
  return {
    color: parsed.color,
    icon: parsed.icon,
    name: group?.name ?? "",
  };
};

/**
 * 自定义画板新增 / 编辑共享弹框。
 */
const ClipboardGroupModal: FC<ClipboardGroupModalProps> = (props) => {
  const { group, mode, onCancel, onSubmit, open } = props;
  const { t } = useTranslation(["clipboard", "common"]);
  const [form] = Form.useForm<ClipboardGroupFormValues>();

  const [submitting, setSubmitting] = useState(false);
  const nameInputRef = useRef<InputRef>(null);
  const name = Form.useWatch("name", form) ?? "";
  const color = Form.useWatch("color", form) ?? DEFAULT_GROUP_COLOR;
  const icon = Form.useWatch("icon", form) ?? DEFAULT_GROUP_ICON;

  useEffect(() => {
    if (!open) return;

    form.setFieldsValue(buildInitialValues(group));
  }, [form, group, open]);

  /**
   * 弹框打开后聚焦名称输入框。
   */
  const handleAfterOpenChange = (nextOpen: boolean) => {
    if (!nextOpen) return;

    requestAnimationFrame(() => {
      nameInputRef.current?.focus({ cursor: "end" });
    });
  };

  /**
   * 提交表单，交由调用方决定是新增还是更新。
   */
  const handleSubmit = async () => {
    const values = await form.validateFields();

    setSubmitting(true);

    try {
      await onSubmit({
        icon: encodeGroupIcon(values.color, values.icon),
        isHidden: group?.isHidden ?? false,
        name: values.name,
      });
    } finally {
      setSubmitting(false);
    }
  };

  /**
   * 选择颜色。
   */
  const handleColorClick = (selectedColor: string) => {
    form.setFieldValue("color", selectedColor);
  };

  /**
   * 选择一个预设图标。
   */
  const handlePresetIconClick = (event: MouseEvent<HTMLButtonElement>) => {
    const nextIcon = event.currentTarget.dataset.icon;
    if (!nextIcon) return;

    form.setFieldValue("icon", nextIcon);
  };

  /**
   * 使用 Tauri dialog 选择 SVG 文件，并交给 Rust 读取和校验。
   */
  const importSvg = async () => {
    await setClipboardWindowAutoHideSuspended(true);

    try {
      const selected = await openFileDialog({
        filters: [{ extensions: ["svg"], name: "SVG" }],
        multiple: false,
      });
      if (typeof selected !== "string") return;

      const svg = await importClipboardGroupSvg(selected);
      form.setFieldValue("icon", svg);
    } finally {
      await setClipboardWindowAutoHideSuspended(false);
    }
  };

  /**
   * 删除当前自定义 SVG，回退到默认预设图标。
   */
  const removeCustomIcon = () => {
    form.setFieldValue("icon", DEFAULT_GROUP_ICON);
  };

  const title =
    mode === "create" ? t("clipboard:groups.add") : t("clipboard:groups.edit");
  const customIconSelected = isCustomSvgIcon(icon);

  return (
    <Modal
      afterOpenChange={handleAfterOpenChange}
      confirmLoading={submitting}
      destroyOnHidden
      mask={{ closable: false }}
      okText={t("common:actions.save")}
      onCancel={onCancel}
      onOk={handleSubmit}
      open={open}
      title={title}
    >
      <Form<ClipboardGroupFormValues>
        form={form}
        initialValues={buildInitialValues(group)}
        layout="vertical"
      >
        {/* 顶部实时预览胶囊 */}
        <div className="my-2 flex select-none items-center justify-center rounded-2xl bg-neutral-100/80 p-3.5 dark:bg-white/5">
          <div className="flex items-center gap-2 rounded-full bg-white px-4 py-1.5 font-medium text-[13px] text-neutral-800 shadow-xs dark:bg-[#2A2A2A] dark:text-neutral-100">
            <span
              className="size-2.5 shrink-0 rounded-full transition-colors"
              style={{ backgroundColor: color }}
            />
            <ClipboardGroupIcon
              className="size-4 shrink-0 text-current"
              icon={icon}
            />
            <span className="max-w-[180px] truncate">{name || "画板名称"}</span>
          </div>
        </div>

        <Form.Item
          label={t("clipboard:groups.name")}
          name="name"
          rules={[{ required: true, whitespace: true }]}
        >
          <Input
            maxLength={32}
            placeholder={t("clipboard:groups.namePlaceholder")}
            ref={nameInputRef}
          />
        </Form.Item>

        <Form.Item hidden name="color">
          <Input />
        </Form.Item>

        <Form.Item hidden name="icon">
          <Input />
        </Form.Item>

        {/* 颜色选择 */}
        <Form.Item label="标识颜色">
          <div className="flex items-center gap-2.5 py-1">
            {PRESET_GROUP_COLORS.map((presetColor) => {
              const selected = color === presetColor;
              return (
                <button
                  aria-label={presetColor}
                  className={cn(
                    "relative flex size-7 cursor-pointer items-center justify-center rounded-full shadow-2xs transition-transform hover:scale-110",
                    selected && "scale-105 ring-2 ring-[#007AFF] ring-offset-2",
                  )}
                  key={presetColor}
                  onClick={() => handleColorClick(presetColor)}
                  style={{ backgroundColor: presetColor }}
                  type="button"
                >
                  {selected && (
                    <i className="i-lucide:check size-3.5 stroke-[2.5] text-white" />
                  )}
                </button>
              );
            })}
          </div>
        </Form.Item>

        {/* 图标选择 */}
        <Form.Item label={t("clipboard:groups.icon")}>
          <div className="flex flex-col gap-3">
            <div className="grid grid-cols-6 gap-2">
              {PRESET_GROUP_ICONS.map((presetIcon) => {
                const selected = icon === presetIcon;

                return (
                  <button
                    className={cn(
                      "flex size-9 cursor-pointer items-center justify-center rounded-2 border border-ant-border bg-ant-container transition-colors",
                      {
                        "border-ant-primary bg-ant-primary": selected,
                        "hover:bg-ant-fill-tertiary": !selected,
                      },
                    )}
                    data-icon={presetIcon}
                    key={presetIcon}
                    onClick={handlePresetIconClick}
                    type="button"
                  >
                    <ClipboardGroupIcon icon={presetIcon} selected={selected} />
                  </button>
                );
              })}
            </div>

            <div className="flex gap-2">
              <CustomIconButton
                className="flex-1"
                icon={
                  <ClipboardGroupIcon
                    icon={icon}
                    selected={customIconSelected}
                  />
                }
                onClick={importSvg}
              >
                {t(
                  customIconSelected
                    ? "clipboard:groups.customIcon"
                    : "clipboard:groups.useCustomIcon",
                )}
              </CustomIconButton>

              {customIconSelected ? (
                <CustomIconButton
                  icon={
                    <i
                      aria-hidden="true"
                      className="i-lucide:trash-2 text-sm!"
                    />
                  }
                  onClick={removeCustomIcon}
                  title={t("clipboard:groups.removeIcon")}
                />
              ) : null}
            </div>
          </div>
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default ClipboardGroupModal;
