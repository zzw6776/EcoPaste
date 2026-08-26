import { Button, Input } from "antd";
import type { ChangeEvent, FC, FocusEvent } from "react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { PreferenceSetting } from "../../types/preferences";
import { translatePreferencePlaceholder } from "../../utils/preferenceI18n";
import type { ControlProps } from "./types";

interface TextareaControlProps extends ControlProps {
  setting: PreferenceSetting;
  value: string[];
}

/**
 * 多行规则输入：一行一个值，失焦后整体替换数组。
 */
const TextareaControl: FC<TextareaControlProps> = (props) => {
  const { t } = useTranslation("preferences");
  const { disabled, onChange, setting, value } = props;
  const [draft, setDraft] = useState(value.join("\n"));
  const [saving, setSaving] = useState(false);

  const placeholder =
    setting.control.type === "textarea"
      ? translatePreferencePlaceholder(t, setting)
      : "";

  useEffect(() => {
    setDraft(value.join("\n"));
  }, [value]);

  const explicitSave = setting.id.startsWith("sync.server");
  const nextValue = normalizeLines(draft);
  const dirty =
    nextValue.length !== value.length ||
    nextValue.some((line, index) => {
      return line !== value[index];
    });

  const handleChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    setDraft(event.target.value);
  };

  const handleBlur = async (_event: FocusEvent<HTMLTextAreaElement>) => {
    await onChange(setting, nextValue);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await onChange(setting, nextValue);
    } finally {
      setSaving(false);
    }
  };

  if (explicitSave) {
    return (
      <div className="flex w-full flex-col items-end gap-2 md:w-72">
        <Input.TextArea
          disabled={disabled}
          onChange={handleChange}
          placeholder={placeholder}
          value={draft}
        />
        <Button
          disabled={disabled || !dirty}
          loading={saving}
          onClick={handleSave}
          size="small"
          type="primary"
        >
          {t("sync.actions.saveServer")}
        </Button>
      </div>
    );
  }

  return (
    <Input.TextArea
      disabled={disabled}
      onBlur={handleBlur}
      onChange={handleChange}
      placeholder={placeholder}
      value={draft}
    />
  );
};

export default TextareaControl;

/**
 * 把多行输入转换为设置数组，同时过滤空行。
 */
function normalizeLines(value: string) {
  return value
    .split("\n")
    .map((line) => {
      return line.trim();
    })
    .filter((line) => {
      return line.length > 0;
    });
}
