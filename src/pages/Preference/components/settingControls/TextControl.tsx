import { Button, Input } from "antd";
import type { ChangeEvent, FC } from "react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { PreferenceSetting } from "../../types/preferences";
import { translatePreferencePlaceholder } from "../../utils/preferenceI18n";
import type { ControlProps } from "./types";

interface TextControlProps extends ControlProps {
  setting: PreferenceSetting;
  value: string;
}

/**
 * 短文本输入失焦保存，主要用于快捷键字面量。
 */
const TextControl: FC<TextControlProps> = (props) => {
  const { t } = useTranslation("preferences");
  const { disabled, onChange, setting, value } = props;
  const [draft, setDraft] = useState(value);
  const [saving, setSaving] = useState(false);
  const control = setting.control.type === "text" ? setting.control : null;

  useEffect(() => {
    setDraft(value);
  }, [value]);

  if (!control) return null;

  const explicitSave = setting.id === "sync.serverEndpointId";
  const dirty = draft.trim() !== value;

  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    setDraft(event.target.value);
  };

  const commit = async () => {
    await onChange(setting, draft.trim());
  };

  const handleBlur = async () => {
    await commit();
  };

  const handlePressEnter = async () => {
    await commit();
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await commit();
    } finally {
      setSaving(false);
    }
  };

  if (explicitSave) {
    return (
      <div className="flex w-full flex-col items-end gap-2 md:w-72">
        <Input
          disabled={disabled}
          onChange={handleChange}
          onPressEnter={handlePressEnter}
          placeholder={translatePreferencePlaceholder(t, setting)}
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
    <Input
      disabled={disabled}
      onBlur={handleBlur}
      onChange={handleChange}
      onPressEnter={handlePressEnter}
      placeholder={translatePreferencePlaceholder(t, setting)}
      value={draft}
    />
  );
};

export default TextControl;
