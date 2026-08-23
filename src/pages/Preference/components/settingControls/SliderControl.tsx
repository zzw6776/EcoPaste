import { Slider } from "antd";
import type { FC } from "react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { PreferenceSetting } from "../../types/preferences";
import { translatePreferenceNumberSuffix } from "../../utils/preferenceI18n";
import type { ControlProps } from "./types";

interface SliderControlProps extends ControlProps {
  setting: PreferenceSetting;
  value: number;
}

/** 滑动时即时预览数值，释放滑块后持久化并触发原生配置热更新。 */
const SliderControl: FC<SliderControlProps> = (props) => {
  const { t } = useTranslation("preferences");
  const { disabled, onChange, setting, value } = props;
  const [draft, setDraft] = useState(value);
  const control = setting.control.type === "slider" ? setting.control : null;

  useEffect(() => {
    setDraft(value);
  }, [value]);

  if (!control) return null;

  const suffix = translatePreferenceNumberSuffix(t, setting);

  const handleChange = (next: number) => {
    setDraft(next);
  };

  const handleChangeComplete = async (next: number) => {
    setDraft(next);
    await onChange(setting, next);
  };

  return (
    <div className="flex w-full min-w-52 items-center gap-3 md:w-72">
      <Slider
        className="m-0 min-w-0 flex-1"
        disabled={disabled}
        max={control.max}
        min={control.min}
        onChange={handleChange}
        onChangeComplete={handleChangeComplete}
        step={control.step}
        tooltip={{ open: false }}
        value={draft}
      />
      <span className="w-14 shrink-0 text-right text-ant-secondary text-xs tabular-nums">
        {draft} {suffix}
      </span>
    </div>
  );
};

export default SliderControl;
