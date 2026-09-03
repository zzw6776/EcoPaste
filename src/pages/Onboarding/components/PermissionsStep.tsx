import { useMount } from "ahooks";
import { Button, Tag } from "antd";
import type { FC } from "react";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useSnapshot } from "valtio";
import { findPreferenceSectionSettings } from "@/pages/Preference/config/preferenceSchema";
import { commitSettingChange } from "@/pages/Preference/services/preferenceSettings";
import type {
  PreferenceSetting,
  SettingValue,
} from "@/pages/Preference/types/preferences";
import {
  androidState,
  checkAndAutoPromptAndroidPermissions,
  openAndroidPermissionsModal,
} from "@/stores/android";
import { settingsState } from "@/stores/settings";
import type { Settings } from "@/types/settings";
import { cn } from "@/utils/cn";
import { isAndroid } from "@/utils/is";
import type { OnboardingStepProps } from "../types";
import OnboardingPreferenceCard from "./OnboardingPreferenceCard";
import OnboardingStepLayout from "./OnboardingStepLayout";

const PERMISSION_SETTINGS = findPreferenceSectionSettings("permissions");
const DESCRIPTION_PLATFORM = PERMISSION_SETTINGS.some((setting) => {
  return setting.id === "permissions.runAsAdministrator";
})
  ? "windows"
  : "macos";

const PermissionsStep: FC<OnboardingStepProps> = (props) => {
  const { onActionsChange } = props;
  const { t } = useTranslation(["onboarding", "common"]);
  const settings = useSnapshot(settingsState) as Settings;
  const androidSnapshot = useSnapshot(androidState);
  const androidStatus = androidSnapshot.status;
  const modeSelected = androidStatus?.modeSelected ?? false;

  useMount(() => {
    if (isAndroid) {
      void checkAndAutoPromptAndroidPermissions();
    }
  });

  useEffect(() => {
    if (!isAndroid) return;

    onActionsChange?.({
      nextDisabled: !modeSelected,
    });

    return () => {
      onActionsChange?.(null);
    };
  }, [modeSelected, onActionsChange]);

  const handleChange = async (
    setting: PreferenceSetting,
    value: SettingValue,
  ) => {
    await commitSettingChange(setting, value);
  };

  const handleConfigureAndroid = () => {
    openAndroidPermissionsModal();
  };

  if (isAndroid) {
    return (
      <OnboardingStepLayout
        contentClassName="flex flex-col gap-3"
        description={t("common:androidPermissions.description")}
        icon={<i aria-hidden="true" className="i-lucide:shield-check" />}
        title={t("common:androidPermissions.title")}
      >
        <div className="grid gap-2 sm:grid-cols-2">
          <OnboardingModeCard
            description={t("common:androidPermissions.modes.full.description")}
            icon="i-lucide:zap"
            selected={modeSelected && androidStatus?.mode === "full"}
            title={t("common:androidPermissions.modes.full.title")}
          />
          <OnboardingModeCard
            description={t("common:androidPermissions.modes.basic.description")}
            icon="i-lucide:panel-top"
            selected={modeSelected && androidStatus?.mode === "basic"}
            title={t("common:androidPermissions.modes.basic.title")}
          />
        </div>

        <div className="rounded-xl border border-ant-border-secondary bg-ant-fill-quaternary p-3">
          <div className="flex flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex min-w-0 flex-1 flex-col gap-1">
              <span className="font-medium text-ant-text text-sm">
                {modeSelected
                  ? t("common:androidPermissions.onboarding.selected")
                  : t("common:androidPermissions.onboarding.required")}
              </span>
              <span className="text-ant-secondary text-xs">
                {modeSelected
                  ? t("common:androidPermissions.onboarding.changeLater")
                  : t("common:androidPermissions.onboarding.chooseMode")}
              </span>
            </div>
            <Button onClick={handleConfigureAndroid} type="primary">
              {modeSelected
                ? t("common:androidPermissions.actions.reconfigure")
                : t("common:androidPermissions.actions.configure")}
            </Button>
          </div>
        </div>
      </OnboardingStepLayout>
    );
  }

  return (
    <OnboardingStepLayout
      contentClassName="flex flex-col gap-4"
      description={t(
        `onboarding:permissions.description.${DESCRIPTION_PLATFORM}`,
      )}
      icon={<i aria-hidden="true" className="i-lucide:shield-check" />}
      title={t("onboarding:permissions.title")}
    >
      {PERMISSION_SETTINGS.map((setting) => {
        return (
          <OnboardingPreferenceCard
            key={setting.id}
            onChange={handleChange}
            setting={setting}
            settings={settings}
          />
        );
      })}
    </OnboardingStepLayout>
  );
};

interface OnboardingModeCardProps {
  description: string;
  icon: string;
  selected: boolean;
  title: string;
}

const OnboardingModeCard: FC<OnboardingModeCardProps> = (props) => {
  const { description, icon, selected, title } = props;
  const { t } = useTranslation("common");

  return (
    <div
      className={cn(
        "rounded-xl border p-3",
        selected
          ? "border-ant-primary bg-ant-primary-bg"
          : "border-ant-border-secondary bg-ant-container",
      )}
    >
      <div className="flex items-center gap-2">
        <i
          aria-hidden="true"
          className={cn(icon, "text-ant-primary text-lg")}
        />
        <span className="font-medium text-ant-text text-sm">{title}</span>
        {selected && (
          <Tag className="m-0" color="blue">
            {t("androidPermissions.status.current")}
          </Tag>
        )}
      </div>
      <p className="mt-2 mb-0 text-ant-secondary text-xs leading-relaxed">
        {description}
      </p>
    </div>
  );
};

export default PermissionsStep;
