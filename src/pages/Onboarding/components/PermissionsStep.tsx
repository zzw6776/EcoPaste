import { useInterval, useMount } from "ahooks";
import { Button, Tag } from "antd";
import type { FC } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useSnapshot } from "valtio";
import {
  type AndroidPermissionsStatus,
  getAndroidPermissionsStatus,
  requestAndroidPermission,
} from "@/commands/android";
import { findPreferenceSectionSettings } from "@/pages/Preference/config/preferenceSchema";
import { commitSettingChange } from "@/pages/Preference/services/preferenceSettings";
import type {
  PreferenceSetting,
  SettingValue,
} from "@/pages/Preference/types/preferences";
import { settingsState } from "@/stores/settings";
import type { Settings } from "@/types/settings";
import { isAndroid } from "@/utils/is";
import OnboardingPreferenceCard from "./OnboardingPreferenceCard";
import OnboardingStepLayout from "./OnboardingStepLayout";

const PERMISSION_SETTINGS = findPreferenceSectionSettings("permissions");
const DESCRIPTION_PLATFORM = PERMISSION_SETTINGS.some((setting) => {
  return setting.id === "permissions.runAsAdministrator";
})
  ? "windows"
  : "macos";

const PermissionsStep: FC = () => {
  const { t } = useTranslation("onboarding");
  const settings = useSnapshot(settingsState) as Settings;
  const [androidStatus, setAndroidStatus] =
    useState<AndroidPermissionsStatus | null>(null);

  const fetchAndroidStatus = async () => {
    if (!isAndroid) return;
    try {
      const res = await getAndroidPermissionsStatus();
      setAndroidStatus(res);
    } catch {
      // ignore
    }
  };

  useMount(() => {
    void fetchAndroidStatus();
  });

  useInterval(() => {
    if (isAndroid) {
      void fetchAndroidStatus();
    }
  }, 1500);

  const handleChange = async (
    setting: PreferenceSetting,
    value: SettingValue,
  ) => {
    await commitSettingChange(setting, value);
  };

  const handleRequestAndroid = async (
    kind: "overlay" | "accessibility" | "battery" | "notification",
  ) => {
    await requestAndroidPermission(kind);
    void fetchAndroidStatus();
  };

  if (isAndroid) {
    return (
      <OnboardingStepLayout
        contentClassName="flex flex-col gap-2.5"
        description="请授权以下系统权限以启用后台剪贴板捕获与底角上滑手势呼出："
        icon={<i aria-hidden="true" className="i-lucide:shield-check" />}
        title="系统权限配置"
      >
        {/* 1. 悬浮窗 */}
        <div className="flex items-center justify-between rounded-xl border border-ant-border-secondary bg-ant-container p-3 shadow-xs">
          <div className="flex flex-col gap-0.5">
            <div className="flex items-center gap-1.5 font-medium text-xs">
              <span>悬浮窗权限 (Overlay)</span>
              {androidStatus?.overlayGranted ? (
                <Tag color="success">已开启</Tag>
              ) : (
                <Tag color="warning">待开启</Tag>
              )}
            </div>
            <span className="text-[11px] text-ant-secondary">
              用于屏幕底角向上滑动手势唤起抽屉
            </span>
          </div>
          <Button
            disabled={androidStatus?.overlayGranted}
            onClick={() => void handleRequestAndroid("overlay")}
            size="small"
            type={androidStatus?.overlayGranted ? "default" : "primary"}
          >
            {androidStatus?.overlayGranted ? "已就绪" : "去授权"}
          </Button>
        </div>

        {/* 2. 无障碍 */}
        <div className="flex items-center justify-between rounded-xl border border-ant-border-secondary bg-ant-container p-3 shadow-xs">
          <div className="flex flex-col gap-0.5">
            <div className="flex items-center gap-1.5 font-medium text-xs">
              <span>无障碍服务 (Accessibility)</span>
              {androidStatus?.accessibilityGranted ? (
                <Tag color="success">已开启</Tag>
              ) : (
                <Tag color="warning">待开启</Tag>
              )}
            </div>
            <span className="text-[11px] text-ant-secondary">
              用于后台自动采集剪贴板与模拟自动粘贴
            </span>
          </div>
          <Button
            disabled={androidStatus?.accessibilityGranted}
            onClick={() => void handleRequestAndroid("accessibility")}
            size="small"
            type={androidStatus?.accessibilityGranted ? "default" : "primary"}
          >
            {androidStatus?.accessibilityGranted ? "已就绪" : "去开启"}
          </Button>
        </div>

        {/* 3. 电池优化 */}
        <div className="flex items-center justify-between rounded-xl border border-ant-border-secondary bg-ant-container p-3 shadow-xs">
          <div className="flex flex-col gap-0.5">
            <div className="flex items-center gap-1.5 font-medium text-xs">
              <span>忽略电池优化 (后台保活)</span>
              {androidStatus?.batteryIgnored ? (
                <Tag color="success">已加白</Tag>
              ) : (
                <Tag color="default">建议开启</Tag>
              )}
            </div>
            <span className="text-[11px] text-ant-secondary">
              防止手机锁屏或切换后台时杀死剪贴板服务
            </span>
          </div>
          <Button
            disabled={androidStatus?.batteryIgnored}
            onClick={() => void handleRequestAndroid("battery")}
            size="small"
          >
            {androidStatus?.batteryIgnored ? "已就绪" : "去加白"}
          </Button>
        </div>

        {/* 4. 通知权限 */}
        <div className="flex items-center justify-between rounded-xl border border-ant-border-secondary bg-ant-container p-3 shadow-xs">
          <div className="flex flex-col gap-0.5">
            <div className="flex items-center gap-1.5 font-medium text-xs">
              <span>通知权限</span>
              {androidStatus?.notificationGranted ? (
                <Tag color="success">已允许</Tag>
              ) : (
                <Tag color="default">建议允许</Tag>
              )}
            </div>
            <span className="text-[11px] text-ant-secondary">
              用于显示常驻前台服务通知以保活
            </span>
          </div>
          <Button
            disabled={androidStatus?.notificationGranted}
            onClick={() => void handleRequestAndroid("notification")}
            size="small"
          >
            {androidStatus?.notificationGranted ? "已允许" : "去允许"}
          </Button>
        </div>
      </OnboardingStepLayout>
    );
  }

  return (
    <OnboardingStepLayout
      contentClassName="flex flex-col gap-4"
      description={t(`permissions.description.${DESCRIPTION_PLATFORM}`)}
      icon={<i aria-hidden="true" className="i-lucide:shield-check" />}
      title={t("permissions.title")}
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

export default PermissionsStep;
