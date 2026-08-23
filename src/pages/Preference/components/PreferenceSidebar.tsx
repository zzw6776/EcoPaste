import type { FC } from "react";
import { useTranslation } from "react-i18next";
import type { StorageUsage } from "@/commands";
import { openAndroidPermissionsModal } from "@/stores/android";
import { cn } from "@/utils/cn";
import { isAndroid, isMac, isMobile } from "@/utils/is";
import { preferenceTabs } from "../config/preferenceSchema";
import { APP_NAME_PLACEHOLDER, PREFERENCE_TAB_META } from "../constants";
import type {
  PreferenceStorageState,
  PreferenceTabId,
} from "../types/preferences";
import { translatePreferenceTab } from "../utils/preferenceI18n";
import PreferenceStorageUsagePanel from "./PreferenceStorageUsagePanel";

interface PreferenceSidebarProps {
  activeTabId: PreferenceTabId;
  appName: string;
  appVersion: string;
  storageState: PreferenceStorageState;
  storageUsage: StorageUsage | null;
  onTabSelect: (tabId: PreferenceTabId) => void;
}

/**
 * 偏好窗口左侧导航栏：展示应用身份、一级分类和本地存储概览。
 */
const PreferenceSidebar: FC<PreferenceSidebarProps> = (props) => {
  const { t } = useTranslation("preferences");
  const {
    activeTabId,
    appName,
    appVersion,
    storageState,
    storageUsage,
    onTabSelect,
  } = props;
  const isCompact = isAndroid || isMobile();
  const appNameLabel = appName.length > 0 ? appName : APP_NAME_PLACEHOLDER;
  const appVersionLabel = appVersion.length > 0 ? `v${appVersion}` : "";

  return (
    <aside
      className={cn(
        "flex flex-col border-ant-border-secondary border-r bg-ant-container",
        isCompact ? "w-14 shrink-0 items-center px-1" : "w-56 shrink-0",
      )}
      data-tauri-drag-region
    >
      <div
        className={cn("flex items-center gap-2 px-4 py-4", {
          "justify-center p-2": isCompact,
          "pt-10": isMac,
        })}
      >
        <img
          alt=""
          className={cn(
            "shrink-0 object-contain",
            isCompact ? "size-7" : "size-10",
          )}
          draggable={false}
          src="/logo.png"
        />
        {!isCompact && (
          <div className="flex h-full flex-col justify-between">
            <div className="font-semibold text-ant-text text-base leading-none">
              {appNameLabel}
            </div>
            {appVersionLabel.length > 0 && (
              <div className="text-ant-tertiary text-xs">{appVersionLabel}</div>
            )}
          </div>
        )}
      </div>

      <nav
        className={cn(
          "flex flex-1 flex-col gap-0.5 pb-3",
          isCompact ? "w-full px-1" : "px-3",
        )}
        data-tauri-drag-region
      >
        {preferenceTabs.map((tab) => {
          const meta = PREFERENCE_TAB_META[tab.id];
          const selected = tab.id === activeTabId;
          const handleClick = () => {
            onTabSelect(tab.id);
          };

          return (
            <button
              className={cn(
                "group relative flex h-10 w-full cursor-pointer items-center rounded-1.75 border-0 bg-transparent text-left transition-colors focus-visible:ring-1 focus-visible:ring-ant-primary motion-reduce:transition-none",
                isCompact ? "justify-center px-0" : "gap-2 px-2",
                selected
                  ? meta.activeClass
                  : "text-ant-secondary hover:bg-ant-fill-tertiary hover:text-ant-text",
              )}
              key={tab.id}
              onClick={handleClick}
              title={isCompact ? translatePreferenceTab(t, tab) : void 0}
              type="button"
            >
              <span
                className={cn(
                  "flex size-7.5 shrink-0 items-center justify-center text-lg transition-colors motion-reduce:transition-none",
                  selected
                    ? "text-ant-primary"
                    : "text-ant-tertiary group-hover:text-ant-secondary",
                )}
              >
                <i aria-hidden="true" className={meta.icon} />
              </span>
              {!isCompact && (
                <>
                  <span className="min-w-0 flex-1 truncate font-medium text-sm leading-tight">
                    {translatePreferenceTab(t, tab)}
                  </span>
                  <span
                    className={cn(
                      "h-5 w-0.75 rounded-full transition-colors motion-reduce:transition-none",
                      selected ? "bg-ant-primary" : "bg-transparent",
                    )}
                  />
                </>
              )}
            </button>
          );
        })}
      </nav>

      {isAndroid && (
        <div className="mt-auto w-full p-2">
          <button
            className="flex w-full cursor-pointer items-center justify-center gap-1.5 rounded-lg border border-ant-border-secondary bg-ant-fill-quaternary p-2 text-ant-primary text-xs transition-colors hover:bg-ant-fill"
            onClick={openAndroidPermissionsModal}
            title={isCompact ? "Android 权限与引擎设置" : void 0}
            type="button"
          >
            <i className="i-lucide:smartphone text-sm" />
            {!isCompact && <span>Android 权限设置</span>}
          </button>
        </div>
      )}

      {!isCompact && (
        <PreferenceStorageUsagePanel
          state={storageState}
          storageUsage={storageUsage}
        />
      )}
    </aside>
  );
};

export default PreferenceSidebar;
