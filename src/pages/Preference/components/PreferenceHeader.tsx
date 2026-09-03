import { Input } from "antd";
import type { ChangeEvent, FC } from "react";
import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { router } from "@/router";
import { openAndroidPermissionsModal } from "@/stores/android";
import { cn } from "@/utils/cn";
import { isAndroid, isMobile } from "@/utils/is";
import { preferenceTabs } from "../config/preferenceSchema";
import { PREFERENCE_TAB_META } from "../constants";
import type {
  PreferenceSection,
  PreferenceTab,
  PreferenceTabId,
} from "../types/preferences";
import {
  translatePreferenceSection,
  translatePreferenceTab,
} from "../utils/preferenceI18n";
import type { PreferenceSearchResult } from "../utils/preferenceSearch";
import PreferenceCountTag from "./PreferenceCountTag";
import PreferenceSearchResults from "./PreferenceSearchResults";

interface PreferenceHeaderProps {
  activeSectionId: string;
  activeTab: PreferenceTab;
  searchQuery: string;
  searchResults: PreferenceSearchResult[];
  shouldReduceMotion: boolean;
  totalSettings: number;
  onPickSearchResult: (result: PreferenceSearchResult) => void;
  onSearchChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onSectionSelect: (sectionId: string) => void;
  onTabSelect: (tabId: PreferenceTabId) => void;
}

/**
 * 偏好窗口主区域头部：标题、全局搜索和二级分组导航。
 */
const PreferenceHeader: FC<PreferenceHeaderProps> = (props) => {
  const { t } = useTranslation(["preferences", "common"]);
  const {
    activeSectionId,
    activeTab,
    searchQuery,
    searchResults,
    shouldReduceMotion,
    totalSettings,
    onPickSearchResult,
    onSearchChange,
    onSectionSelect,
    onTabSelect,
  } = props;
  const mobile = isAndroid || isMobile();
  const activeTabRef = useRef<HTMLButtonElement | null>(null);

  useEffect(() => {
    if (!mobile || activeTabRef.current?.dataset.tabId !== activeTab.id) return;

    activeTabRef.current.scrollIntoView({
      behavior: "auto",
      block: "nearest",
      inline: "start",
    });
  }, [activeTab.id, mobile]);

  const handleBack = () => {
    if (isAndroid) {
      void router.navigate(-1);
      return;
    }

    void router.navigate("/", { replace: true });
  };

  const searchInput = (
    <div className="relative z-3 w-full">
      <Input
        allowClear
        autoCapitalize="off"
        autoCorrect="off"
        className="border-ant-border-secondary bg-ant-fill-quaternary text-ant-text"
        onChange={onSearchChange}
        placeholder={t("preferences:search.placeholder")}
        prefix={
          <i
            aria-hidden="true"
            className="i-lucide:search text-ant-secondary text-base"
          />
        }
        spellCheck={false}
        value={searchQuery}
      />

      <PreferenceSearchResults
        onPick={onPickSearchResult}
        query={searchQuery.trim()}
        results={searchResults}
        shouldReduceMotion={shouldReduceMotion}
      />
    </div>
  );

  return (
    <header
      className={cn(
        "shrink-0 border-ant-border-secondary border-b bg-ant-container",
        mobile ? "mobile-safe-area-top px-3 pb-2" : "px-6 pt-4 pb-2",
      )}
      data-tauri-drag-region
    >
      <div
        className="flex items-center justify-between gap-5"
        data-tauri-drag-region
      >
        <div className="flex min-w-0 items-center gap-2">
          {mobile ? (
            <button
              aria-label={t("common:actions.back", { defaultValue: "返回" })}
              className="flex size-7 shrink-0 cursor-pointer items-center justify-center rounded-lg border border-ant-border-secondary bg-ant-fill-quaternary text-ant-text transition-colors hover:bg-ant-fill"
              onClick={handleBack}
              type="button"
            >
              <i aria-hidden="true" className="i-lucide:arrow-left text-sm" />
            </button>
          ) : null}
          <h1 className="m-0 flex items-center gap-2 font-semibold text-ant-text text-lg leading-snug">
            <i
              aria-hidden="true"
              className={cn(
                "text-ant-primary text-lg",
                PREFERENCE_TAB_META[activeTab.id].icon,
              )}
            />
            <span className="truncate">
              {translatePreferenceTab(t, activeTab)}
            </span>
          </h1>
        </div>

        {!mobile ? (
          <div className="flex w-64 shrink-0 items-center">{searchInput}</div>
        ) : null}
      </div>

      {mobile ? <div className="mt-2">{searchInput}</div> : null}

      {mobile ? (
        <div className="no-scrollbar -mx-3 mt-2 flex gap-1 overflow-x-auto px-3">
          {preferenceTabs.map((tab) => {
            const selected = tab.id === activeTab.id;
            const handleClick = () => {
              onTabSelect(tab.id);
            };

            return (
              <button
                className={cn(
                  "flex h-8 shrink-0 cursor-pointer items-center gap-1.5 rounded-lg border-0 px-2.5 font-medium text-xs transition-colors",
                  selected
                    ? "bg-ant-primary text-ant-light-solid"
                    : "bg-ant-fill-quaternary text-ant-secondary",
                )}
                data-tab-id={tab.id}
                key={tab.id}
                onClick={handleClick}
                ref={selected ? activeTabRef : void 0}
                type="button"
              >
                <i
                  aria-hidden="true"
                  className={PREFERENCE_TAB_META[tab.id].icon}
                />
                <span>{translatePreferenceTab(t, tab)}</span>
              </button>
            );
          })}

          {isAndroid ? (
            <button
              className="flex h-8 shrink-0 cursor-pointer items-center gap-1.5 rounded-lg border border-ant-border-secondary bg-ant-fill-quaternary px-2.5 font-medium text-ant-primary text-xs"
              onClick={openAndroidPermissionsModal}
              type="button"
            >
              <i aria-hidden="true" className="i-lucide:shield-check" />
              <span>{t("common:androidPermissions.title")}</span>
            </button>
          ) : null}
        </div>
      ) : null}

      <SectionTabs
        activeSectionId={activeSectionId}
        mobile={mobile}
        onSectionSelect={onSectionSelect}
        sections={activeTab.sections}
        totalSettings={totalSettings}
      />
    </header>
  );
};

interface SectionTabsProps {
  activeSectionId: string;
  mobile: boolean;
  sections: PreferenceSection[];
  totalSettings: number;
  onSectionSelect: (sectionId: string) => void;
}

/**
 * 偏好页二级分组导航，紧贴标题栏用于快速切换当前分类。
 */
const SectionTabs: FC<SectionTabsProps> = (props) => {
  const { t } = useTranslation(["preferences", "common"]);
  const { activeSectionId, mobile, sections, totalSettings, onSectionSelect } =
    props;

  return (
    <div
      className={cn(
        "mt-2 flex items-center gap-3",
        mobile && "no-scrollbar -mx-3 overflow-x-auto px-3",
      )}
      data-tauri-drag-region
    >
      {sections.map((section) => {
        const selected = section.id === activeSectionId;
        const handleClick = () => {
          onSectionSelect(section.id);
        };

        return (
          <button
            className={cn(
              "relative h-7.5 cursor-pointer whitespace-nowrap border-0 bg-transparent px-0.5 font-medium text-sm transition-colors focus-visible:ring-1 focus-visible:ring-ant-primary motion-reduce:transition-none",
              selected
                ? "text-ant-text"
                : "text-ant-secondary hover:text-ant-text",
            )}
            key={section.id}
            onClick={handleClick}
            type="button"
          >
            {translatePreferenceSection(t, section, "title")}
            <span
              className={cn(
                "absolute right-0 bottom-0 left-0 h-0.5 rounded-full transition-colors motion-reduce:transition-none",
                selected ? "bg-ant-primary" : "bg-transparent",
              )}
            />
          </button>
        );
      })}

      {!mobile ? (
        <PreferenceCountTag className="ml-auto">
          {t("common:units.settings", { count: totalSettings })}
        </PreferenceCountTag>
      ) : null}
    </div>
  );
};

export default PreferenceHeader;
