import { useDebounceFn, useMount } from "ahooks";
import type { MenuProps } from "antd";
import type { ChangeEvent, FC } from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useSnapshot } from "valtio";
import {
  clearClipboardItems,
  createClipboardGroup,
  deleteClipboardGroup,
  listClipboardGroups,
  showWindow,
  updateClipboardGroup,
} from "@/commands";
import ClipboardGroupIcon from "@/components/ClipboardGroupIcon";
import ClipboardGroupPopover, {
  parseGroupIcon,
} from "@/components/ClipboardGroupPopover";
import Dropdown, {
  type AppDropdownProps,
  type DropdownMenuItems,
} from "@/components/Dropdown";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useTauriListen } from "@/hooks/useTauriListen";
import { router } from "@/router";
import { openAndroidPermissionsModal } from "@/stores/android";
import { clipboardViewState } from "@/stores/clipboardView";
import { settingsState } from "@/stores/settings";
import type {
  ClipboardGroupInput,
  ClipboardGroupRecord,
} from "@/types/clipboard";
import { cn } from "@/utils/cn";
import { getModalApi } from "@/utils/feedback";
import { isAndroid, isMobile } from "@/utils/is";
import { formatShortcutDisplay } from "@/utils/shortcut";
import SearchInput from "./SearchInput";
import SyncStatusIcons from "./SyncStatusIcons";

interface WindowVisibilityPayload {
  label: string;
  visible: boolean;
}

interface SearchHandoffPayload {
  sessionId: number;
}

type HeaderMoreMenuKey = "clear" | "preference" | "android_permissions";

const MORE_ACTION_TRIGGER: AppDropdownProps["trigger"] = ["click"];
const PREFERENCE_SHORTCUT = formatShortcutDisplay("CmdOrCtrl+,", " ");

/**
 * 1:1 复刻 Paste 官方顶部栏：
 * 左侧 🔍 极简搜索，中间绝对居中 Pinboards（剪贴板胶囊 + 收藏 + 自定义画板 + 新建），右侧极简 `...` 更多。
 */
const Header: FC = () => {
  const { t } = useTranslation("clipboard");
  const settings = useSnapshot(settingsState);
  const snapshot = useSnapshot(clipboardViewState);
  const [searchOpen, setSearchOpen] = useState(false);
  const [customGroups, setCustomGroups] = useState<ClipboardGroupRecord[]>([]);
  const [popoverOpen, setPopoverOpen] = useState(false);
  const [popoverGroup, setPopoverGroup] = useState<ClipboardGroupRecord | null>(
    null,
  );
  const [popoverMode, setPopoverMode] = useState<"create" | "edit">("create");

  const [searchBlurToken, setSearchBlurToken] = useState(0);
  const [searchClearToken, setSearchClearToken] = useState(0);
  const [searchFocusCursor, setSearchFocusCursor] = useState<"auto" | "end">(
    "auto",
  );
  const [searchFocusToken, setSearchFocusToken] = useState(0);
  const [searchHandoffSessionId, setSearchHandoffSessionId] = useState<
    number | null
  >(null);
  const mobileGroupsRef = useRef<HTMLDivElement>(null);

  const { groupId, range } = snapshot;

  const loadGroups = async () => {
    const groups = await listClipboardGroups();
    setCustomGroups((groups || []).filter((g) => !g.isHidden));
  };

  useMount(() => {
    void loadGroups();
  });

  useTauriListen(TAURI_EVENT.CLIPBOARD_GROUPS_UPDATED, () => {
    void loadGroups();
  });

  const handleOpenPreference = () => {
    if (isAndroid) {
      void router.navigate("/preference");
      return Promise.resolve();
    }
    return showWindow(WINDOW_LABEL.PREFERENCE);
  };

  const handleClearClipboardItems = async () => {
    await clearClipboardItems();
  };

  const handleMoreMenuClick: MenuProps["onClick"] = async (info) => {
    const key = info.key as HeaderMoreMenuKey;
    if (key === "clear") {
      await handleClearClipboardItems();
      return;
    }
    if (key === "android_permissions") {
      openAndroidPermissionsModal();
      return;
    }
    await handleOpenPreference();
  };

  const [searchValue, setSearchValue] = useState("");

  const { cancel: cancelKeywordChange, run: debouncedSetKeyword } =
    useDebounceFn(
      (val: string) => {
        clipboardViewState.keyword = val;
      },
      { wait: 200 },
    );

  const handleKeywordChange = (event: ChangeEvent<HTMLInputElement>) => {
    const val = event.target.value;
    setSearchValue(val);
    debouncedSetKeyword(val.trim());
  };

  const clearSearch = () => {
    cancelKeywordChange();
    setSearchValue("");
    clipboardViewState.keyword = "";
    setSearchClearToken((c) => c + 1);
  };

  const blurSearch = () => {
    setSearchBlurToken((c) => c + 1);
  };

  const focusSearch = () => {
    setSearchOpen(true);
    setSearchFocusCursor("auto");
    setSearchFocusToken((c) => c + 1);
  };

  useMount(() => {
    void loadGroups();

    const handleTypeToSearch = (event: Event) => {
      const activeEl = document.activeElement;
      if (
        activeEl &&
        (activeEl.tagName === "INPUT" ||
          activeEl.tagName === "TEXTAREA" ||
          (activeEl as HTMLElement).isContentEditable)
      ) {
        return;
      }

      const customEvent = event as CustomEvent<{ key?: string }>;
      const key = customEvent.detail?.key;
      if (!key) return;

      setSearchOpen(true);
      setSearchValue((prev) => {
        const next = prev ? prev + key : key;
        debouncedSetKeyword(next.trim());
        return next;
      });
      setSearchFocusCursor("end");
      setSearchFocusToken((c) => c + 1);
    };

    window.addEventListener("ecopaste:type-to-search", handleTypeToSearch);
    return () => {
      window.removeEventListener("ecopaste:type-to-search", handleTypeToSearch);
    };
  });

  const handleWindowVisibility = (event: {
    payload: WindowVisibilityPayload;
  }) => {
    const { label, visible } = event.payload;
    if (label !== WINDOW_LABEL.CLIPBOARD) return;

    if (!visible) {
      blurSearch();
      if (settings.clipboard.search.clearOnHide) {
        clearSearch();
        setSearchOpen(false);
      }
      return;
    }

    if (settings.clipboard.search.clearOnHide) {
      clearSearch();
    }

    if (settings.clipboard.search.defaultFocus) {
      focusSearch();
    }
  };

  useTauriListen<WindowVisibilityPayload>(
    TAURI_EVENT.WINDOW_VISIBILITY,
    handleWindowVisibility,
  );

  const handleSearchHandoff = (event: { payload: SearchHandoffPayload }) => {
    setSearchOpen(true);
    setSearchFocusCursor("end");
    setSearchHandoffSessionId(event.payload.sessionId);
    setSearchFocusToken((current) => current + 1);
  };

  useTauriListen<SearchHandoffPayload>(
    TAURI_EVENT.KEYBOARD_SEARCH_HANDOFF,
    handleSearchHandoff,
  );

  const selectAllHistory = () => {
    clipboardViewState.range = "all";
    clipboardViewState.groupId = null;
    clipboardViewState.category = null;
  };

  const selectFavoriteHistory = () => {
    clipboardViewState.range = "favorite";
    clipboardViewState.groupId = null;
    clipboardViewState.category = null;
  };

  const selectCustomGroup = (id: string) => {
    clipboardViewState.range = "all";
    clipboardViewState.groupId = id;
    clipboardViewState.category = null;
  };

  const handleOpenCreateGroup = () => {
    setPopoverGroup(null);
    setPopoverMode("create");
    setPopoverOpen(true);
  };

  const handleEditGroup = (group: ClipboardGroupRecord) => {
    setPopoverGroup(group);
    setPopoverMode("edit");
    setPopoverOpen(true);
  };

  const handleDeleteGroup = (group: ClipboardGroupRecord) => {
    getModalApi().confirm({
      centered: true,
      content: t("groups.deleteConfirmContent", {
        defaultValue:
          "删除画板后，画板内的剪贴板记录不会被删除，将保留在全部历史中。",
      }),
      okButtonProps: { danger: true },
      okText: t("common:confirm.delete", { defaultValue: "删除" }),
      onOk: async () => {
        await deleteClipboardGroup(group.id);
        if (clipboardViewState.groupId === group.id) {
          selectAllHistory();
        }
        await loadGroups();
      },
      title: t("groups.deleteConfirmTitle", {
        defaultValue: `确定删除画板「${group.name}」吗？`,
        name: group.name,
      }),
    });
  };

  const handleGroupPopoverSubmit = async (input: ClipboardGroupInput) => {
    if (popoverMode === "edit" && popoverGroup) {
      await updateClipboardGroup(popoverGroup.id, input);
    } else {
      await createClipboardGroup(input);
    }
    setPopoverOpen(false);
    setPopoverGroup(null);
    await loadGroups();
  };

  const moreMenuItems: DropdownMenuItems = [
    ...(isAndroid
      ? [
          {
            icon: "i-lucide:smartphone",
            key: "android_permissions",
            label: "Android 权限与引擎配置",
          },
        ]
      : []),
    {
      extra: PREFERENCE_SHORTCUT,
      icon: "i-lucide:settings",
      key: "preference",
      label: t("header.openPreference"),
    },
    {
      danger: true,
      icon: "i-lucide:trash-2",
      key: "clear",
      label: t("header.clearRecords"),
    },
  ];

  const isAllActive = range === "all" && groupId === null;
  const isFavoriteActive = range === "favorite" && groupId === null;

  // biome-ignore lint/correctness/useExhaustiveDependencies: 分类状态变化后需要滚动到本次渲染产生的选中按钮。
  useEffect(() => {
    const container = mobileGroupsRef.current;
    const active = container?.querySelector<HTMLElement>(
      '[aria-current="page"]',
    );
    active?.scrollIntoView({
      behavior: "smooth",
      block: "nearest",
      inline: "center",
    });
  }, [groupId, range, snapshot.category]);

  if (isMobile()) {
    return (
      <div className="flex shrink-0 select-none flex-col gap-2 px-3 pt-1 pb-2">
        <div className="flex h-10 items-center justify-between gap-2">
          <div className="flex min-w-0 items-center gap-2">
            <img
              alt=""
              className="size-8 shrink-0 object-contain"
              draggable={false}
              src="/logo.png"
            />
            <span className="truncate font-semibold text-ant-text text-lg">
              EcoPaste
            </span>
          </div>

          <div className="flex shrink-0 items-center gap-1">
            <SyncStatusIcons compact />

            <Dropdown
              menu={{ items: moreMenuItems, onClick: handleMoreMenuClick }}
              trigger={MORE_ACTION_TRIGGER}
            >
              <button
                className="flex size-9 cursor-pointer items-center justify-center rounded-xl bg-ant-container text-ant-secondary shadow-xs transition-colors hover:bg-ant-fill-tertiary"
                type="button"
              >
                <i className="i-lucide:more-vertical size-4" />
              </button>
            </Dropdown>
          </div>
        </div>

        <SearchInput
          allowClear
          blurToken={searchBlurToken}
          className="w-full"
          clearToken={searchClearToken}
          focusCursor={searchFocusCursor}
          focusToken={searchFocusToken}
          handoffSessionId={searchHandoffSessionId}
          onChange={handleKeywordChange}
          placeholder="搜索剪贴板历史..."
          size="middle"
          value={searchValue}
        />

        {/* 分类与画板横向滑块 */}
        <div
          className="no-scrollbar -mx-3 flex items-center gap-1.5 overflow-x-auto px-3 py-0.5"
          ref={mobileGroupsRef}
        >
          {/* 全部 */}
          <button
            aria-current={isAllActive && !snapshot.category ? "page" : void 0}
            className={cn(
              "flex shrink-0 cursor-pointer items-center gap-1.5 rounded-full px-3.5 py-1.5 font-medium text-xs transition-all",
              isAllActive && !snapshot.category
                ? "bg-[#007AFF] text-white shadow-xs"
                : "bg-white text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300",
            )}
            onClick={selectAllHistory}
            type="button"
          >
            <i className="i-lucide:clock size-3.5" />
            <span>全部</span>
          </button>

          {/* 收藏 */}
          <button
            aria-current={isFavoriteActive ? "page" : void 0}
            className={cn(
              "flex shrink-0 cursor-pointer items-center gap-1.5 rounded-full px-3.5 py-1.5 font-medium text-xs transition-all",
              isFavoriteActive
                ? "bg-[#007AFF] text-white shadow-xs"
                : "bg-white text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300",
            )}
            onClick={selectFavoriteHistory}
            type="button"
          >
            <i className="i-lucide:star size-3.5 fill-current" />
            <span>收藏</span>
          </button>

          {/* 文本 */}
          <button
            aria-current={
              snapshot.category === "text" && !groupId ? "page" : void 0
            }
            className={cn(
              "flex shrink-0 cursor-pointer items-center gap-1.5 rounded-full px-3.5 py-1.5 font-medium text-xs transition-all",
              snapshot.category === "text" && !groupId
                ? "bg-[#007AFF] text-white shadow-xs"
                : "bg-white text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300",
            )}
            onClick={() => {
              clipboardViewState.range = "all";
              clipboardViewState.groupId = null;
              clipboardViewState.category = "text";
            }}
            type="button"
          >
            <i className="i-lucide:type size-3.5" />
            <span>文本</span>
          </button>

          {/* 图片 */}
          <button
            aria-current={
              snapshot.category === "image" && !groupId ? "page" : void 0
            }
            className={cn(
              "flex shrink-0 cursor-pointer items-center gap-1.5 rounded-full px-3.5 py-1.5 font-medium text-xs transition-all",
              snapshot.category === "image" && !groupId
                ? "bg-[#007AFF] text-white shadow-xs"
                : "bg-white text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300",
            )}
            onClick={() => {
              clipboardViewState.range = "all";
              clipboardViewState.groupId = null;
              clipboardViewState.category = "image";
            }}
            type="button"
          >
            <i className="i-lucide:image size-3.5" />
            <span>图片</span>
          </button>

          {/* 文件 */}
          <button
            aria-current={
              snapshot.category === "files" && !groupId ? "page" : void 0
            }
            className={cn(
              "flex shrink-0 cursor-pointer items-center gap-1.5 rounded-full px-3.5 py-1.5 font-medium text-xs transition-all",
              snapshot.category === "files" && !groupId
                ? "bg-[#007AFF] text-white shadow-xs"
                : "bg-white text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300",
            )}
            onClick={() => {
              clipboardViewState.range = "all";
              clipboardViewState.groupId = null;
              clipboardViewState.category = "files";
            }}
            type="button"
          >
            <i className="i-lucide:folder size-3.5" />
            <span>文件</span>
          </button>

          {/* 自定义画板 */}
          {customGroups.map((group) => {
            const isCurrent = groupId === group.id;
            const { color, icon } = parseGroupIcon(group.icon);
            return (
              <button
                aria-current={isCurrent ? "page" : void 0}
                className={cn(
                  "flex shrink-0 cursor-pointer items-center gap-1.5 rounded-full px-3.5 py-1.5 font-medium text-xs transition-all",
                  isCurrent
                    ? "bg-[#007AFF] text-white shadow-xs"
                    : "bg-white text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300",
                )}
                key={group.id}
                onClick={() => selectCustomGroup(group.id)}
                type="button"
              >
                {color ? (
                  <span
                    className="size-2 shrink-0 rounded-full"
                    style={{ backgroundColor: color }}
                  />
                ) : null}
                {icon ? (
                  <ClipboardGroupIcon
                    className="size-3.5 shrink-0 text-current"
                    icon={icon}
                  />
                ) : null}
                <span>{group.name}</span>
              </button>
            );
          })}
        </div>
      </div>
    );
  }

  return (
    <div
      className="relative flex h-11 shrink-0 select-none items-center justify-between px-5"
      data-tauri-drag-region
    >
      {/* 左侧：保持通透拖拽区 */}
      <div className="w-8 shrink-0" />

      {/* 中间：搜索与 Pinboard 画板组（整体水平居中，1:1 完全对齐 Paste） */}
      <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
        {searchOpen ? (
          <div className="pointer-events-auto z-20 flex items-center gap-2">
            <SearchInput
              allowClear
              blurToken={searchBlurToken}
              className="w-64"
              clearToken={searchClearToken}
              focusCursor={searchFocusCursor}
              focusToken={searchFocusToken}
              handoffSessionId={searchHandoffSessionId}
              onChange={handleKeywordChange}
              placeholder={t("header.searchPlaceholder")}
              size="small"
              value={searchValue}
            />
            <button
              className="cursor-pointer px-2.5 py-1 text-neutral-500 text-xs hover:text-neutral-900"
              onClick={() => {
                clearSearch();
                setSearchOpen(false);
              }}
              type="button"
            >
              取消
            </button>
          </div>
        ) : (
          <div className="pointer-events-auto flex select-none items-center gap-2">
            {/* 搜索纯图标按钮（无外层灰圈，1:1 对齐 Paste） */}
            <button
              className="mr-1 flex size-6 cursor-pointer items-center justify-center text-neutral-800 transition-opacity hover:opacity-70 dark:text-white/80"
              onClick={focusSearch}
              title="搜索 (⌘F)"
              type="button"
            >
              <i className="i-lucide:search size-4 stroke-[1.75]" />
            </button>

            {/* 全部历史（剪贴板胶囊） */}
            <Dropdown
              menu={{
                items: [
                  {
                    danger: true,
                    icon: "i-lucide:trash-2",
                    key: "clearHistory",
                    label: "清空历史记录...",
                    onClick: handleClearClipboardItems,
                  },
                ],
              }}
              trigger={["contextMenu"]}
            >
              <button
                className={cn(
                  "flex cursor-pointer items-center gap-1.5 rounded-full px-3 py-1 font-medium text-[13px] transition-all",
                  isAllActive
                    ? "bg-[#E5E7EB] text-[#111827] dark:bg-white/20 dark:text-white"
                    : "text-neutral-700 hover:bg-black/[0.04] hover:text-neutral-900 dark:text-neutral-300",
                )}
                onClick={selectAllHistory}
                type="button"
              >
                <i
                  className={cn(
                    "i-lucide:history size-3.5 stroke-[1.75]",
                    isAllActive
                      ? "text-[#4B5563] dark:text-white/80"
                      : "text-neutral-400 dark:text-white/60",
                  )}
                />
                <span>剪贴板</span>
              </button>
            </Dropdown>

            {/* 收藏 */}
            <Dropdown
              menu={{
                items: [
                  {
                    danger: true,
                    icon: "i-lucide:trash-2",
                    key: "clearFavorites",
                    label: "清空历史记录...",
                    onClick: handleClearClipboardItems,
                  },
                ],
              }}
              trigger={["contextMenu"]}
            >
              <button
                className={cn(
                  "flex cursor-pointer items-center gap-1.5 rounded-full px-3 py-1 font-medium text-[13px] transition-all",
                  isFavoriteActive
                    ? "bg-[#E5E7EB] text-[#111827] dark:bg-white/20 dark:text-white"
                    : "text-neutral-700 hover:bg-black/[0.04] hover:text-neutral-900 dark:text-neutral-300",
                )}
                onClick={selectFavoriteHistory}
                type="button"
              >
                <i
                  className={cn(
                    "i-lucide:star size-3.5 stroke-[1.75]",
                    isFavoriteActive
                      ? "fill-amber-500 text-amber-500 dark:fill-amber-400 dark:text-amber-400"
                      : "text-neutral-400 dark:text-white/60",
                  )}
                />
                <span>收藏</span>
              </button>
            </Dropdown>

            {/* 自定义画板 */}
            {customGroups.map((group) => {
              const isCurrent = groupId === group.id;
              const { color, icon } = parseGroupIcon(group.icon);
              const isEditingThis =
                popoverOpen &&
                popoverMode === "edit" &&
                popoverGroup?.id === group.id;

              const groupMenuItems: DropdownMenuItems = [
                {
                  icon: "i-lucide:edit-3",
                  key: "edit",
                  label: "编辑画板",
                  onClick: () => handleEditGroup(group),
                },
                {
                  danger: true,
                  icon: "i-lucide:trash-2",
                  key: "delete",
                  label: "删除画板",
                  onClick: () => handleDeleteGroup(group),
                },
              ];

              const tabButton = (
                <button
                  className={cn(
                    "flex cursor-pointer items-center gap-1.5 rounded-full px-3 py-1 font-medium text-[13px] transition-all",
                    isCurrent
                      ? "bg-[#E5E7EB] text-[#111827] dark:bg-white/20 dark:text-white"
                      : "text-neutral-700 hover:bg-black/[0.04] hover:text-neutral-900 dark:text-neutral-300",
                  )}
                  onClick={() => selectCustomGroup(group.id)}
                  type="button"
                >
                  {color ? (
                    <span
                      className="size-2.5 shrink-0 rounded-full transition-colors"
                      style={{ backgroundColor: color }}
                    />
                  ) : null}
                  {icon ? (
                    <ClipboardGroupIcon
                      className="size-3.5 shrink-0 text-current"
                      icon={icon}
                    />
                  ) : null}
                  <span>{group.name}</span>
                </button>
              );

              return (
                <ClipboardGroupPopover
                  group={group}
                  key={group.id}
                  mode="edit"
                  onClose={() => {
                    setPopoverOpen(false);
                    setPopoverGroup(null);
                  }}
                  onSubmit={handleGroupPopoverSubmit}
                  open={isEditingThis}
                >
                  <Dropdown
                    menu={{ items: groupMenuItems }}
                    trigger={["contextMenu"]}
                  >
                    {tabButton}
                  </Dropdown>
                </ClipboardGroupPopover>
              );
            })}

            {/* 新建画板加号 */}
            <ClipboardGroupPopover
              group={null}
              mode="create"
              onClose={() => {
                setPopoverOpen(false);
                setPopoverGroup(null);
              }}
              onSubmit={handleGroupPopoverSubmit}
              open={popoverOpen && popoverMode === "create"}
            >
              <button
                className="ml-1 flex size-6 cursor-pointer items-center justify-center text-neutral-800 transition-opacity hover:opacity-70 dark:text-white/80"
                onClick={handleOpenCreateGroup}
                title="新建画板"
                type="button"
              >
                <i className="i-lucide:plus size-4 stroke-[1.75]" />
              </button>
            </ClipboardGroupPopover>
          </div>
        )}
      </div>

      {/* 右侧：极简 `...` 更多操作 */}
      <div className="z-10 flex items-center gap-1">
        <SyncStatusIcons />
        <Dropdown
          menu={{ items: moreMenuItems, onClick: handleMoreMenuClick }}
          tooltip={t("header.moreActions")}
          trigger={MORE_ACTION_TRIGGER}
        >
          <button
            className="flex size-7 cursor-pointer items-center justify-center rounded-full text-neutral-600 transition-colors hover:bg-black/5 hover:text-neutral-900 dark:text-white/70 dark:hover:bg-white/10 dark:hover:text-white"
            type="button"
          >
            <i className="i-lucide:more-horizontal size-4" />
          </button>
        </Dropdown>
      </div>
    </div>
  );
};

export default Header;
