import { useEventListener, useMount } from "ahooks";
import { Empty, Spin } from "antd";
import type { TFunction } from "i18next";
import type {
  FC,
  MouseEvent as ReactMouseEvent,
  PointerEvent as ReactPointerEvent,
} from "react";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Virtuoso, type VirtuosoHandle } from "react-virtuoso";
import { useSnapshot } from "valtio";
import {
  deleteClipboardItem,
  getClipboardPreviewPayload,
  getSyncItemStatuses,
  hideWindow,
  listClipboardGroups,
  openAndroidClipboardFile,
  openClipboardItemLink,
  pasteClipboardItem,
  revealClipboardItem,
  type SyncItemStatus,
  saveAndroidClipboardFile,
  saveClipboardImageToFile,
  toggleClipboardItemFavorite,
  toggleClipboardItemPinned,
  updateClipboardItemGroup,
  writeToClipboard,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { buildItemActionLabels } from "@/constants/itemActions";
import {
  parseWindowOpenGroupId,
  WINDOW_OPEN_SELECTION_ALL,
  WINDOW_OPEN_SELECTION_PRESERVE,
} from "@/constants/windowOpenSelection";
import { WINDOW_LABEL } from "@/constants/windows";
import { useClipboardItems } from "@/hooks/useClipboardItems";
import { useKeyboardEvent } from "@/hooks/useKeyboardEvent";
import { useTauriListen } from "@/hooks/useTauriListen";
import { clipboardStatsState } from "@/stores/clipboardStats";
import { clipboardViewState } from "@/stores/clipboardView";
import { settingsState } from "@/stores/settings";
import type {
  ClipboardAction,
  ClipboardGroupRecord,
  ClipboardItem,
  ClipboardKind,
  ClipboardRange,
  FileEntry,
} from "@/types/clipboard";
import type { ItemAction } from "@/types/settings";
import { cn } from "@/utils/cn";
import { isAndroid, isMac, isMobile, isTauri } from "@/utils/is";
import type { WindowVisibilityPayload } from "../hooks/previewController";
import {
  isSpaceKey,
  useClipboardPreviewController,
} from "../hooks/useClipboardPreviewController";
import AndroidFileDetailsDrawer from "./AndroidFileDetailsDrawer";
import ClipboardCard from "./cards/ClipboardCard";
import NoteModal from "./NoteModal";

/** 前 10 项的快捷键：index 0-8 对应 1-9，index 9 对应 0 */
const KEY_HINTS = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "0"];

interface ClipboardUpdatedPayload {
  cleanup?: number;
  deduplicated?: boolean;
  id?: string;
  imported?: boolean;
  kind?: ClipboardKind;
  reconciled?: boolean;
}

interface ClipboardMenuActionPayload {
  action: ClipboardAction;
  groupId?: string;
  itemId: string;
}

/**
 * 剪贴板历史列表：虚拟滚动 + 分类型卡片 + 可视范围分页加载，
 * 跟随关键词（Header 已防抖）检索。
 */
const List: FC = () => {
  const { t } = useTranslation("clipboard");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [firstVisibleIndex, setFirstVisibleIndex] = useState(0);
  const [isModifierPressed, setIsModifierPressed] = useState(false);
  const [customGroups, setCustomGroups] = useState<ClipboardGroupRecord[]>([]);
  const [noteTarget, setNoteTarget] = useState<ClipboardItem | null>(null);
  const [fileDetailsTarget, setFileDetailsTarget] =
    useState<ClipboardItem | null>(null);
  const [fileDetailsEntries, setFileDetailsEntries] = useState<FileEntry[]>([]);
  const [fileActionKey, setFileActionKey] = useState<string | null>(null);
  const [syncStatuses, setSyncStatuses] = useState(
    () => new Map<string, SyncItemStatus>(),
  );
  const listContainerRef = useRef<HTMLDivElement>(null);
  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const isAtTopRef = useRef(true);
  const itemElementMapRef = useRef(new Map<string, HTMLDivElement>());
  const fileDetailsRequestRef = useRef(0);
  const closePreviewRef = useRef<(reason: string) => void>(() => {});
  const displaySettingsMountedRef = useRef(false);
  const keywordRef = useRef("");
  const reloadCurrentRangeRef = useRef<() => void>(() => {});
  const deferredReloadRef = useRef(false);
  // 剪贴板窗口启动即隐藏，初值取 false；首个 `window://visibility` show 事件会翻正。
  // 隐藏期间在 WebView 内预刷新当前可视范围，避免 show 后才用新数据挤开旧卡片。
  // Android 移动端与 Web 预览模式下首屏直接处于激活态，初值置为 true。
  const clipboardWindowVisibleRef = useRef(!isTauri || isAndroid);

  useEventListener(
    "wheel",
    (event: WheelEvent) => {
      // 鼠标滚轮垂直滚动（deltaY）自动转换为横向卡片列表平滑滚动
      if (Math.abs(event.deltaY) > 0 && Math.abs(event.deltaX) === 0) {
        event.preventDefault();
        const scroller = listContainerRef.current?.querySelector(
          '[data-virtuoso-scroller="true"]',
        ) as HTMLElement | null;
        if (scroller) {
          scroller.scrollLeft += event.deltaY;
        } else if (virtuosoRef.current) {
          virtuosoRef.current.scrollBy({ left: event.deltaY });
        }
      }
    },
    {
      passive: false,
      target: listContainerRef,
    },
  );

  const snapshot = useSnapshot(clipboardViewState);
  const settings = useSnapshot(settingsState);
  const { category, keyword, groupId, range } = snapshot;
  const autoPaste = settings.clipboard.content.autoPaste;
  const middleClick = settings.clipboard.content.middleClick;
  const display = settings.clipboard.display;
  const sort = settings.clipboard.content.sort;
  const redactSecrets = settings.clipboard.sensitive.redactSecrets;
  const quickActions = settings.clipboard.content.itemActions;
  const deleteFavoriteItems = settings.clipboard.content.deleteFavoriteItems;
  const deletePinnedItems = settings.clipboard.content.deletePinnedItems;
  const deleteFavoriteItemsOnlyInFavoriteGroup =
    settings.clipboard.content.deleteFavoriteItemsOnlyInFavoriteGroup;
  const { fileMaxCount } = display;
  const showOriginalPreview = settings.clipboard.content.showOriginalPreview;
  const quickActionLabels = buildItemActionLabels(t);
  const currentGroupName = getCurrentGroupName(customGroups, groupId);

  const {
    findItemById,
    getItem,
    getItemIndexById,
    loadRange,
    loadedInitial,
    loading,
    loadedItems,
    patchItemById,
    reload,
    reloadCurrentRange,
    removeItemById,
    total,
  } = useClipboardItems({
    favorite: range === "favorite" ? true : void 0,
    groupId: groupId ?? void 0,
    keyword,
    kind: category ?? void 0,
    sort,
  });

  async function refreshSyncStatuses() {
    const itemIds = [...loadedItems.values()].map((item) => item.id);
    if (itemIds.length === 0) {
      setSyncStatuses(new Map());
      return;
    }
    const statuses = await getSyncItemStatuses(itemIds);
    setSyncStatuses(new Map(statuses.map((status) => [status.itemId, status])));
  }

  // biome-ignore lint/correctness/useExhaustiveDependencies: loadedItems 是批量状态查询的唯一触发快照
  useEffect(() => {
    void refreshSyncStatuses();
  }, [loadedItems]);

  useTauriListen(TAURI_EVENT.SYNC_UPDATED, () => {
    void refreshSyncStatuses();
  });
  const {
    closeHoverPreviewForScroll,
    closePreview,
    handleItemPointerEnter,
    handleItemPointerLeave,
    handleItemPointerMove,
    handleKeyboardPreviewMove,
    handlePreviewAreaPointerLeave,
    handlePreviewSpaceDown,
    previewSession,
  } = useClipboardPreviewController({
    getActiveItem,
    itemElementMapRef,
    onHoverSelect: () => {},
  });
  closePreviewRef.current = closePreview;
  reloadCurrentRangeRef.current = reloadCurrentRange;

  // 把 Rust 返回的同过滤下总数同步给 Footer（共享 store），避免 Footer 单独 IPC 计数。
  useEffect(() => {
    if (loadedInitial) clipboardStatsState.total = total;
  }, [loadedInitial, total]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: snapshot 作触发器，不需在回调内读取
  useEffect(() => {
    setSelectedId(null);
    if (keywordRef.current !== keyword) keywordRef.current = keyword;
    deferredReloadRef.current = false;
    closePreview("filterChange");
  }, [snapshot]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: 仅按影响列表 payload 的展示设置触发重拉，函数引用用 ref 读取最新值
  useEffect(() => {
    if (!displaySettingsMountedRef.current) {
      displaySettingsMountedRef.current = true;
      return;
    }

    closePreviewRef.current("displaySettingChange");
    reloadCurrentRangeRef.current();
  }, [fileMaxCount, redactSecrets]);

  /**
   * 从 Rust 拉取自定义分组，用于空状态展示当前分组名称。
   */
  const loadGroups = async () => {
    const groups = await listClipboardGroups();

    setCustomGroups(groups);
  };

  /**
   * 首次挂载时拉取分组名称。
   */
  useMount(() => {
    void loadGroups();

    if (isAndroid) {
      clipboardWindowVisibleRef.current = true;
      const handleMobileFocus = () => {
        clipboardWindowVisibleRef.current = true;
        requestReloadAtTop();
      };
      window.addEventListener("focus", handleMobileFocus);
      document.addEventListener("visibilitychange", () => {
        if (!document.hidden) {
          handleMobileFocus();
        }
      });
    }
  });

  /**
   * 自定义分组变化后同步刷新空状态文案可用的分组名。
   */
  const handleGroupsUpdated = () => {
    void loadGroups();
  };

  useTauriListen(TAURI_EVENT.CLIPBOARD_GROUPS_UPDATED, handleGroupsUpdated);

  /**
   * 收到剪贴板更新：仅在列表位于顶部时刷新；否则延后到用户回到顶部后再刷新，
   * 避免打断当前浏览位置。
   * 用 ref 读取最新滚动位置，规避闭包陷旧值（事件订阅只挂载一次）。
   */
  const handleClipboardUpdated = (payload: ClipboardUpdatedPayload) => {
    // 隐藏期间直接在不可见 WebView 内重建当前范围；查询返回前缓存已清空，
    // 即使用户立即唤起窗口，也不会先展示旧卡片再把新记录插到头部。
    if (!clipboardWindowVisibleRef.current) {
      refreshHiddenRange();
      return;
    }

    if (payload.cleanup !== void 0) {
      closePreview("cleanup");
      setSelectedId(null);
      deferredReloadRef.current = false;
      if (clipboardStatsState.total !== null) {
        clipboardStatsState.total = Math.max(
          clipboardStatsState.total - payload.cleanup,
          0,
        );
      }
      requestReloadAtTop();
      return;
    }

    if (payload.imported) {
      closePreview("backupImport");
      setSelectedId(null);
      requestReloadAtTop();
      return;
    }

    if (payload.reconciled) {
      requestReloadAtTop();
      return;
    }

    if (payload.deduplicated) {
      if (
        !shouldRefreshCurrentGroup(
          clipboardViewState.range,
          clipboardViewState.category,
          clipboardViewState.groupId,
          payload.kind,
        )
      ) {
        return;
      }

      requestReloadAtTop();
      return;
    }

    if (
      !shouldRefreshCurrentGroup(
        clipboardViewState.range,
        clipboardViewState.category,
        clipboardViewState.groupId,
        payload.kind,
      )
    ) {
      return;
    }

    requestReloadAtTop();
  };

  useTauriListen<ClipboardUpdatedPayload>(
    TAURI_EVENT.CLIPBOARD_UPDATED,
    (event) => {
      handleClipboardUpdated(event.payload);
    },
  );

  useTauriListen(TAURI_EVENT.SOURCE_APP_UPDATED, () => {
    reloadCurrentRangeRef.current();
  });

  /**
   * 剪贴板窗口显隐变化：更新可见性镜像；显示时按偏好重置分组与滚动位置。
   * 可见性 ref 供 `handleClipboardUpdated` 判断是否处于冻结态——隐藏期间只记 pending，不立即 reload。
   */
  const handleWindowVisibility = (event: {
    payload: WindowVisibilityPayload;
  }) => {
    const { label, visible } = event.payload;
    if (label !== WINDOW_LABEL.CLIPBOARD) return;

    clipboardWindowVisibleRef.current = visible;
    if (!visible) {
      // 窗口可见且离开顶部时可能累积 pending；趁 hide 后预刷新，
      // 确保下次 show 的首帧已经是最新数据。
      if (deferredReloadRef.current) refreshHiddenRange();
      return;
    }

    const {
      scrollToTopOnOpen,
      selectCategoryOnOpen,
      selectGroupOnOpen,
      selectRangeOnOpen,
    } = settings.clipboard.window;
    const shouldResetSelection =
      selectRangeOnOpen !== WINDOW_OPEN_SELECTION_PRESERVE ||
      selectCategoryOnOpen !== WINDOW_OPEN_SELECTION_PRESERVE ||
      selectGroupOnOpen !== WINDOW_OPEN_SELECTION_PRESERVE;
    if (!scrollToTopOnOpen && !shouldResetSelection) return;

    closePreview("windowOpenReset");

    if (selectRangeOnOpen !== WINDOW_OPEN_SELECTION_PRESERVE) {
      clipboardViewState.range = selectRangeOnOpen;
    }

    if (selectCategoryOnOpen === WINDOW_OPEN_SELECTION_ALL) {
      clipboardViewState.category = null;
    } else if (selectCategoryOnOpen !== WINDOW_OPEN_SELECTION_PRESERVE) {
      clipboardViewState.category = selectCategoryOnOpen;
    }

    const openGroupId = parseWindowOpenGroupId(selectGroupOnOpen);
    if (selectGroupOnOpen === WINDOW_OPEN_SELECTION_ALL) {
      clipboardViewState.groupId = null;
    } else if (openGroupId) {
      clipboardViewState.groupId = openGroupId;
    }

    if (!scrollToTopOnOpen) return;

    setSelectedId(null);
    virtuosoRef.current?.scrollToIndex({ behavior: "auto", index: 0 });
    consumeDeferredReloadAtTop();
  };

  useTauriListen<WindowVisibilityPayload>(
    TAURI_EVENT.WINDOW_VISIBILITY,
    handleWindowVisibility,
  );

  /**
   * 删除 / 收藏 / 备注命令均不广播 clipboard://updated，故就地改本地镜像，
   * 避免整页 reload 打断滚动与选中态。
   */
  const removeItem = (id: string) => {
    removeItemById(id);
  };

  const patchItem = (id: string, patch: Partial<ClipboardItem>) => {
    patchItemById(id, patch);
  };

  /**
   * 收藏切换后：favorite 分组下取消收藏的条目应即时移出列表，其余分组仅更新标记。
   */
  const handleFavoriteToggled = (id: string, isFavorite: boolean) => {
    if (range === "favorite" && !isFavorite) {
      removeItem(id);
      return;
    }

    patchItem(id, { isFavorite });
  };

  /**
   * 备注保存后同步本地镜像；后端可能因 autoFavorite 设置联动收藏，故一并回填。
   */
  const handleNoteSaved = (
    id: string,
    note: string | null,
    autoFavorited: boolean,
  ) => {
    patchItem(id, autoFavorited ? { isFavorite: true, note } : { note });
  };

  const handleCloseNote = () => {
    setNoteTarget(null);
  };

  /**
   * 打开条目备注编辑框；若该条正在预览，先关闭预览避免窗口层级互相遮挡。
   */
  const handleOpenNote = (item: ClipboardItem, reason: string) => {
    if (previewSession?.itemId === item.id) closePreview(reason);

    setNoteTarget(item);
  };

  /**
   * 右键菜单移动分组后同步本地镜像；当前分组视图下移出其它分组时直接移除。
   */
  const handleMoveToGroup = async (
    item: ClipboardItem,
    nextGroupId: string,
  ) => {
    if (previewSession?.itemId === item.id) closePreview("moveToGroup");

    await updateClipboardItemGroup(item.id, nextGroupId);

    if (groupId !== null && groupId !== nextGroupId) {
      removeItem(item.id);
      return;
    }

    patchItem(item.id, { groupId: nextGroupId });
  };

  /**
   * 读取当前选中项。无显式选中时，优先取当前可视范围第一项。
   */
  function getActiveItem() {
    if (total === 0) return null;

    if (selectedId === null) {
      return getItem(firstVisibleIndex) ?? getItem(0);
    }

    return findItemById(selectedId);
  }

  /**
   * 注册虚拟列表项对应的 DOM 节点，预览打开时用它采集 anchor rect。
   */
  const registerItemElement = (id: string) => {
    return (node: HTMLDivElement | null) => {
      if (node) {
        itemElementMapRef.current.set(id, node);
        return;
      }

      itemElementMapRef.current.delete(id);
    };
  };

  /**
   * 快捷键触发的删除：复用 `deleteClipboardItem` 内置的二次确认弹窗，
   * 仅当用户确认且 Rust 删除成功时才同步本地镜像。
   */
  const handleShortcutDelete = async (id: string) => {
    const target = findItemById(id);

    if (!target || !canDeleteItem(target)) return;

    if (previewSession?.itemId === id) closePreview("delete");

    const deleted = await deleteClipboardItem(
      id,
      target.isFavorite,
      target.isPinned,
    );

    if (!deleted) return;

    setSelectedId(
      getSelectedIdAfterDelete(
        getItem,
        getItemIndexById,
        firstVisibleIndex,
        selectedId,
        id,
      ),
    );
    removeItem(id);
  };

  /**
   * 快捷键触发的收藏切换：读当前项的 isFavorite 计算下一态，
   * Rust 返回真实状态后走统一的 `handleFavoriteToggled`（favorite 分组内取消会移除）。
   */
  const handleShortcutToggleFavorite = async (id: string) => {
    const current = findItemById(id);

    if (!current) return;

    const next = await toggleClipboardItemFavorite(id, !current.isFavorite);

    handleFavoriteToggled(id, next);
  };

  /**
   * 切换条目置顶态；置顶影响排序，成功后刷新当前范围以继续信任后端顺序。
   */
  const handleTogglePinned = async (id: string) => {
    const current = findItemById(id);

    if (!current) return;

    const next = await toggleClipboardItemPinned(id, !current.isPinned);

    patchItem(id, { isPinned: next });
    reloadCurrentRange();
  };

  /**
   * 按当前条目后端声明的可用动作执行“打开”：链接 / 邮箱 / 定位文件共用 Cmd/Ctrl+O。
   */
  const handleShortcutOpen = async (
    item: ClipboardItem,
    action: ClipboardAction,
  ) => {
    if (previewSession?.itemId === item.id) closePreview("shortcutOpen");

    switch (action) {
      case "openLink":
        await openClipboardItemLink(item.id, false);
        return;
      case "sendEmail":
        await openClipboardItemLink(item.id, true);
        return;
      case "revealInFinder":
      case "revealInExplorer":
        await revealClipboardItem(item.id);
        return;
      default:
        return;
    }
  };

  /**
   * Rust 右键菜单点击事件：携带 `{action, itemId}`。
   * 用 ref 持续指向「当前 render 的派发函数」，规避 `useTauriListen` 只在挂载时
   * 抓一次闭包导致的状态过期（同款做法见 `handleClipboardUpdated`）。
   */
  const handleMenuActionRef = useRef<
    (payload: ClipboardMenuActionPayload) => void
  >(() => {});
  handleMenuActionRef.current = (payload) => {
    const { action, groupId: targetGroupId, itemId } = payload;
    const target = findItemById(itemId);

    if (!target) return;

    switch (action) {
      case "paste":
        closePreview("paste");
        pasteClipboardItem(target.id, false);
        return;
      case "pasteAsPlainText":
      case "pasteAsPath":
        closePreview("pastePlain");
        pasteClipboardItem(target.id, true);
        return;
      case "copy":
        if (previewSession?.itemId === target.id) closePreview("copy");
        writeToClipboard(target.id, false);
        return;
      case "saveImage":
        if (previewSession?.itemId === target.id) closePreview("saveImage");
        saveClipboardImageToFile(target.id);
        return;
      case "openLink":
        if (previewSession?.itemId === target.id) closePreview("openLink");
        openClipboardItemLink(target.id, false);
        return;
      case "sendEmail":
        if (previewSession?.itemId === target.id) closePreview("sendEmail");
        openClipboardItemLink(target.id, true);
        return;
      case "revealInFinder":
      case "revealInExplorer":
        if (previewSession?.itemId === target.id) closePreview("reveal");
        revealClipboardItem(target.id);
        return;
      case "toggleFavorite":
        handleShortcutToggleFavorite(target.id);
        return;
      case "togglePinned":
        handleTogglePinned(target.id);
        return;
      case "moveToGroup":
        if (!targetGroupId) return;

        void handleMoveToGroup(target, targetGroupId);
        return;
      case "editNote":
        handleOpenNote(target, "editNote");
        return;
      case "delete":
        if (!canDeleteItem(target)) return;

        handleShortcutDelete(target.id);
        return;
    }
  };

  const handleMenuActionEvent = (event: { payload: unknown }) => {
    handleMenuActionRef.current(event.payload as ClipboardMenuActionPayload);
  };

  useTauriListen(TAURI_EVENT.CLIPBOARD_MENU_ACTION, handleMenuActionEvent);

  const handleKeyDown = (event: KeyboardEvent) => {
    const target = event.target as HTMLElement | null;
    const isInputActive =
      target &&
      (target.tagName === "INPUT" ||
        target.tagName === "TEXTAREA" ||
        target.isContentEditable);

    if (isInputActive) {
      if (event.key === "Escape") {
        event.preventDefault();
        closeTopEscapeLayer();
      }
      return;
    }

    const eventModifierPressed = isMac ? event.metaKey : event.ctrlKey;

    setIsModifierPressed(eventModifierPressed);

    if (event.key === "Escape") {
      event.preventDefault();
      closeTopEscapeLayer();

      return;
    }

    if (total === 0) return;

    if (event.key === "Enter") {
      event.preventDefault();

      const activeItem = getActiveItem();

      if (!activeItem) return;

      closePreview("enterPaste");
      pasteClipboardItem(activeItem.id, eventModifierPressed);

      return;
    }

    if (isSpaceKey(event)) {
      handlePreviewSpaceDown(event);
      return;
    }

    if (
      eventModifierPressed &&
      (event.key === "Backspace" || event.key === "Delete")
    ) {
      event.preventDefault();

      const activeItem = getActiveItem();

      if (!activeItem) return;

      handleShortcutDelete(activeItem.id);

      return;
    }

    if (
      eventModifierPressed &&
      event.key.toLowerCase() === "c" &&
      !shouldUseNativeCopy(event)
    ) {
      event.preventDefault();

      const activeItem = getActiveItem();

      if (!activeItem) return;

      if (previewSession?.itemId === activeItem.id)
        closePreview("shortcutCopy");

      writeToClipboard(activeItem.id, false);

      return;
    }

    if (eventModifierPressed && event.key.toLowerCase() === "o") {
      const activeItem = getActiveItem();

      if (!activeItem) return;

      const openAction = getOpenClipboardAction(activeItem.availableActions);

      if (!openAction) return;

      event.preventDefault();
      void handleShortcutOpen(activeItem, openAction);

      return;
    }

    if (eventModifierPressed && event.key.toLowerCase() === "d") {
      event.preventDefault();

      const activeItem = getActiveItem();

      if (!activeItem) return;

      handleShortcutToggleFavorite(activeItem.id);

      return;
    }

    if (eventModifierPressed && event.key.toLowerCase() === "t") {
      event.preventDefault();

      const activeItem = getActiveItem();

      if (!activeItem) return;

      handleTogglePinned(activeItem.id);

      return;
    }

    if (eventModifierPressed && event.key.toLowerCase() === "m") {
      event.preventDefault();

      const activeItem = getActiveItem();

      if (!activeItem) return;

      handleOpenNote(activeItem, "shortcutNote");

      return;
    }

    if (
      event.key !== "ArrowUp" &&
      event.key !== "ArrowDown" &&
      event.key !== "ArrowLeft" &&
      event.key !== "ArrowRight"
    ) {
      if (
        !eventModifierPressed &&
        !event.altKey &&
        event.key.length === 1 &&
        ![" ", "Enter", "Tab", "Escape"].includes(event.key)
      ) {
        window.dispatchEvent(
          new CustomEvent("ecopaste:type-to-search", {
            detail: { key: event.key },
          }),
        );
      }

      return;
    }

    event.preventDefault();

    const next = getNextKeyboardTarget(event);

    if (!next) return;

    setSelectedId(next.item.id);
    virtuosoRef.current?.scrollIntoView({
      behavior: "auto",
      index: next.index,
    });

    handleKeyboardPreviewMove(next.item);
  };

  useKeyboardEvent("keydown", handleKeyDown);

  const handleKeyUp = (event: KeyboardEvent) => {
    const eventModifierPressed = isMac ? event.metaKey : event.ctrlKey;

    setIsModifierPressed(eventModifierPressed);
  };

  useKeyboardEvent("keyup", handleKeyUp);

  const handleRangeChanged = ({
    endIndex,
    startIndex,
  }: {
    startIndex: number;
    endIndex: number;
  }) => {
    setFirstVisibleIndex(startIndex);

    closeHoverPreviewForScroll();
    loadRange(startIndex, endIndex);
  };

  const handleAtTopStateChange = (atTop: boolean) => {
    isAtTopRef.current = atTop;

    if (!atTop) return;

    consumeDeferredReloadAtTop();
  };

  async function handleOpenAndroidFile(item: ClipboardItem, index: number) {
    const key = `open:${item.id}:${index}`;
    setFileActionKey(key);
    try {
      await openAndroidClipboardFile(item.id, index);
    } catch {
      // 命令层已统一显示错误。
    } finally {
      setFileActionKey(null);
    }
  }

  async function handleSaveAndroidFile(item: ClipboardItem, index: number) {
    const key = `save:${item.id}:${index}`;
    setFileActionKey(key);
    try {
      await saveAndroidClipboardFile(item.id, index);
    } catch {
      // 命令层已统一显示错误。
    } finally {
      setFileActionKey(null);
    }
  }

  /** 使用完整路径数量判断，避免卡片展示上限把多文件记录误判为单文件。 */
  function hasOneOpenableFile(item: ClipboardItem) {
    const entries = item.fileEntries ?? [];
    const totalFiles = item.content.split("\n").filter((path) => {
      return path.length > 0;
    }).length;

    return totalFiles === 1 && entries.length === 1 && !entries[0]?.isDir;
  }

  /** 先显示卡片已有数据，再复用预览接口补齐最多 64 个文件条目。 */
  async function showAndroidFileDetails(item: ClipboardItem) {
    const requestId = fileDetailsRequestRef.current + 1;
    fileDetailsRequestRef.current = requestId;
    setFileDetailsTarget(item);
    setFileDetailsEntries(item.fileEntries ?? []);

    try {
      const payload = await getClipboardPreviewPayload(item.id);
      if (fileDetailsRequestRef.current !== requestId || !payload) return;

      setFileDetailsEntries(payload.files);
    } catch {
      // 命令层已统一显示错误，已加载的卡片条目仍可操作。
    }
  }

  function handleMobileFileOpen(item: ClipboardItem) {
    if (!hasOneOpenableFile(item)) {
      void showAndroidFileDetails(item);
      return;
    }

    void handleOpenAndroidFile(item, 0);
  }

  function handleMobileFileSave(item: ClipboardItem) {
    if (!hasOneOpenableFile(item)) {
      void showAndroidFileDetails(item);
      return;
    }

    void handleSaveAndroidFile(item, 0);
  }

  function handleCloseFileDetails() {
    fileDetailsRequestRef.current += 1;
    setFileDetailsTarget(null);
    setFileDetailsEntries([]);
  }

  /**
   * 自动刷新请求只在顶部执行；离开顶部时保留 pending，等待回顶后消费。
   */
  function requestReloadAtTop() {
    if (!isAtTopRef.current) {
      deferredReloadRef.current = true;
      return;
    }

    deferredReloadRef.current = false;
    reload();
  }

  /**
   * 在隐藏的 WebView 内重建当前查询范围，避免窗口显示后再替换旧列表。
   */
  function refreshHiddenRange() {
    deferredReloadRef.current = false;
    reloadCurrentRange();
  }

  /**
   * 消费已有 pending；用于窗口回顶偏好或用户手动回到顶部后的补刷。
   */
  function consumeDeferredReloadAtTop() {
    if (!deferredReloadRef.current) return;

    requestReloadAtTop();
  }

  if (loading && !loadedInitial) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <Spin />
      </div>
    );
  }

  if (loadedInitial && total === 0) {
    const description = getEmptyDescription(
      t,
      keyword,
      range,
      category,
      groupId,
      currentGroupName,
    );

    return (
      <div
        className="flex flex-1 flex-col items-center justify-center"
        data-tauri-drag-region
      >
        <Empty description={description} image={Empty.PRESENTED_IMAGE_SIMPLE} />
      </div>
    );
  }

  return (
    <div
      className="relative size-full overflow-hidden"
      onPointerLeave={handlePreviewAreaPointerLeave}
      ref={listContainerRef}
      role="listbox"
    >
      <Virtuoso
        atTopStateChange={handleAtTopStateChange}
        className="no-scrollbar"
        computeItemKey={computeItemKey}
        horizontalDirection={!isMobile()}
        itemContent={renderItemContent}
        rangeChanged={handleRangeChanged}
        ref={virtuosoRef}
        style={{
          height: "100%",
          paddingBottom: isMobile() ? "var(--mobile-safe-area-bottom)" : 0,
          paddingLeft: isMobile() ? 0 : 16,
          paddingRight: isMobile() ? 0 : 16,
          width: "100%",
        }}
        totalCount={total}
      />

      <NoteModal
        item={noteTarget}
        onClose={handleCloseNote}
        onSaved={handleNoteSaved}
      />
      <AndroidFileDetailsDrawer
        busyKey={fileActionKey}
        entries={fileDetailsEntries}
        item={fileDetailsTarget}
        onClose={handleCloseFileDetails}
        onOpen={(item, index) => {
          void handleOpenAndroidFile(item, index);
        }}
        onSave={(item, index) => {
          void handleSaveAndroidFile(item, index);
        }}
      />
    </div>
  );

  function renderItemContent(index: number) {
    const item = getItem(index);
    if (!item) return renderPlaceholderItem(index);

    const handlePointerEnter = (event: ReactPointerEvent<HTMLDivElement>) => {
      handleItemPointerEnter(item, event);
    };

    const handlePointerLeave = () => {
      handleItemPointerLeave();
    };

    const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
      handleItemPointerMove(item, event);
    };

    const relativeIndex = index - firstVisibleIndex;
    const hintKey =
      relativeIndex >= 0 && relativeIndex < 10
        ? KEY_HINTS[relativeIndex]
        : void 0;

    const handleQuickPaste = () => {
      closePreview("quickPaste");
      pasteClipboardItem(item.id, false);
    };

    const handleOpenLink = () => {
      closePreview("openLink");
      openClipboardItemLink(item.id, item.subKind === "email");
    };

    const handleEditNote = () => {
      handleOpenNote(item, "editNote");
    };

    const handleQuickAction = async (action: ItemAction) => {
      if (action === "delete" && !canDeleteItem(item)) return;

      switch (action) {
        case "paste":
          closePreview("quickPaste");
          await pasteClipboardItem(item.id, false);
          return;
        case "pastePlain":
          closePreview("quickPastePlain");
          await pasteClipboardItem(item.id, true);
          return;
        case "pastePath":
          closePreview("quickPastePath");
          await pasteClipboardItem(item.id, true);
          return;
        case "copy":
          if (previewSession?.itemId === item.id) closePreview("quickCopy");
          await writeToClipboard(item.id, false);
          return;
        case "copyPlain":
          if (previewSession?.itemId === item.id) {
            closePreview("quickCopyPlain");
          }
          await writeToClipboard(item.id, true);
          return;
        case "openLink":
          if (previewSession?.itemId === item.id) {
            closePreview("quickOpenLink");
          }
          await openClipboardItemLink(item.id, false);
          return;
        case "sendEmail":
          if (previewSession?.itemId === item.id) {
            closePreview("quickSendEmail");
          }
          await openClipboardItemLink(item.id, true);
          return;
        case "reveal":
          if (previewSession?.itemId === item.id) closePreview("quickReveal");
          await revealClipboardItem(item.id);
          return;
        case "note":
          handleEditNote();
          return;
        case "pinItem":
          await handleTogglePinned(item.id);
          return;
        case "star":
          await handleShortcutToggleFavorite(item.id);
          return;
        case "delete":
          await handleShortcutDelete(item.id);
          return;
      }
    };

    const handleSyncStatusChange = (status: SyncItemStatus) => {
      setSyncStatuses((current) => {
        const next = new Map(current);
        next.set(status.itemId, status);
        return next;
      });
    };

    const handleMouseDown = (event: ReactMouseEvent<HTMLDivElement>) => {
      if (event.button !== 0) {
        if (event.button !== 1) return;

        event.preventDefault();

        if (middleClick === "singleClickPaste") {
          setSelectedId(item.id);
          closePreview("middleClickPaste");
          pasteClipboardItem(item.id, false);
          return;
        }

        if (middleClick === "singleClickPastePlain") {
          setSelectedId(item.id);
          closePreview("middleClickPastePlain");
          pasteClipboardItem(item.id, true);
          return;
        }

        if (middleClick === "singleClickCopy") {
          setSelectedId(item.id);
          closePreview("middleClickCopy");
          writeToClipboard(item.id, false);
          return;
        }

        if (middleClick === "singleClickCopyPlain") {
          setSelectedId(item.id);
          closePreview("middleClickCopyPlain");
          writeToClipboard(item.id, true);
        }

        return;
      }

      setSelectedId(item.id);

      if (isMobile()) {
        if (item.kind === "files") {
          handleMobileFileOpen(item);
          return;
        }

        closePreview("mobileSingleClickPaste");
        pasteClipboardItem(item.id, false);
        return;
      }

      if (autoPaste === "singleClickPaste") {
        closePreview("singleClickPaste");
        pasteClipboardItem(item.id, false);
        return;
      }

      if (autoPaste === "singleClickCopy") {
        closePreview("singleClickCopy");
        writeToClipboard(item.id, false);
      }
    };

    const handleAuxClick = (event: ReactMouseEvent<HTMLDivElement>) => {
      if (event.button !== 1) return;

      event.preventDefault();
    };

    const handleDoubleClick = () => {
      if (autoPaste === "doubleClickPaste") {
        closePreview("doubleClickPaste");
        pasteClipboardItem(item.id, false);
        return;
      }

      if (autoPaste === "doubleClickCopy") {
        closePreview("doubleClickCopy");
        writeToClipboard(item.id, false);
      }
    };

    const availableActions = getAllowedClipboardActions(
      item.availableActions,
      item,
      canDeleteItem,
    );
    const visibleQuickActions = getAllowedItemActions(
      quickActions,
      item,
      canDeleteItem,
    );

    return (
      <div
        className={cn(
          isMobile()
            ? "w-full px-3 py-1"
            : "flex aspect-square h-full min-w-0 shrink-0 items-center px-[6px] py-2.5",
        )}
      >
        <ClipboardCard
          availableActions={availableActions}
          hintKey={hintKey}
          isLinkActive={isModifierPressed}
          isSelected={
            selectedId === null
              ? index === firstVisibleIndex
              : item.id === selectedId
          }
          item={item}
          mobileFileOpenLabel={t("fileAccess.open")}
          mobileFileSaveLabel={t("fileAccess.saveAs")}
          onAuxClick={handleAuxClick}
          onDoubleClick={handleDoubleClick}
          onMobileFileOpen={() => {
            handleMobileFileOpen(item);
          }}
          onMobileFileSave={() => {
            handleMobileFileSave(item);
          }}
          onMouseDown={handleMouseDown}
          onOpenLink={handleOpenLink}
          onPointerEnter={handlePointerEnter}
          onPointerLeave={handlePointerLeave}
          onPointerMove={handlePointerMove}
          onQuickAction={handleQuickAction}
          onQuickPaste={hintKey ? handleQuickPaste : void 0}
          onSyncStatusChange={handleSyncStatusChange}
          quickActionLabels={quickActionLabels}
          quickActions={visibleQuickActions}
          rootRef={registerItemElement(item.id)}
          showOriginalOnHover={showOriginalPreview}
          syncStatus={syncStatuses.get(item.id)}
        />
      </div>
    );
  }

  function renderPlaceholderItem(_index: number) {
    return (
      <div
        aria-hidden="true"
        className="flex aspect-square h-full shrink-0 items-center px-[6px] py-2.5"
      >
        <div className="flex size-full animate-pulse flex-col justify-between rounded-[16px] bg-white/70 p-3.5 shadow-xs">
          <div className="h-[60px] w-full rounded-xl bg-slate-200/60" />
          <div className="mt-3 flex flex-1 flex-col gap-2">
            <span className="h-3 w-9/12 rounded-xs bg-slate-200/60" />
            <span className="h-3 w-6/12 rounded-xs bg-slate-200/60" />
          </div>
        </div>
      </div>
    );
  }

  /**
   * 根据方向键计算下一项。
   */
  function getNextKeyboardTarget(event: KeyboardEvent) {
    const nextIndex = getNextKeyboardIndex(
      getItemIndexById,
      firstVisibleIndex,
      selectedId,
      total,
      event.key,
    );
    const item = getItem(nextIndex);
    if (!item) {
      loadRange(nextIndex, nextIndex);
      return null;
    }

    return { index: nextIndex, item };
  }

  /**
   * 判断当前条目是否允许删除：收藏 / 置顶条目分别受各自保护开关约束。
   */
  function canDeleteItem(item: ClipboardItem) {
    if (item.isPinned && !deletePinnedItems) return false;

    if (!item.isFavorite) return true;

    if (!deleteFavoriteItems) return false;

    if (!deleteFavoriteItemsOnlyInFavoriteGroup) return true;

    return range === "favorite";
  }

  /**
   * ESC 按预览、分组、分类、窗口的顺序逐层退出。
   */
  function closeTopEscapeLayer() {
    if (previewSession !== null) {
      closePreview("escape");
      return;
    }

    if (clipboardViewState.groupId !== null) {
      clipboardViewState.groupId = null;
      return;
    }

    if (clipboardViewState.category !== null) {
      clipboardViewState.category = null;
      return;
    }

    void hideWindow(WINDOW_LABEL.CLIPBOARD);
  }
};

/**
 * 生成空列表文案，按搜索词、范围、分类和自定义分组组合出具体提示。
 */
function getEmptyDescription(
  t: TFunction<"clipboard">,
  keyword: string,
  range: ClipboardRange,
  category: ClipboardKind | null,
  groupId: string | null,
  groupName: string | null,
) {
  const isSearching = keyword.length > 0;
  const isFavorite = range === "favorite";
  const hasGroup = groupId !== null;
  const categoryLabel = category ? t(`empty.categories.${category}`) : "";
  const groupLabel = groupName ?? t("empty.groupFallback");

  if (isSearching) {
    return getSearchingEmptyDescription(
      t,
      keyword,
      isFavorite,
      hasGroup,
      groupLabel,
      categoryLabel,
    );
  }

  if (hasGroup) {
    if (isFavorite && category) {
      return t("empty.groupFavoriteCategory", {
        category: categoryLabel,
        group: groupLabel,
      });
    }

    if (isFavorite) {
      return t("empty.groupFavorites", { group: groupLabel });
    }

    if (category) {
      return t("empty.groupCategory", {
        category: categoryLabel,
        group: groupLabel,
      });
    }

    return t("empty.group", { group: groupLabel });
  }

  if (isFavorite && category) {
    return t("empty.favoriteCategory", { category: categoryLabel });
  }

  if (category) {
    return t("empty.category", { category: categoryLabel });
  }

  return t(isFavorite ? "empty.favorites" : "empty.history");
}

/**
 * 生成搜索空状态文案，覆盖范围 / 分类 / 分组三种过滤维度。
 */
function getSearchingEmptyDescription(
  t: TFunction<"clipboard">,
  keyword: string,
  isFavorite: boolean,
  hasGroup: boolean,
  groupLabel: string,
  categoryLabel: string,
) {
  const hasCategory = categoryLabel.length > 0;

  if (hasGroup) {
    if (isFavorite && hasCategory) {
      return t("empty.searchGroupFavoriteCategory", {
        category: categoryLabel,
        group: groupLabel,
        keyword,
      });
    }

    if (isFavorite) {
      return t("empty.searchGroupFavorites", {
        group: groupLabel,
        keyword,
      });
    }

    if (hasCategory) {
      return t("empty.searchGroupCategory", {
        category: categoryLabel,
        group: groupLabel,
        keyword,
      });
    }

    return t("empty.searchGroup", { group: groupLabel, keyword });
  }

  if (isFavorite && hasCategory) {
    return t("empty.searchFavoriteCategory", {
      category: categoryLabel,
      keyword,
    });
  }

  if (isFavorite) {
    return t("empty.searchFavorites", { keyword });
  }

  if (hasCategory) {
    return t("empty.searchCategory", { category: categoryLabel, keyword });
  }

  return t("empty.searchHistory", { keyword });
}

/**
 * 从当前已加载分组列表中取出选中分组名称；找不到时交给文案层兜底。
 */
function getCurrentGroupName(
  groups: ClipboardGroupRecord[],
  groupId: string | null,
) {
  if (!groupId) return null;

  const current = groups.find((record) => {
    return record.id === groupId;
  });

  return current?.name ?? null;
}

/**
 * 根据方向键和当前选中 id 计算下一项索引。
 */
function getNextKeyboardIndex(
  getItemIndexById: (id: string) => number | null,
  firstVisibleIndex: number,
  selectedId: string | null,
  total: number,
  key: string,
) {
  const selectedIndex =
    selectedId === null ? null : getItemIndexById(selectedId);
  const currentIndex = selectedIndex ?? firstVisibleIndex;

  if (key === "ArrowUp" || key === "ArrowLeft") {
    return Math.max(0, currentIndex - 1);
  }

  return Math.min(total - 1, currentIndex + 1);
}

/**
 * 按条目保护规则过滤右键菜单动作，避免受保护项出现删除入口。
 */
function getAllowedClipboardActions(
  actions: ClipboardAction[] | undefined,
  item: ClipboardItem,
  canDeleteItem: (item: ClipboardItem) => boolean,
) {
  if (canDeleteItem(item)) return actions;

  return actions?.filter((action) => {
    return action !== "delete";
  });
}

/**
 * 从后端声明的右键动作中取出可由 Cmd/Ctrl+O 触发的“打开”动作。
 */
function getOpenClipboardAction(actions: ClipboardAction[] | undefined) {
  const openActions: ClipboardAction[] = [
    "openLink",
    "sendEmail",
    "revealInFinder",
    "revealInExplorer",
  ];

  return actions?.find((action) => {
    return openActions.includes(action);
  });
}

/**
 * 按条目保护规则过滤悬停快捷动作，避免受保护项出现删除按钮。
 */
function getAllowedItemActions(
  actions: readonly ItemAction[],
  item: ClipboardItem,
  canDeleteItem: (item: ClipboardItem) => boolean,
) {
  if (canDeleteItem(item)) return [...actions];

  return actions.filter((action) => {
    return action !== "delete";
  });
}

/**
 * 删除当前 active 项后优先选后一项；删除末尾时回退到前一项。
 * 右键删除非 active 项时保留当前显式选中，避免意外跳选。
 */
function getSelectedIdAfterDelete(
  getItem: (index: number) => ClipboardItem | null,
  getItemIndexById: (id: string) => number | null,
  activeFallbackIndex: number,
  selectedId: string | null,
  deletedId: string,
) {
  const deletedIndex = getItemIndexById(deletedId);
  if (deletedIndex === null) return selectedId;

  const activeId =
    selectedId ?? getItem(activeFallbackIndex)?.id ?? getItem(0)?.id ?? null;

  if (activeId !== deletedId) return selectedId;

  const nextItem = getItem(deletedIndex + 1) ?? getItem(deletedIndex - 1);

  return nextItem?.id ?? null;
}

/**
 * 判断 Cmd/Ctrl+C 是否应交给浏览器原生复制，避免覆盖输入框或文本选区复制。
 */
function shouldUseNativeCopy(event: KeyboardEvent) {
  const target = event.target;
  if (target instanceof HTMLElement) {
    const tagName = target.tagName.toLowerCase();
    if (target.isContentEditable) return true;
    if (tagName === "input" || tagName === "textarea") return true;
  }

  const selection = window.getSelection();

  return Boolean(selection && !selection.isCollapsed);
}

const computeItemKey = (index: number, item?: ClipboardItem) => {
  return item?.id ?? `placeholder-${index}`;
};

/**
 * 判断普通剪贴板更新是否会出现在当前分组列表中。
 */
function shouldRefreshCurrentGroup(
  range: ClipboardRange,
  category: ClipboardKind | null,
  groupId: string | null,
  kind?: ClipboardKind,
) {
  if (groupId) return false;
  if (range === "favorite") return false;
  if (!category) return true;
  if (kind === void 0) return false;

  return category === kind;
}

export default List;
