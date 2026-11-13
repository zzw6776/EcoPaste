import type { FC } from "react";
import { lazy, Suspense, useEffect, useRef } from "react";
import { useSnapshot } from "valtio";
import { TAURI_EVENT } from "@/constants/events";
import { useClipboardItems } from "@/hooks/useClipboardItems";
import { useTauriListen } from "@/hooks/useTauriListen";
import { clipboardViewState } from "@/stores/clipboardView";
import { settingsState } from "@/stores/settings";
import type { ClipboardKind, ClipboardRange } from "@/types/clipboard";
import { recordStartupTiming } from "@/utils/startupTiming";

export interface ClipboardListView {
  category: ClipboardKind | null;
  groupId: string | null;
  range: ClipboardRange;
}

export interface ClipboardListProps {
  active?: boolean;
  view?: ClipboardListView;
}

const List = lazy(async () => {
  return await import("./List");
});

/** 查询与列表代码加载并行；等待期间沿用外层已经占好的内容区域。 */
const ClipboardList: FC<ClipboardListProps> = (props) => {
  const { active = true, view } = props;
  const snapshot = useSnapshot(clipboardViewState);
  const settings = useSnapshot(settingsState);
  const viewMountedRef = useRef(false);
  const category = view ? view.category : snapshot.category;
  const groupId = view ? view.groupId : snapshot.groupId;
  const range = view ? view.range : snapshot.range;
  const items = useClipboardItems(
    {
      favorite: range === "favorite" ? true : void 0,
      groupId: groupId ?? void 0,
      keyword: snapshot.keyword,
      kind: category ?? void 0,
      sort: settings.clipboard.content.sort,
    },
    active,
  );

  // 卡片视图挂载后接管增量更新；代码加载期间先保证查询结果跟上新复制。
  useTauriListen(TAURI_EVENT.CLIPBOARD_UPDATED, () => {
    if (!viewMountedRef.current) items.reload();
  });

  useEffect(() => {
    if (!active || !items.loadedInitial) return;

    recordStartupTiming("clipboard-data-ready", {
      loadedItems: items.loadedItems.size,
      total: items.total,
    });
  }, [active, items.loadedInitial, items.loadedItems.size, items.total]);

  function handleViewMounted() {
    viewMountedRef.current = true;
  }

  return (
    <Suspense fallback={null}>
      <List
        active={active}
        items={items}
        onMounted={handleViewMounted}
        view={view}
      />
    </Suspense>
  );
};

export default ClipboardList;
