import { useCallback, useEffect, useRef, useState } from "react";
import { listClipboardItems } from "@/commands";
import type { ClipboardItem, ClipboardItemQuery } from "@/types/clipboard";
import {
  type ClipboardItemsRange,
  getMissingClipboardRanges,
} from "./clipboardItemRanges";

/**
 * 预加载范围按页对齐，实际请求只读取其中尚未加载的部分。
 */
const PAGE_SIZE = 30;
const PRELOAD_ROWS = 30;
const CACHE_MAX_ROWS = 180;
const CACHE_KEEP_RADIUS = 90;

interface FetchRangeOptions {
  force?: boolean;
  token: number;
}

/**
 * 剪贴板列表 range cache：Rust 仍是查询、排序、搜索和 payload 裁剪的唯一真相；
 * 前端只按 Virtuoso 的可视范围缓存少量已加载行，避免无限滚动后持有完整列表。
 */
export const useClipboardItems = (query: ClipboardItemQuery) => {
  const queryRef = useRef(query);

  const requestTokenRef = useRef(0);
  const staleRangeRef = useRef(false);
  const itemsRef = useRef(new Map<number, ClipboardItem>());
  const totalRef = useRef(0);
  const loadingRangesRef = useRef<ClipboardItemsRange[]>([]);
  const loadedInitialRef = useRef(false);
  const viewRangeRef = useRef<ClipboardItemsRange>({
    end: PAGE_SIZE - 1,
    start: 0,
  });

  const [items, setItems] = useState(() => new Map<number, ClipboardItem>());
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [loadedInitial, setLoadedInitial] = useState(false);
  const [loadingRangeCount, setLoadingRangeCount] = useState(0);

  const commitItems = useCallback((nextItems: Map<number, ClipboardItem>) => {
    itemsRef.current = nextItems;
    setItems(nextItems);
  }, []);

  const commitTotal = useCallback((nextTotal: number) => {
    totalRef.current = nextTotal;
    setTotal(nextTotal);
  }, []);

  const commitLoadedInitial = useCallback((nextLoadedInitial: boolean) => {
    loadedInitialRef.current = nextLoadedInitial;
    setLoadedInitial(nextLoadedInitial);
  }, []);

  const resetLoadingRanges = useCallback(() => {
    loadingRangesRef.current = [];
    setLoadingRangeCount(0);
  }, []);

  const addLoadingRange = useCallback((range: ClipboardItemsRange) => {
    loadingRangesRef.current = [...loadingRangesRef.current, range];
    setLoadingRangeCount(loadingRangesRef.current.length);
  }, []);

  const removeLoadingRange = useCallback((range: ClipboardItemsRange) => {
    loadingRangesRef.current = loadingRangesRef.current.filter((current) => {
      return current !== range;
    });
    setLoadingRangeCount(loadingRangesRef.current.length);
  }, []);

  const fetchRange = useCallback(
    async function fetchRange(
      rawStartIndex: number,
      rawEndIndex: number,
      options: FetchRangeOptions,
    ): Promise<void> {
      if (options.token !== requestTokenRef.current) return;

      const range = normalizeFetchRange(
        rawStartIndex,
        rawEndIndex,
        totalRef.current,
      );
      if (range === null) return;

      let ranges = options.force
        ? [range]
        : getMissingClipboardRanges(
            itemsRef.current,
            loadingRangesRef.current,
            range,
          );
      if (ranges.length === 0) return;

      if (staleRangeRef.current) {
        // 暂缓刷新时保留已显示行；需要补读时整段换代，不能把旧位置与新分页拼接。
        staleRangeRef.current = false;
        itemsRef.current = new Map();
        ranges = [range];
      }

      await Promise.all(
        ranges.map(async (range) => {
          addLoadingRange(range);

          try {
            const page = await listClipboardItems({
              ...queryRef.current,
              limit: range.end - range.start + 1,
              offset: range.start,
            });

            if (options.token !== requestTokenRef.current) return;

            const nextTotal = Math.max(0, page.total);
            const nextItems = new Map(itemsRef.current);
            page.list.forEach((item, offset) => {
              const index = range.start + offset;
              if (index < nextTotal) nextItems.set(index, item);
            });

            trimCache(nextItems, viewRangeRef.current, nextTotal);
            commitItems(nextItems);
            commitTotal(nextTotal);
            commitLoadedInitial(true);
          } catch {
            // 命令包装层已统一 log + toast；这里只避免初始请求失败后卡在 loading。
            if (
              options.token === requestTokenRef.current &&
              !loadedInitialRef.current
            ) {
              commitItems(new Map());
              commitTotal(0);
              commitLoadedInitial(true);
            }
          } finally {
            if (options.token === requestTokenRef.current) {
              removeLoadingRange(range);
              setLoading(false);
            } else if (
              staleRangeRef.current &&
              !isRangeLoaded(itemsRef.current, viewRangeRef.current)
            ) {
              // 数据变化取消了正在补读的可见行时，补完该次滚动，避免占位行一直无法加载。
              void fetchRange(
                viewRangeRef.current.start - PRELOAD_ROWS,
                viewRangeRef.current.end + PRELOAD_ROWS,
                { token: requestTokenRef.current },
              );
            }
          }
        }),
      );
    },
    [
      addLoadingRange,
      commitItems,
      commitLoadedInitial,
      commitTotal,
      removeLoadingRange,
    ],
  );

  /** 标记排序位置可能已变化，但不主动刷新正在浏览的已加载行。 */
  const invalidate = useCallback(() => {
    requestTokenRef.current += 1;
    staleRangeRef.current = true;
    resetLoadingRanges();
  }, [resetLoadingRanges]);

  const reload = useCallback(() => {
    itemsRef.current = new Map();
    const token = requestTokenRef.current + 1;
    requestTokenRef.current = token;
    staleRangeRef.current = false;
    resetLoadingRanges();
    if (!loadedInitialRef.current) {
      commitItems(new Map());
      commitTotal(0);
      commitLoadedInitial(false);
      setLoading(true);
    }
    viewRangeRef.current = {
      end: PAGE_SIZE - 1,
      start: 0,
    };

    void fetchRange(0, PAGE_SIZE - 1, {
      force: true,
      token,
    });
  }, [
    commitItems,
    commitLoadedInitial,
    commitTotal,
    fetchRange,
    resetLoadingRanges,
  ]);

  const resetAndReload = useCallback(() => {
    const token = requestTokenRef.current + 1;
    requestTokenRef.current = token;
    staleRangeRef.current = false;
    resetLoadingRanges();
    commitItems(new Map());
    commitTotal(0);
    commitLoadedInitial(false);
    setLoading(true);
    viewRangeRef.current = {
      end: PAGE_SIZE - 1,
      start: 0,
    };

    void fetchRange(0, PAGE_SIZE - 1, {
      force: true,
      token,
    });
  }, [
    commitItems,
    commitLoadedInitial,
    commitTotal,
    fetchRange,
    resetLoadingRanges,
  ]);

  const reloadCurrentRange = useCallback(async () => {
    const token = requestTokenRef.current + 1;
    requestTokenRef.current = token;
    staleRangeRef.current = false;
    resetLoadingRanges();
    commitItems(new Map());

    const { end, start } = viewRangeRef.current;
    await fetchRange(start - PRELOAD_ROWS, end + PRELOAD_ROWS, {
      force: true,
      token,
    });
  }, [commitItems, fetchRange, resetLoadingRanges]);

  const loadRange = useCallback(
    (startIndex: number, endIndex: number) => {
      viewRangeRef.current = {
        end: Math.max(startIndex, endIndex),
        start: Math.max(0, Math.min(startIndex, endIndex)),
      };

      void fetchRange(startIndex - PRELOAD_ROWS, endIndex + PRELOAD_ROWS, {
        token: requestTokenRef.current,
      });
    },
    [fetchRange],
  );

  const getItem = useCallback(
    (index: number) => {
      return items.get(index) ?? null;
    },
    [items],
  );

  const findItemById = useCallback(
    (id: string) => {
      for (const item of items.values()) {
        if (item.id === id) return item;
      }

      return null;
    },
    [items],
  );

  const getItemIndexById = useCallback(
    (id: string) => {
      for (const [index, item] of items) {
        if (item.id === id) return index;
      }

      return null;
    },
    [items],
  );

  const removeItemById = useCallback(
    (id: string) => {
      const removeIndex = getItemIndexById(id);
      if (removeIndex === null) return;

      const token = requestTokenRef.current + 1;
      requestTokenRef.current = token;
      resetLoadingRanges();
      const wasStale = staleRangeRef.current;
      staleRangeRef.current = false;

      const nextItems = new Map<number, ClipboardItem>();
      for (const [index, item] of itemsRef.current) {
        if (item.id === id) continue;

        nextItems.set(index > removeIndex ? index - 1 : index, item);
      }

      const nextTotal = Math.max(0, totalRef.current - 1);
      trimCache(nextItems, viewRangeRef.current, nextTotal);
      commitItems(nextItems);
      commitTotal(nextTotal);
      if (wasStale) itemsRef.current = new Map();
      void fetchRange(
        viewRangeRef.current.start - PRELOAD_ROWS,
        viewRangeRef.current.end + PRELOAD_ROWS,
        {
          force: true,
          token,
        },
      );
    },
    [
      commitItems,
      commitTotal,
      fetchRange,
      getItemIndexById,
      resetLoadingRanges,
    ],
  );

  const patchItemById = useCallback(
    (id: string, patch: Partial<ClipboardItem>) => {
      const token = requestTokenRef.current + 1;
      requestTokenRef.current = token;
      resetLoadingRanges();
      const index = getItemIndexById(id);
      const current = index === null ? null : itemsRef.current.get(index);

      if (index !== null && current) {
        const nextItems = new Map(itemsRef.current);
        nextItems.set(index, { ...current, ...patch });
        commitItems(nextItems);
      }

      void fetchRange(
        viewRangeRef.current.start - PRELOAD_ROWS,
        viewRangeRef.current.end + PRELOAD_ROWS,
        { token },
      );
    },
    [commitItems, fetchRange, getItemIndexById, resetLoadingRanges],
  );

  useEffect(() => {
    queryRef.current = {
      favorite: query.favorite,
      group: query.group,
      groupId: query.groupId,
      keyword: query.keyword,
      kind: query.kind,
      pinned: query.pinned,
      sort: query.sort,
    };
    resetAndReload();

    return () => {
      requestTokenRef.current += 1;
      staleRangeRef.current = false;
      loadingRangesRef.current = [];
    };
  }, [
    resetAndReload,
    query.favorite,
    query.group,
    query.groupId,
    query.keyword,
    query.kind,
    query.pinned,
    query.sort,
  ]);

  return {
    findItemById,
    getItem,
    getItemIndexById,
    invalidate,
    loadedInitial,
    loadedItems: items,
    loading,
    loadingMore: loadingRangeCount > 0 && loadedInitial,
    loadRange,
    patchItemById,
    reload,
    reloadCurrentRange,
    removeItemById,
    total,
  };
};

function normalizeFetchRange(
  rawStartIndex: number,
  rawEndIndex: number,
  total: number,
): ClipboardItemsRange | null {
  if (rawEndIndex < 0) return null;

  const maxKnownIndex = total > 0 ? total - 1 : Math.max(0, rawEndIndex);
  const clampedStart = Math.max(0, Math.min(rawStartIndex, maxKnownIndex));
  const clampedEnd = Math.max(
    clampedStart,
    Math.min(rawEndIndex, maxKnownIndex),
  );
  const start = Math.floor(clampedStart / PAGE_SIZE) * PAGE_SIZE;
  const end = Math.ceil((clampedEnd + 1) / PAGE_SIZE) * PAGE_SIZE - 1;

  return {
    end: total > 0 ? Math.min(end, total - 1) : end,
    start,
  };
}

function isRangeLoaded(
  items: Map<number, ClipboardItem>,
  range: ClipboardItemsRange,
) {
  for (let index = range.start; index <= range.end; index += 1) {
    if (!items.has(index)) return false;
  }

  return true;
}

function trimCache(
  items: Map<number, ClipboardItem>,
  viewRange: ClipboardItemsRange,
  total: number,
) {
  for (const [index] of items) {
    if (index >= total) items.delete(index);
  }

  if (items.size <= CACHE_MAX_ROWS) return;

  const center = Math.floor((viewRange.start + viewRange.end) / 2);
  const keepStart = Math.max(0, center - CACHE_KEEP_RADIUS);
  const keepEnd = Math.min(total - 1, center + CACHE_KEEP_RADIUS);
  const leadingPinnedEnd = getLeadingPinnedEnd(items);

  for (const [index] of items) {
    if (index <= leadingPinnedEnd) continue;
    if (index < keepStart || index > keepEnd) items.delete(index);
  }
}

function getLeadingPinnedEnd(items: Map<number, ClipboardItem>) {
  let index = 0;
  let lastPinnedIndex = -1;

  while (true) {
    const item = items.get(index);
    if (!item?.isPinned) return lastPinnedIndex;

    lastPinnedIndex = index;
    index += 1;
  }
}
