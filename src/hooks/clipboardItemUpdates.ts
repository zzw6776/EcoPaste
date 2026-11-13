import type {
  ClipboardItem,
  ClipboardItemQuery,
  ClipboardItemSort,
} from "@/types/clipboard";

interface UpsertClipboardItemCacheInput {
  existing: boolean;
  item: ClipboardItem;
  items: Map<number, ClipboardItem>;
  maxItems: number;
  query: ClipboardItemQuery;
  total: number;
}

interface UpsertClipboardItemCacheResult {
  items: Map<number, ClipboardItem>;
  total: number;
}

/**
 * 在已加载的连续顶部缓存中插入或移动一条权威记录。
 * 调用方只在无关键词查询时使用；位置超出已知排序边界时返回 null，由 Rust 重读分页。
 */
export function upsertClipboardItemCache(
  input: UpsertClipboardItemCacheInput,
): UpsertClipboardItemCacheResult | null {
  const prefix = getLoadedPrefix(input.items);
  const loadedExisting = prefix.some((current) => {
    return current.id === input.item.id;
  });
  const matches = itemMatchesQuery(input.item, input.query);
  const boundary = prefix[prefix.length - 1];
  if (
    matches &&
    prefix.length < input.total &&
    (!boundary ||
      compareClipboardItems(input.item, boundary, input.query.sort) > 0)
  ) {
    return null;
  }

  let total = input.total;
  if (!input.existing && !loadedExisting && matches) total += 1;
  if (loadedExisting && !matches) total = Math.max(0, total - 1);

  const nextPrefix = prefix.filter((current) => {
    return current.id !== input.item.id;
  });
  if (matches) nextPrefix.push(input.item);
  nextPrefix.sort((left, right) => {
    return compareClipboardItems(left, right, input.query.sort);
  });

  const inserted = !input.existing && !loadedExisting && matches;
  const targetSize = Math.min(
    total,
    input.maxItems,
    Math.max(prefix.length + (inserted ? 1 : 0), matches ? 1 : 0),
  );
  const items = new Map<number, ClipboardItem>();
  nextPrefix.slice(0, targetSize).forEach((current, index) => {
    items.set(index, current);
  });

  return { items, total };
}

function getLoadedPrefix(items: Map<number, ClipboardItem>) {
  const prefix: ClipboardItem[] = [];
  for (let index = 0; ; index += 1) {
    const item = items.get(index);
    if (!item) return prefix;
    prefix.push(item);
  }
}

function itemMatchesQuery(item: ClipboardItem, query: ClipboardItemQuery) {
  if (query.kind !== void 0 && item.kind !== query.kind) return false;
  if (query.groupId !== void 0 && item.groupId !== query.groupId) return false;
  if (query.favorite !== void 0 && item.isFavorite !== query.favorite) {
    return false;
  }
  if (query.pinned !== void 0 && item.isPinned !== query.pinned) return false;
  if (query.group === "favorite" && !item.isFavorite) return false;
  if (
    query.group !== void 0 &&
    query.group !== "all" &&
    query.group !== "favorite" &&
    item.kind !== query.group
  ) {
    return false;
  }

  return true;
}

function compareClipboardItems(
  left: ClipboardItem,
  right: ClipboardItem,
  sort: ClipboardItemSort | undefined,
) {
  if (left.isPinned !== right.isPinned) return left.isPinned ? -1 : 1;

  let primary = 0;
  if (sort === "createdAtDesc") {
    primary = compareDateDescending(left.createdAt, right.createdAt);
  } else if (sort === "useCountDesc") {
    primary = right.useCount - left.useCount;
    if (primary === 0) {
      primary = compareDateDescending(left.createdAt, right.createdAt);
    }
  } else {
    primary = compareDateDescending(left.updatedAt, right.updatedAt);
    if (primary === 0) {
      primary = compareDateDescending(left.createdAt, right.createdAt);
    }
  }
  if (primary !== 0) return primary;

  if (left.id === right.id) return 0;
  return left.id < right.id ? -1 : 1;
}

function compareDateDescending(left: string, right: string) {
  return Date.parse(right) - Date.parse(left);
}
