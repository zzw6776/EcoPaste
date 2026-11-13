import assert from "node:assert/strict";
import test from "node:test";
import type { ClipboardItem } from "@/types/clipboard";
import { upsertClipboardItemCache } from "./clipboardItemUpdates";

function item(id: string, patch: Partial<ClipboardItem> = {}): ClipboardItem {
  return {
    content: "",
    contentHash: id,
    createdAt: "2026-01-01T00:00:00.000Z",
    fileTypes: null,
    groupId: null,
    height: null,
    id,
    isFavorite: false,
    isPinned: false,
    isSensitive: false,
    kind: "text",
    note: null,
    platform: "macos",
    searchText: null,
    size: null,
    sourceAppId: null,
    subKind: null,
    summary: id,
    updatedAt: "2026-01-01T00:00:00.000Z",
    useCount: 1,
    width: null,
    ...patch,
  };
}

function ids(items: Map<number, ClipboardItem>) {
  return [...items.values()].map((current) => {
    return current.id;
  });
}

test("新记录增量插入顶部且重复事件不会重复增加总数", () => {
  const original = item("original");
  const inserted = item("inserted", {
    createdAt: "2026-01-02T00:00:00.000Z",
    updatedAt: "2026-01-02T00:00:00.000Z",
  });
  const first = upsertClipboardItemCache({
    existing: false,
    item: inserted,
    items: new Map([[0, original]]),
    maxItems: 180,
    query: { sort: "updatedAtDesc" },
    total: 1,
  });
  assert.ok(first);
  const repeated = upsertClipboardItemCache({
    existing: false,
    item: inserted,
    items: first.items,
    maxItems: 180,
    query: { sort: "updatedAtDesc" },
    total: first.total,
  });
  assert.ok(repeated);

  assert.deepEqual(ids(first.items), ["inserted", "original"]);
  assert.equal(first.total, 2);
  assert.deepEqual(ids(repeated.items), ["inserted", "original"]);
  assert.equal(repeated.total, 2);
});

test("复用记录按后端 useCount 规则移动但不改变总数", () => {
  const first = item("first", { useCount: 10 });
  const reused = item("reused", { useCount: 20 });
  const result = upsertClipboardItemCache({
    existing: true,
    item: reused,
    items: new Map([
      [0, first],
      [1, item("reused", { useCount: 1 })],
    ]),
    maxItems: 180,
    query: { sort: "useCountDesc" },
    total: 2,
  });
  assert.ok(result);

  assert.deepEqual(ids(result.items), ["reused", "first"]);
  assert.equal(result.total, 2);
});

test("不满足当前筛选的新记录不会进入缓存或改变总数", () => {
  const favorite = item("favorite", { isFavorite: true });
  const result = upsertClipboardItemCache({
    existing: false,
    item: item("ordinary"),
    items: new Map([[0, favorite]]),
    maxItems: 180,
    query: { favorite: true, sort: "updatedAtDesc" },
    total: 1,
  });
  assert.ok(result);

  assert.deepEqual(ids(result.items), ["favorite"]);
  assert.equal(result.total, 1);
});

test("已加载记录离开当前筛选时就地移除并扣减总数", () => {
  const result = upsertClipboardItemCache({
    existing: true,
    item: item("favorite", { isFavorite: false }),
    items: new Map([[0, item("favorite", { isFavorite: true })]]),
    maxItems: 180,
    query: { favorite: true, sort: "updatedAtDesc" },
    total: 1,
  });
  assert.ok(result);

  assert.deepEqual(ids(result.items), []);
  assert.equal(result.total, 0);
});
