import assert from "node:assert/strict";
import test from "node:test";
import { getMissingClipboardRanges } from "./clipboardItemRanges";

/** 创建已加载的连续行，用于模拟真实的分页与缓存淘汰边界。 */
function loadedRows(start: number, end: number) {
  const items = new Map<number, boolean>();
  for (let index = start; index <= end; index += 1) {
    items.set(index, true);
  }
  return items;
}

test("扩展预加载范围时只查询新增行", () => {
  assert.deepEqual(
    getMissingClipboardRanges(loadedRows(0, 29), [], { end: 59, start: 0 }),
    [{ end: 59, start: 30 }],
  );
});

test("扣除已加载和多个重叠的在途请求", () => {
  assert.deepEqual(
    getMissingClipboardRanges(
      loadedRows(0, 29),
      [
        { end: 59, start: 30 },
        { end: 74, start: 50 },
      ],
      { end: 89, start: 0 },
    ),
    [{ end: 89, start: 75 }],
  );
});

test("缓存中间或两端有缺口时只查询缺口", () => {
  const items = loadedRows(0, 29);
  for (let index = 20; index < 25; index += 1) items.delete(index);
  assert.deepEqual(
    getMissingClipboardRanges(items, [], { end: 59, start: 0 }),
    [
      { end: 24, start: 20 },
      { end: 59, start: 30 },
    ],
  );
  assert.deepEqual(
    getMissingClipboardRanges(loadedRows(60, 89), [], { end: 119, start: 30 }),
    [
      { end: 59, start: 30 },
      { end: 119, start: 90 },
    ],
  );
});

test("多个在途请求已覆盖目标时不再重复请求", () => {
  assert.deepEqual(
    getMissingClipboardRanges(
      new Map(),
      [
        { end: 29, start: 0 },
        { end: 59, start: 30 },
      ],
      { end: 59, start: 0 },
    ),
    [],
  );
});

test("失败请求释放后可以重读且尊重列表末尾", () => {
  const items = loadedRows(0, 29);
  const target = { end: 34, start: 0 };
  assert.deepEqual(
    getMissingClipboardRanges(items, [{ end: 34, start: 30 }], target),
    [],
  );
  assert.deepEqual(getMissingClipboardRanges(items, [], target), [
    { end: 34, start: 30 },
  ]);
});
