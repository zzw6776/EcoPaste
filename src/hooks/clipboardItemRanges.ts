export interface ClipboardItemsRange {
  end: number;
  start: number;
}

/**
 * 从目标范围扣除已加载行和在途请求，合并相邻缺口，避免重复读取重叠的预加载区域。
 */
export function getMissingClipboardRanges(
  items: ReadonlyMap<number, unknown>,
  loadingRanges: readonly ClipboardItemsRange[],
  target: ClipboardItemsRange,
): ClipboardItemsRange[] {
  const missing: ClipboardItemsRange[] = [];
  let start: number | null = null;

  for (let index = target.start; index <= target.end; index += 1) {
    const covered =
      items.has(index) ||
      loadingRanges.some((range) => {
        return range.start <= index && index <= range.end;
      });

    if (!covered) {
      if (start === null) start = index;
      continue;
    }

    if (start !== null) {
      missing.push({ end: index - 1, start });
      start = null;
    }
  }

  if (start !== null) missing.push({ end: target.end, start });

  return missing;
}
