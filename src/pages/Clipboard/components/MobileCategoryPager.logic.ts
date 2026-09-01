const CATEGORY_DRAG_DISTANCE_RATIO = 0.2;
const CATEGORY_DRAG_MIN_FLING_DISTANCE = 12;
const CATEGORY_DRAG_VELOCITY_THRESHOLD = 450;

interface ResolveCategoryPagerIndexInput {
  currentIndex: number;
  offsetX: number;
  pageCount: number;
  velocityX: number;
  viewportWidth: number;
}

/** 根据单次拖动的距离与末速度决定相邻目标页，结果始终限制在首尾边界内。 */
export function resolveCategoryPagerIndex(
  input: ResolveCategoryPagerIndexInput,
) {
  const { currentIndex, offsetX, pageCount, velocityX, viewportWidth } = input;
  if (pageCount <= 0 || viewportWidth <= 0) return 0;

  const crossedDistance =
    Math.abs(offsetX) >= viewportWidth * CATEGORY_DRAG_DISTANCE_RATIO;
  const flung =
    Math.abs(offsetX) >= CATEGORY_DRAG_MIN_FLING_DISTANCE &&
    Math.abs(velocityX) >= CATEGORY_DRAG_VELOCITY_THRESHOLD;

  let direction = 0;
  if (crossedDistance) {
    direction = offsetX < 0 ? 1 : -1;
  } else if (flung) {
    direction = velocityX < 0 ? 1 : -1;
  }

  return Math.max(0, Math.min(pageCount - 1, currentIndex + direction));
}
