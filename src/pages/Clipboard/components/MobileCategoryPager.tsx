import { animate, motion, type PanInfo, useMotionValue } from "motion/react";
import type { FC, MouseEvent, ReactNode } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { resolveCategoryPagerIndex } from "./MobileCategoryPager.logic";

const CATEGORY_EDGE_ELASTIC = 0.12;

interface MobileCategoryPagerProps {
  index: number;
  onIndexChange: (index: number) => void;
  pageKeys: readonly string[];
  renderPage: (index: number, active: boolean) => ReactNode;
}

/**
 * Android 分类分页轨道，只负责横向手势、相邻页生命周期和索引吸附。
 */
const MobileCategoryPager: FC<MobileCategoryPagerProps> = (props) => {
  const { index, onIndexChange, pageKeys, renderPage } = props;
  const pageCount = pageKeys.length;
  const [viewportWidth, setViewportWidth] = useState(0);
  const [renderAnchorIndex, setRenderAnchorIndex] = useState(index);
  const viewportRef = useRef<HTMLDivElement>(null);
  const indexRef = useRef(index);
  const committedDragIndexRef = useRef<number | null>(null);
  const previousWidthRef = useRef(0);
  const suppressClickRef = useRef(false);
  const clearClickSuppressionRef = useRef(0);
  const animationRef = useRef<ReturnType<typeof animate> | null>(null);
  const x = useMotionValue(0);

  const snapToIndex = useCallback(
    (nextIndex: number, animated: boolean) => {
      const target = -viewportWidth * nextIndex;
      animationRef.current?.stop();
      animationRef.current = null;

      if (!animated || viewportWidth <= 0) {
        x.set(target);
        return null;
      }

      const animation = animate(x, target, {
        bounce: 0,
        duration: 0.24,
        ease: [0.22, 1, 0.36, 1],
        type: "tween",
      });
      animationRef.current = animation;
      return animation;
    },
    [viewportWidth, x],
  );

  useEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;

    const updateWidth = () => {
      setViewportWidth(viewport.clientWidth);
    };
    updateWidth();

    const observer = new ResizeObserver(updateWidth);
    observer.observe(viewport);

    return () => {
      observer.disconnect();
    };
  }, []);

  useEffect(() => {
    const widthChanged = previousWidthRef.current !== viewportWidth;
    const indexChanged = indexRef.current !== index;
    const committedDrag =
      !widthChanged && committedDragIndexRef.current === index;
    indexRef.current = index;
    previousWidthRef.current = viewportWidth;
    committedDragIndexRef.current = null;

    if (committedDrag) return;

    const animation = snapToIndex(index, indexChanged && !widthChanged);
    if (!animation) {
      setRenderAnchorIndex(index);
      return;
    }

    void animation.then(() => {
      if (animationRef.current !== animation) return;

      setRenderAnchorIndex(index);
    });
  }, [index, viewportWidth, snapToIndex]);

  useEffect(() => {
    return () => {
      animationRef.current?.stop();
      window.clearTimeout(clearClickSuppressionRef.current);
    };
  }, []);

  const handleDragStart = () => {
    animationRef.current?.stop();
    animationRef.current = null;
    suppressClickRef.current = true;
    window.clearTimeout(clearClickSuppressionRef.current);
  };

  const handleDragEnd = (_event: Event, info: PanInfo) => {
    const currentIndex = indexRef.current;
    const nextIndex = resolveCategoryPagerIndex({
      currentIndex,
      offsetX: info.offset.x,
      pageCount,
      velocityX: info.velocity.x,
      viewportWidth,
    });

    indexRef.current = nextIndex;
    const animation = snapToIndex(nextIndex, true);
    if (animation) {
      void animation.then(() => {
        if (animationRef.current !== animation) return;

        setRenderAnchorIndex(nextIndex);
      });
    }
    if (nextIndex !== currentIndex) {
      committedDragIndexRef.current = nextIndex;
      onIndexChange(nextIndex);
    }

    clearClickSuppressionRef.current = window.setTimeout(() => {
      suppressClickRef.current = false;
    }, 0);
  };

  const handleClickCapture = (event: MouseEvent<HTMLDivElement>) => {
    if (!suppressClickRef.current) return;

    event.preventDefault();
    event.stopPropagation();
  };

  return (
    <div
      className="mobile-category-pager relative min-h-0 w-full flex-1 overflow-hidden"
      onClickCapture={handleClickCapture}
      ref={viewportRef}
    >
      <motion.div
        className="flex size-full will-change-transform"
        drag="x"
        dragConstraints={{
          left: -viewportWidth * (pageCount - 1),
          right: 0,
        }}
        dragDirectionLock
        dragElastic={CATEGORY_EDGE_ELASTIC}
        dragMomentum={false}
        onDragEnd={handleDragEnd}
        onDragStart={handleDragStart}
        style={{ x }}
      >
        {pageKeys.map((pageKey, pageIndex) => {
          const active = pageIndex === index;
          const firstRenderedIndex = Math.min(index, renderAnchorIndex) - 1;
          const lastRenderedIndex = Math.max(index, renderAnchorIndex) + 1;
          const preloaded =
            pageIndex >= firstRenderedIndex && pageIndex <= lastRenderedIndex;

          return (
            <section
              className="h-full w-full shrink-0 overflow-hidden"
              key={pageKey}
            >
              {preloaded ? renderPage(pageIndex, active) : null}
            </section>
          );
        })}
      </motion.div>
    </div>
  );
};

export default MobileCategoryPager;
