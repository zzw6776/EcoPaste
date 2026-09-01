import assert from "node:assert/strict";
import test from "node:test";
import { resolveCategoryPagerIndex } from "./MobileCategoryPager.logic";

const BASE_INPUT = {
  currentIndex: 2,
  pageCount: 5,
  viewportWidth: 400,
};

test("short and slow drag snaps back", () => {
  assert.equal(
    resolveCategoryPagerIndex({
      ...BASE_INPUT,
      offsetX: -40,
      velocityX: -100,
    }),
    2,
  );
});

test("distance threshold changes at most one adjacent page", () => {
  assert.equal(
    resolveCategoryPagerIndex({
      ...BASE_INPUT,
      offsetX: -300,
      velocityX: -2_000,
    }),
    3,
  );
  assert.equal(
    resolveCategoryPagerIndex({
      ...BASE_INPUT,
      offsetX: 300,
      velocityX: 2_000,
    }),
    1,
  );
});

test("short fling follows terminal velocity, including reversed velocity", () => {
  assert.equal(
    resolveCategoryPagerIndex({
      ...BASE_INPUT,
      offsetX: -20,
      velocityX: -800,
    }),
    3,
  );
  assert.equal(
    resolveCategoryPagerIndex({
      ...BASE_INPUT,
      offsetX: -20,
      velocityX: 800,
    }),
    1,
  );
});

test("first and last page stay inside fixed boundaries", () => {
  assert.equal(
    resolveCategoryPagerIndex({
      ...BASE_INPUT,
      currentIndex: 0,
      offsetX: 200,
      velocityX: 1_000,
    }),
    0,
  );
  assert.equal(
    resolveCategoryPagerIndex({
      ...BASE_INPUT,
      currentIndex: 4,
      offsetX: -200,
      velocityX: -1_000,
    }),
    4,
  );
});
