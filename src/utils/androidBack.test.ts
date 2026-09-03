import assert from "node:assert/strict";
import test from "node:test";
import { handleAndroidBack, registerAndroidBackHandler } from "./androidBack";

test("Android back closes registered layers in last-opened-first order", () => {
  const closed: string[] = [];
  const unregisterFirst = registerAndroidBackHandler(() => {
    closed.push("first");
  });
  const unregisterSecond = registerAndroidBackHandler(() => {
    closed.push("second");
  });

  assert.equal(handleAndroidBack(), true);
  assert.deepEqual(closed, ["second"]);

  unregisterSecond();
  assert.equal(handleAndroidBack(), true);
  assert.deepEqual(closed, ["second", "first"]);

  unregisterFirst();
  assert.equal(handleAndroidBack(), false);
});

test("Android back always closes an interaction layer before a page", () => {
  const closed: string[] = [];
  const unregisterLayer = registerAndroidBackHandler(() => {
    closed.push("layer");
  });
  const unregisterPage = registerAndroidBackHandler(() => {
    closed.push("page");
  }, "page");

  assert.equal(handleAndroidBack(), true);
  assert.deepEqual(closed, ["layer"]);

  unregisterLayer();
  assert.equal(handleAndroidBack(), true);
  assert.deepEqual(closed, ["layer", "page"]);

  unregisterPage();
  assert.equal(handleAndroidBack(), false);
});

test("Android back closes a nested child before its parent and page", () => {
  const closed: string[] = [];
  const unregisterPage = registerAndroidBackHandler(() => {
    closed.push("page");
  }, "page");
  const unregisterParent = registerAndroidBackHandler(() => {
    closed.push("parent");
  });
  const unregisterChild = registerAndroidBackHandler(() => {
    closed.push("child");
  });

  assert.equal(handleAndroidBack(), true);
  unregisterChild();
  assert.equal(handleAndroidBack(), true);
  unregisterParent();
  assert.equal(handleAndroidBack(), true);
  assert.deepEqual(closed, ["child", "parent", "page"]);

  unregisterPage();
  assert.equal(handleAndroidBack(), false);
});
