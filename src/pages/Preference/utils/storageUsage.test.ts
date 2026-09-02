import assert from "node:assert/strict";
import test from "node:test";
import { STORAGE_ERROR_BYTES, STORAGE_WARNING_BYTES } from "../constants";
import { storageMeterPercent } from "./storageUsage";

test("storage meter uses the actual ratio below the warning threshold", () => {
  assert.equal(storageMeterPercent(94 * 1024 * 1024), 9.1796875);
});

test("storage meter switches target at the warning threshold", () => {
  assert.equal(storageMeterPercent(STORAGE_WARNING_BYTES), 50);
});

test("storage meter clamps empty and oversized usage", () => {
  assert.equal(storageMeterPercent(0), 0);
  assert.equal(storageMeterPercent(STORAGE_ERROR_BYTES * 2), 100);
});
