import assert from "node:assert/strict";
import test from "node:test";
import { BYTES_PER_GB } from "../constants";
import { formatBytes, storageMeterPercent, storageTone } from "./storageUsage";

test("storage labels use binary GB and MB", () => {
  assert.equal(formatBytes(100 * 1024 ** 2), "100 MB");
  assert.equal(formatBytes(Math.round(BYTES_PER_GB / 10)), "102 MB");
  assert.equal(formatBytes(1024 ** 3), "1.0 GB");
});

test("storage meter uses the configured limit, including fractional GB", () => {
  assert.equal(storageMeterPercent(BYTES_PER_GB, 4 * BYTES_PER_GB), 25);
  assert.equal(storageMeterPercent(BYTES_PER_GB / 4, BYTES_PER_GB / 2), 50);
});

test("storage meter handles unlimited, empty and over-limit usage", () => {
  assert.equal(storageMeterPercent(BYTES_PER_GB, 0), 0);
  assert.equal(storageMeterPercent(0, BYTES_PER_GB), 0);
  assert.equal(storageMeterPercent(2 * BYTES_PER_GB, BYTES_PER_GB), 100);
});

test("storage tone follows the configured quota policy", () => {
  assert.equal(storageTone(69, 90, 70).text, "text-ant-success");
  assert.equal(storageTone(70, 90, 70).text, "text-ant-warning");
  assert.equal(storageTone(90, 90, 70).text, "text-ant-error");
});
