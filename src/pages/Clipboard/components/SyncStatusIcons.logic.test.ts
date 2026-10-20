import assert from "node:assert/strict";
import test from "node:test";
import { createSyncStatusRefresh } from "./SyncStatusIcons.logic";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}

test("coalesces an event burst and reads the latest status once more", async () => {
  const reads = [deferred<number>(), deferred<number>()];
  const written: number[] = [];
  let readCount = 0;
  const refresh = createSyncStatusRefresh({
    isActive: () => {
      return true;
    },
    onError: (error) => {
      throw error;
    },
    read: () => {
      return reads[readCount++].promise;
    },
    write: (status) => {
      written.push(status);
    },
  });

  const first = refresh();
  assert.ok(first);
  await Promise.resolve();
  assert.equal(readCount, 1);

  const pending = refresh();
  assert.ok(pending);
  for (let index = 0; index < 100; index += 1) {
    assert.equal(refresh(), pending);
  }

  reads[0].resolve(1);
  await first;
  assert.equal(readCount, 2);
  assert.deepEqual(written, [1]);

  reads[1].resolve(2);
  await pending;
  assert.deepEqual(written, [1, 2]);
});

test("skips hidden updates and refreshes once after becoming active", async () => {
  const reads = [deferred<number>(), deferred<number>()];
  const written: number[] = [];
  let active = false;
  let readCount = 0;
  const refresh = createSyncStatusRefresh({
    isActive: () => {
      return active;
    },
    onError: (error) => {
      throw error;
    },
    read: () => {
      return reads[readCount++].promise;
    },
    write: (status) => {
      written.push(status);
    },
  });

  for (let index = 0; index < 100; index += 1) {
    assert.equal(refresh(), void 0);
  }
  assert.equal(readCount, 0);

  active = true;
  const visible = refresh();
  assert.ok(visible);
  await Promise.resolve();
  assert.equal(readCount, 1);

  active = false;
  reads[0].resolve(1);
  await visible;
  assert.deepEqual(written, []);

  active = true;
  const reopened = refresh();
  assert.ok(reopened);
  await Promise.resolve();
  assert.equal(readCount, 2);
  reads[1].resolve(2);
  await reopened;
  assert.deepEqual(written, [2]);
});
