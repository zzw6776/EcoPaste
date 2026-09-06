import assert from "node:assert/strict";
import { test } from "node:test";
import { createCoalescedRequest } from "./coalescedRequest";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((accept, fail) => {
    resolve = accept;
    reject = fail;
  });
  return { promise, reject, resolve };
}

test("merges a burst and reads again for updates during the active query", async () => {
  const reads = [deferred<number>(), deferred<number>()];
  let count = 0;
  const request = createCoalescedRequest(() => {
    return reads[count++].promise;
  });
  const first = request();
  assert.equal(request(), first);
  await Promise.resolve();
  assert.equal(count, 1);
  const later = request();
  assert.notEqual(later, first);
  for (let i = 0; i < 100; i += 1) assert.equal(request(), later);
  reads[0].resolve(1);
  assert.equal(await first, 1);
  assert.equal(count, 2);
  reads[1].resolve(2);
  assert.equal(await later, 2);
});

test("a failed active query does not discard the pending latest read", async () => {
  const failed = deferred<number>();
  let count = 0;
  const request = createCoalescedRequest(async () => {
    count += 1;
    if (count === 1) return await failed.promise;
    return 2;
  });
  const first = request();
  const rejected = assert.rejects(first, /failure/);
  await Promise.resolve();
  const later = request();
  failed.reject(new Error("failure"));
  await rejected;
  assert.equal(await later, 2);
  assert.equal(await request(), 2);
  assert.equal(count, 3);
});
