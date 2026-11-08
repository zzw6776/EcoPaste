import assert from "node:assert/strict";
import test from "node:test";
import type { TransferTask } from "@/commands";
import {
  aggregateTransferTasks,
  mergeTransferTaskSnapshot,
  transferRouteIconClass,
  upsertTransferTask,
} from "./transferTasks";

function uploadTask(overrides: Partial<TransferTask>): TransferTask {
  return {
    cancellable: true,
    direction: "upload",
    elapsedMs: 1_000,
    error: null,
    etaSeconds: null,
    fileName: null,
    id: "blob-1",
    itemId: "item-1",
    operationId: "event-1",
    operationTotalBytes: 100,
    phase: "transferring",
    route: "cloud",
    speedBytesPerSecond: 10,
    target: "cloud",
    totalBytes: 100,
    transferredBytes: 40,
    ...overrides,
  };
}

test("aggregates parallel blobs only within the same event route and target", () => {
  const tasks = aggregateTransferTasks([
    uploadTask({}),
    uploadTask({
      id: "blob-2",
      operationTotalBytes: 300,
      speedBytesPerSecond: 20,
      totalBytes: 200,
      transferredBytes: 50,
    }),
    uploadTask({ id: "peer-blob", route: "lan", target: "peer:phone" }),
  ]);

  assert.equal(tasks.length, 2);
  const cloud = tasks.find((task) => task.route === "cloud");
  assert.equal(cloud?.totalBytes, 300);
  assert.equal(cloud?.transferredBytes, 90);
  assert.equal(cloud?.speedBytesPerSecond, 30);
  assert.equal(cloud?.etaSeconds, 7);
});

test("reports upload preparation before transport phases", () => {
  const [task] = aggregateTransferTasks([
    uploadTask({
      phase: "preparing",
      speedBytesPerSecond: 25,
      transferredBytes: 50,
    }),
  ]);

  assert.equal(task?.phase, "preparing");
  assert.equal(task?.transferredBytes, 50);
  assert.equal(task?.speedBytesPerSecond, 25);
  assert.equal(transferRouteIconClass(task), "i-lucide:loader-circle");
});

test("uses a genuinely cancellable child as the aggregate representative", () => {
  const [task] = aggregateTransferTasks([
    uploadTask({
      cancellable: false,
      id: "confirming",
      phase: "confirming",
      transferredBytes: 100,
    }),
    uploadTask({ id: "transferring" }),
  ]);

  assert.equal(task?.id, "transferring");
  assert.equal(task?.cancellable, true);
});

test("does not expose cancellation after every child closes its window", () => {
  const [task] = aggregateTransferTasks([
    uploadTask({ cancellable: false, phase: "confirming" }),
  ]);

  assert.equal(task?.phase, "confirming");
  assert.equal(task?.cancellable, false);
});

test("keeps downloads independent and reports a failed upload group", () => {
  const download = uploadTask({
    direction: "download",
    id: "download-1",
    operationId: "remote-event",
  });
  const tasks = aggregateTransferTasks([
    uploadTask({ phase: "completed", transferredBytes: 100 }),
    uploadTask({
      cancellable: false,
      error: "network unavailable",
      id: "blob-2",
      phase: "failed",
      transferredBytes: 10,
    }),
    download,
  ]);

  assert.equal(tasks.length, 2);
  assert.equal(tasks[0]?.id, download.id);
  assert.equal(tasks[1]?.phase, "failed");
  assert.equal(tasks[1]?.error, "network unavailable");
});

test("replaces a terminal download when a retry starts", () => {
  const failed = uploadTask({
    cancellable: false,
    direction: "download",
    id: "failed-download",
    phase: "failed",
  });
  const retry = uploadTask({
    direction: "download",
    id: "retry-download",
    phase: "queued",
  });

  const tasks = upsertTransferTask(new Map([[failed.id, failed]]), retry);

  assert.deepEqual([...tasks.keys()], [retry.id]);
});

test("does not overwrite a newer event with an older command snapshot", () => {
  const connecting = uploadTask({
    direction: "download",
    id: "download",
    phase: "connecting",
  });
  const queued = { ...connecting, phase: "queued" as const };

  const tasks = mergeTransferTaskSnapshot(
    new Map([[connecting.id, connecting]]),
    [queued],
  );

  assert.equal(tasks.get(connecting.id)?.phase, "connecting");
});

test("bounds terminal transfer history while preserving active tasks", () => {
  const active = uploadTask({ id: "active" });
  let tasks = new Map([[active.id, active]]);
  for (let index = 0; index < 100; index += 1) {
    const terminal = uploadTask({
      cancellable: false,
      id: `terminal-${index}`,
      phase: "completed",
    });
    tasks = upsertTransferTask(tasks, terminal);
  }

  assert.equal(tasks.size, 100);
  assert.equal(tasks.has(active.id), true);
  assert.equal(tasks.has("terminal-0"), false);
  assert.equal(tasks.has("terminal-99"), true);
});
