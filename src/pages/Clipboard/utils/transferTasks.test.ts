import assert from "node:assert/strict";
import test from "node:test";
import type { TransferTask } from "@/commands";
import {
  sortTransferTasksForDisplay,
  transferRouteIconClass,
  upsertTransferTask,
} from "./transferTasks";

function transferTask(overrides: Partial<TransferTask>): TransferTask {
  return {
    cancellable: true,
    direction: "upload",
    elapsedMs: 1_000,
    error: null,
    etaSeconds: null,
    fileName: null,
    id: "operation-1",
    itemId: "item-1",
    operationId: "event-1",
    phase: "transferring",
    revision: 1,
    route: "cloud",
    speedBytesPerSecond: 10,
    startedAtMs: 1_794_542_016_000,
    target: "cloud",
    totalBytes: 100,
    transferredBytes: 40,
    ...overrides,
  };
}

test("keeps the newest Rust-owned operation revision", () => {
  const current = transferTask({ revision: 2, transferredBytes: 60 });
  const older = transferTask({ revision: 1, transferredBytes: 20 });
  const newer = transferTask({ revision: 3, transferredBytes: 80 });

  const unchanged = upsertTransferTask(new Map([[current.id, current]]), older);
  const updated = upsertTransferTask(unchanged, newer);

  assert.equal(unchanged.get(current.id)?.transferredBytes, 60);
  assert.equal(updated.get(current.id)?.transferredBytes, 80);
});

test("uses preparation and route-specific icons", () => {
  assert.equal(
    transferRouteIconClass(transferTask({ phase: "preparing" })),
    "i-lucide:loader-circle",
  );
  assert.equal(
    transferRouteIconClass(transferTask({ route: "lan" })),
    "i-lucide:wifi",
  );
  assert.equal(
    transferRouteIconClass(transferTask({ target: "import" })),
    "i-lucide:hard-drive-download",
  );
});

test("shows active transfers before terminal history", () => {
  const tasks = sortTransferTasksForDisplay([
    transferTask({ id: "new-completed", phase: "completed", revision: 12 }),
    transferTask({ id: "new-active", revision: 9 }),
    transferTask({ id: "old-active", revision: 2 }),
    transferTask({ id: "old-completed", phase: "completed", revision: 4 }),
  ]);

  assert.deepEqual(
    tasks.map((task) => task.id),
    ["new-active", "old-active", "new-completed", "old-completed"],
  );
});
