import type { TransferPhase, TransferTask } from "@/commands";

const terminalPhases: TransferPhase[] = ["completed", "failed", "canceled"];
const MAX_RETAINED_TRANSFER_TASKS = 100;

export function isTransferActive(task: TransferTask) {
  return !terminalPhases.includes(task.phase);
}

export function transferRouteIconClass(task: TransferTask) {
  if (task.phase === "preparing") return "i-lucide:loader-circle";
  return task.route === "lan" ? "i-lucide:wifi" : "i-lucide:cloud";
}

/** Mirrors Rust's replacement rule so one item exposes only its latest download attempt. */
export function upsertTransferTask(
  current: Map<string, TransferTask>,
  task: TransferTask,
) {
  const next = new Map(current);
  if (task.direction === "download") {
    for (const [id, existing] of next) {
      if (
        id !== task.id &&
        existing.direction === "download" &&
        existing.itemId === task.itemId &&
        !isTransferActive(existing)
      ) {
        next.delete(id);
      }
    }
  }
  next.set(task.id, task);
  return pruneTransferTasks(next);
}

/** Keeps every active transfer and only the newest bounded terminal history. */
function pruneTransferTasks(tasks: Map<string, TransferTask>) {
  let removeCount = tasks.size - MAX_RETAINED_TRANSFER_TASKS;
  if (removeCount <= 0) return tasks;

  for (const [id, task] of tasks) {
    if (isTransferActive(task)) continue;
    tasks.delete(id);
    removeCount -= 1;
    if (removeCount === 0) break;
  }
  return tasks;
}

/** Fills an initial snapshot without replacing events that arrived while the command was pending. */
export function mergeTransferTaskSnapshot(
  current: Map<string, TransferTask>,
  snapshot: TransferTask[],
) {
  let next = new Map(current);
  for (const task of snapshot) {
    if (next.has(task.id)) continue;
    if (
      task.direction === "download" &&
      [...next.values()].some((existing) => {
        return (
          existing.direction === "download" && existing.itemId === task.itemId
        );
      })
    ) {
      continue;
    }
    next = upsertTransferTask(next, task);
  }
  return next;
}

/** Combines parallel file blobs into one user-visible upload per event and route. */
export function aggregateTransferTasks(tasks: TransferTask[]) {
  const uploads = new Map<string, TransferTask[]>();
  const downloads: TransferTask[] = [];
  for (const task of tasks) {
    if (task.direction === "download") {
      downloads.push(task);
      continue;
    }
    const key = [task.operationId, task.route, task.target].join("\0");
    const group = uploads.get(key) ?? [];
    group.push(task);
    uploads.set(key, group);
  }

  const aggregated = [...uploads.values()].map((group) => {
    const active = group.filter(isTransferActive);
    const cancellable = active.find((task) => task.cancellable);
    const representative = cancellable ?? active[0] ?? group[0];
    const totalBytes = Math.max(
      ...group.map((task) => task.operationTotalBytes),
    );
    const measuredTransferredBytes = group.reduce((total, task) => {
      return total + task.transferredBytes;
    }, 0);
    const speedBytesPerSecond = active.reduce((total, task) => {
      return total + task.speedBytesPerSecond;
    }, 0);
    const phase = aggregatePhase(group, active);
    const transferredBytes =
      phase === "completed"
        ? totalBytes
        : Math.min(measuredTransferredBytes, totalBytes);

    return {
      ...representative,
      cancellable: cancellable !== void 0,
      elapsedMs: Math.max(...group.map((task) => task.elapsedMs)),
      error: group.find((task) => task.error)?.error ?? null,
      etaSeconds:
        speedBytesPerSecond > 0 && transferredBytes < totalBytes
          ? Math.floor((totalBytes - transferredBytes) / speedBytesPerSecond)
          : null,
      phase,
      speedBytesPerSecond,
      totalBytes,
      transferredBytes,
    } satisfies TransferTask;
  });

  return [...downloads, ...aggregated];
}

function aggregatePhase(group: TransferTask[], active: TransferTask[]) {
  if (active.length > 0) {
    for (const phase of [
      "preparing",
      "transferring",
      "confirming",
      "connecting",
      "queued",
    ] as const) {
      if (active.some((task) => task.phase === phase)) return phase;
    }
  }
  if (group.every((task) => task.phase === "completed")) return "completed";
  if (group.some((task) => task.phase === "failed")) return "failed";
  return "canceled";
}
