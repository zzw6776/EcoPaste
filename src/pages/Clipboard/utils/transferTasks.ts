import type { TransferPhase, TransferTask } from "@/commands";

const terminalPhases: TransferPhase[] = ["completed", "failed", "canceled"];

export function isTransferActive(task: TransferTask) {
  return !terminalPhases.includes(task.phase);
}

export function transferRouteIconClass(task: TransferTask) {
  if (task.target === "import") return "i-lucide:hard-drive-download";
  if (task.phase === "preparing") return "i-lucide:loader-circle";
  return task.route === "lan" ? "i-lucide:wifi" : "i-lucide:cloud";
}

/** Applies one Rust-owned operation update without recreating transfer semantics in the UI. */
export function upsertTransferTask(
  current: Map<string, TransferTask>,
  task: TransferTask,
) {
  const existing = current.get(task.id);
  if (existing && existing.revision >= task.revision) return current;

  const next = new Map(current);
  next.set(task.id, task);
  return next;
}

/** Places active work first while keeping newest-first order inside each section. */
export function sortTransferTasksForDisplay(tasks: TransferTask[]) {
  return [...tasks].sort((left, right) => {
    const leftActive = isTransferActive(left);
    const rightActive = isTransferActive(right);
    if (leftActive !== rightActive) return leftActive ? -1 : 1;
    if (left.revision === right.revision) return 0;
    return left.revision > right.revision ? -1 : 1;
  });
}
