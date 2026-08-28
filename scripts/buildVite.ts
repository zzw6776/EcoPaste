import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { restoreUnchangedFileTimes, snapshotFiles } from "./fileFingerprint";

const projectRoot = resolve(import.meta.dirname, "..");
const distRoot = resolve(projectRoot, "dist");
const before = await snapshotFiles(distRoot);
const result = spawnSync("vite", ["build"], {
  cwd: projectRoot,
  shell: process.platform === "win32",
  stdio: "inherit",
});

if (result.error) {
  throw result.error;
}
if (result.status !== 0) {
  process.exitCode = result.status ?? 1;
} else {
  const restored = await restoreUnchangedFileTimes(distRoot, before);
  process.stdout.write(
    `Preserved timestamps for ${restored} unchanged frontend assets.\n`,
  );
}
