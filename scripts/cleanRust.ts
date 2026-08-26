import { spawnSync } from "node:child_process";
import { homedir } from "node:os";
import { resolve } from "node:path";

const projectRoot = resolve(import.meta.dirname, "..");
const cargoHome = process.env.CARGO_HOME ?? resolve(homedir(), ".cargo");
const cargo = resolve(
  cargoHome,
  "bin",
  process.platform === "win32" ? "cargo.exe" : "cargo",
);
const manifests = ["src-tauri/Cargo.toml", "sync-server/Cargo.toml"];

for (const manifest of manifests) {
  const result = spawnSync(cargo, ["clean", "--manifest-path", manifest], {
    cwd: projectRoot,
    stdio: "inherit",
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
    break;
  }
}
