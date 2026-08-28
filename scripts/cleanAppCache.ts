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
const result = spawnSync(
  cargo,
  [
    "clean",
    "--package",
    "EcoPaste",
    "--manifest-path",
    "src-tauri/Cargo.toml",
    ...process.argv.slice(2),
  ],
  {
    cwd: projectRoot,
    stdio: "inherit",
  },
);

if (result.error) {
  throw result.error;
}

process.exitCode = result.status ?? 1;
