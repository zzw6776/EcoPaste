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
const androidTargetDirectory = resolve(
  projectRoot,
  "src-tauri/target/aarch64-linux-android",
);
const extraArgs = process.argv.slice(2);

/** 清理指定 target 目录中的 EcoPaste 主包产物，保留第三方依赖缓存。 */
function cleanAppArtifacts(targetDirectory?: string) {
  const args = [
    "clean",
    "--package",
    "EcoPaste",
    "--manifest-path",
    "src-tauri/Cargo.toml",
  ];
  if (targetDirectory) {
    args.push("--target-dir", targetDirectory);
  }
  args.push(...extraArgs);

  const result = spawnSync(cargo, args, {
    cwd: projectRoot,
    stdio: "inherit",
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`cargo ${args.join(" ")} exited with ${result.status}`);
  }
}

cleanAppArtifacts();
cleanAppArtifacts(androidTargetDirectory);
