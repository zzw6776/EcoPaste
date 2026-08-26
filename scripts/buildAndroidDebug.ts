import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const projectRoot = resolve(import.meta.dirname, "..");
const packageJson = JSON.parse(
  readFileSync(resolve(projectRoot, "package.json"), "utf8"),
) as { version: string };
const sourceApk = resolve(
  projectRoot,
  "src-tauri/gen/android/app/build/outputs/apk/universal/debug/app-universal-debug.apk",
);
const artifactDirectory = resolve(projectRoot, "artifacts/android");
const artifactApk = resolve(
  artifactDirectory,
  `EcoPaste-${packageJson.version}-android-arm64-debug.apk`,
);

/** 运行 pnpm 子命令，并保留终端中的原始构建输出。 */
function runPnpm(args: string[]) {
  const result = spawnSync("pnpm", args, {
    cwd: projectRoot,
    shell: process.platform === "win32",
    stdio: "inherit",
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`pnpm ${args.join(" ")} exited with ${result.status ?? 1}`);
  }
}

let buildError: unknown;

try {
  runPnpm([
    "tauri",
    "android",
    "build",
    "--debug",
    "--target",
    "aarch64",
    "--apk",
    "--ci",
  ]);
  mkdirSync(artifactDirectory, { recursive: true });
  copyFileSync(sourceApk, artifactApk);
} catch (error) {
  buildError = error;
}

try {
  runPnpm(["clean:android"]);
} catch (cleanError) {
  if (buildError) {
    throw new AggregateError(
      [buildError, cleanError],
      "Android build and Gradle cleanup both failed",
    );
  }
  throw cleanError;
}

if (buildError) {
  throw buildError;
}

process.stdout.write(`Android APK: ${artifactApk}\n`);
