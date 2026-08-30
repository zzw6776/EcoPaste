import { spawn } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  statSync,
} from "node:fs";
import { resolve } from "node:path";
import { cleanAndroidCacheIfNeeded } from "./buildCache";

const projectRoot = resolve(import.meta.dirname, "..");
const mode = process.argv[2];
if (mode !== "debug" && mode !== "release") {
  throw new Error("Android build mode must be debug or release.");
}

const packageJson = JSON.parse(
  readFileSync(resolve(projectRoot, "package.json"), "utf8"),
) as { version: string };
const sourceApk = resolve(
  projectRoot,
  `src-tauri/gen/android/app/build/outputs/apk/universal/${mode}/app-universal-${mode}.apk`,
);
const artifactDirectory = resolve(projectRoot, "artifacts/android");
const artifactApk = resolve(
  artifactDirectory,
  `EcoPaste-${packageJson.version}-android-arm64-${mode}.apk`,
);
const RETRYABLE_NETWORK_ERROR =
  /Could not (?:GET|download)|Remote host terminated the handshake|Connection reset|Read timed out|Connection timed out/i;

/** 防止 WSL 把宿主相关的构建产物写入 Windows 共享工作区。 */
function assertWslBuildMounts() {
  if (process.platform !== "linux") {
    return;
  }

  const projectDevice = statSync(projectRoot).dev;
  if (projectDevice === statSync("/").dev) {
    return;
  }

  const isolatedPaths = [
    resolve(projectRoot, "node_modules"),
    resolve(projectRoot, "src-tauri/target"),
    resolve(projectRoot, "src-tauri/gen/android/.gradle"),
    resolve(projectRoot, "src-tauri/gen/android/build"),
    resolve(projectRoot, "src-tauri/gen/android/buildSrc/.gradle"),
    resolve(projectRoot, "src-tauri/gen/android/buildSrc/build"),
    resolve(projectRoot, "src-tauri/gen/android/app/build"),
    resolve(projectRoot, "src-tauri/gen/android/hidden-api-stubs/build"),
    resolve(projectRoot, "src-tauri/gen/android/app/src/main/jniLibs"),
  ];
  const sharedPaths = isolatedPaths.filter((path) => {
    return !existsSync(path) || statSync(path).dev === projectDevice;
  });
  if (sharedPaths.length === 0) {
    return;
  }

  throw new Error(
    `WSL Android build directories are not isolated with bind mounts:\n${sharedPaths.join("\n")}\nRun "mount -a" in WSL before building.`,
  );
}

assertWslBuildMounts();

/** 运行 pnpm 子命令，并保留终端中的原始构建输出。 */
async function runPnpm(
  args: string[],
  env = process.env,
  retryNetworkFailure = false,
) {
  const maxAttempts = retryNetworkFailure ? 3 : 1;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    let diagnosticTail = "";
    const status = await new Promise<number>(
      (resolvePromise, rejectPromise) => {
        const child = spawn("pnpm", args, {
          cwd: projectRoot,
          env,
          shell: process.platform === "win32",
          stdio: ["inherit", "pipe", "pipe"],
        });

        const forwardOutput = (
          chunk: Buffer,
          destination: NodeJS.WriteStream,
        ) => {
          destination.write(chunk);
          diagnosticTail = `${diagnosticTail}${chunk.toString()}`.slice(
            -16_384,
          );
        };

        child.stdout.on("data", (chunk: Buffer) => {
          forwardOutput(chunk, process.stdout);
        });
        child.stderr.on("data", (chunk: Buffer) => {
          forwardOutput(chunk, process.stderr);
        });
        child.on("error", rejectPromise);
        child.on("close", (code) => {
          resolvePromise(code ?? 1);
        });
      },
    );

    if (status === 0) return;

    const shouldRetry =
      retryNetworkFailure &&
      attempt < maxAttempts &&
      RETRYABLE_NETWORK_ERROR.test(diagnosticTail);
    if (!shouldRetry) {
      throw new Error(`pnpm ${args.join(" ")} exited with ${status}`);
    }

    process.stderr.write(
      `Android dependency download failed; retrying build (${attempt + 1}/${maxAttempts}).\n`,
    );
  }
}

const buildArgs = ["tauri", "android", "build"];
if (mode === "debug") {
  buildArgs.push("--debug");
}
buildArgs.push("--target", "aarch64", "--apk", "--ci");

await cleanAndroidCacheIfNeeded(projectRoot, async () => {
  await runPnpm(["clean:android"]);
});
await runPnpm(buildArgs, process.env, true);
mkdirSync(artifactDirectory, { recursive: true });
copyFileSync(sourceApk, artifactApk);

process.stdout.write(`Android APK: ${artifactApk}\n`);
