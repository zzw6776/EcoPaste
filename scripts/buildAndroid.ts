import { spawn } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  statSync,
} from "node:fs";
import { parse, resolve } from "node:path";
import { acquireAndroidBuildLock } from "./androidBuildLock";
import { normalizeAndroidManifestFile } from "./androidManifest";
import { cleanAndroidCacheIfNeeded } from "./buildCache";

const projectRoot = resolve(import.meta.dirname, "..");
const SKIP_GRADLE_RUST_BUILD_ENV = "ECOPASTE_SKIP_GRADLE_RUST_BUILD";
const androidManifestPath = resolve(
  projectRoot,
  "src-tauri/gen/android/app/src/main/AndroidManifest.xml",
);
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

/** 让 Windows Cargo registry 与 Android 工程位于同一盘符，供 Kotlin 使用可迁移的增量路径。 */
function resolveWindowsAndroidCargoHome() {
  if (process.platform !== "win32") {
    return void 0;
  }

  const configuredCargoHome = process.env.CARGO_HOME;
  if (
    configuredCargoHome &&
    parse(resolve(configuredCargoHome)).root.toLowerCase() ===
      parse(projectRoot).root.toLowerCase()
  ) {
    return resolve(configuredCargoHome);
  }

  return resolve(projectRoot, ".cache", "windows-android-cargo-home");
}

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
const buildEnv: NodeJS.ProcessEnv = {
  ...process.env,
  [SKIP_GRADLE_RUST_BUILD_ENV]: "1",
};
const windowsAndroidCargoHome = resolveWindowsAndroidCargoHome();
if (windowsAndroidCargoHome) {
  mkdirSync(windowsAndroidCargoHome, { recursive: true });
  buildEnv.CARGO_HOME = windowsAndroidCargoHome;
  process.stdout.write(
    `Windows Android Cargo home: ${windowsAndroidCargoHome}\n`,
  );
}

const buildLock = await acquireAndroidBuildLock(projectRoot, `${mode} build`);
try {
  await cleanAndroidCacheIfNeeded(projectRoot, async () => {
    await runPnpm(["clean:android"], buildLock.childEnv);
  });
  await runPnpm(buildArgs, buildEnv, true);
  mkdirSync(artifactDirectory, { recursive: true });
  copyFileSync(sourceApk, artifactApk);

  process.stdout.write(`Android APK: ${artifactApk}\n`);
} finally {
  try {
    if (normalizeAndroidManifestFile(androidManifestPath)) {
      process.stdout.write(
        "Normalized generated Android Manifest whitespace.\n",
      );
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(
      `Failed to normalize generated Android Manifest whitespace: ${message}\n`,
    );
  } finally {
    await buildLock.release();
  }
}
