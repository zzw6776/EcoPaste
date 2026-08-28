import { spawn } from "node:child_process";
import { copyFileSync, mkdirSync, readFileSync } from "node:fs";
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
