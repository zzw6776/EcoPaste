import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { delimiter, resolve } from "node:path";
import { normalizeAndroidManifestFile } from "./androidManifest";
import { cleanDesktopCacheIfNeeded } from "./buildCache";

const projectRoot = resolve(import.meta.dirname, "..");
const androidManifestPath = resolve(
  projectRoot,
  "src-tauri/gen/android/app/src/main/AndroidManifest.xml",
);
const pathKey =
  Object.keys(process.env).find((key) => {
    return key.toLowerCase() === "path";
  }) ?? "PATH";
const cargoHome = process.env.CARGO_HOME ?? resolve(homedir(), ".cargo");
const cargoBin = resolve(cargoHome, "bin");
const tauriArgs = process.argv.slice(2);
if (tauriArgs[0] === "dev" || tauriArgs[0] === "build") {
  await cleanDesktopCacheIfNeeded(projectRoot);
}

/** 尊重显式配置；否则仅在本机可用时为当前 Tauri 调用启用 sccache。 */
function resolveRustcWrapper(): string | undefined {
  if (process.env.RUSTC_WRAPPER !== void 0) {
    return process.env.RUSTC_WRAPPER;
  }

  const result = spawnSync("sccache", ["--version"], {
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (result.status !== 0) {
    return void 0;
  }

  process.stdout.write(`Rust compiler cache: ${result.stdout.trim()}\n`);
  return "sccache";
}

const rustcWrapper = resolveRustcWrapper();
const tauriEnv: NodeJS.ProcessEnv = {
  ...process.env,
  [pathKey]: `${cargoBin}${delimiter}${process.env[pathKey] ?? ""}`,
};
if (rustcWrapper) {
  tauriEnv.RUSTC_WRAPPER = rustcWrapper;
}

const result = spawnSync("tauri", tauriArgs, {
  env: tauriEnv,
  shell: process.platform === "win32",
  stdio: "inherit",
});

if (tauriArgs[0] === "android" && existsSync(androidManifestPath)) {
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
  }
}

if (result.error) {
  throw result.error;
}

process.exitCode = result.status ?? 1;
