import { spawnSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, resolve } from "node:path";
import { hashFile, snapshotFiles } from "./fileFingerprint";

interface IconManifest {
  outputs: Record<string, string>;
  sourceHash: string;
  tauriVersion: string;
}

const projectRoot = resolve(import.meta.dirname, "..");
const isMac =
  process.env.PLATFORM?.startsWith("macos") ?? process.platform === "darwin";
const logoName = isMac ? "logo-mac" : "logo";
const source = resolve(projectRoot, `src-tauri/assets/${logoName}.png`);
const cacheRoot = resolve(projectRoot, "src-tauri/target/.ecopaste-build");
const manifestPath = resolve(cacheRoot, `icons-${logoName}.json`);

/** 运行 Tauri CLI 并返回版本，版本变化时强制刷新生成结果。 */
function getTauriVersion(): string {
  const result = spawnSync("tauri", ["--version"], {
    cwd: projectRoot,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`tauri --version exited with ${result.status}`);
  }

  return result.stdout.trim();
}

/** 读取上次图标生成清单；缓存不存在或损坏时返回空值并重新生成。 */
async function readManifest(): Promise<IconManifest | null> {
  try {
    return JSON.parse(await readFile(manifestPath, "utf8")) as IconManifest;
  } catch (error) {
    if (
      (error as NodeJS.ErrnoException).code === "ENOENT" ||
      error instanceof SyntaxError
    ) {
      return null;
    }
    throw error;
  }
}

/** 收集 Tauri 生成的桌面图标和 Android launcher 资源摘要。 */
async function collectOutputs(): Promise<Record<string, string>> {
  const iconsRoot = resolve(projectRoot, "src-tauri/icons");
  const androidRoot = resolve(
    projectRoot,
    "src-tauri/gen/android/app/src/main/res",
  );
  const icons = await snapshotFiles(iconsRoot);
  const android = await snapshotFiles(androidRoot, (path) => {
    return basename(path).startsWith("ic_launcher");
  });
  const outputs: Record<string, string> = {};

  for (const [path, fingerprint] of icons) {
    outputs[`icons/${path}`] = fingerprint.hash;
  }
  for (const [path, fingerprint] of android) {
    outputs[`android/${path}`] = fingerprint.hash;
  }

  return outputs;
}

/** 比较清单中的所有生成文件，防止输出被删除或手工改写后错误跳过。 */
function outputsMatch(
  expected: Record<string, string>,
  current: Record<string, string>,
): boolean {
  const expectedEntries = Object.entries(expected);
  const currentEntries = Object.entries(current);
  if (
    expectedEntries.length === 0 ||
    expectedEntries.length !== currentEntries.length
  ) {
    return false;
  }

  return expectedEntries.every(([path, hash]) => {
    return current[path] === hash;
  });
}

const tauriVersion = getTauriVersion();
const sourceHash = await hashFile(source);
const previous = await readManifest();
const currentOutputs = await collectOutputs();
if (
  previous?.sourceHash === sourceHash &&
  previous.tauriVersion === tauriVersion &&
  outputsMatch(previous.outputs, currentOutputs)
) {
  process.stdout.write("Icon assets unchanged; skipping generation.\n");
} else {
  const result = spawnSync("tauri", ["icon", source], {
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
    const outputs = await collectOutputs();
    if (Object.keys(outputs).length === 0) {
      throw new Error("tauri icon did not produce any tracked icon assets");
    }

    await mkdir(cacheRoot, { recursive: true });
    await writeFile(
      manifestPath,
      `${JSON.stringify({ outputs, sourceHash, tauriVersion }, null, 2)}\n`,
    );
  }
}
