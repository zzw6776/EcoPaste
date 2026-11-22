import type { Stats } from "node:fs";
import { lstat, readdir, rm, rmdir, statfs } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { acquireAndroidBuildLock } from "./androidBuildLock";

const ANDROID_BUILD_LOCK_FILENAME = ".android-build.lock";
const GIBIBYTE = 1024 ** 3;
const MEBIBYTE = 1024 ** 2;
const TARGET_ROOT_RELATIVE_PATH = "src-tauri/target";

const REPRODUCIBLE_OUTPUT_RELATIVE_PATHS = [
  "src-tauri/gen/android/.gradle",
  "src-tauri/gen/android/.kotlin",
  "src-tauri/gen/android/build",
  "src-tauri/gen/android/buildSrc/.gradle",
  "src-tauri/gen/android/buildSrc/build",
  "src-tauri/gen/android/app/build",
  "src-tauri/gen/android/hidden-api-stubs/build",
  "src-tauri/gen/android/app/src/main/jniLibs",
  "src-tauri/gen/schemas",
  "sync-server/target",
  "sync-server/protocol/target",
  "src-tauri/vendor/swarm-discovery/target",
  "src-tauri/vendor/iroh-mdns-address-lookup/target",
  "dist",
  "dist-ssr",
  "node_modules/.vite",
  "node_modules/.cache",
  "artifacts",
  "apk",
] as const;

export interface CleanOutputsReport {
  availableAfterBytes: number;
  availableBeforeBytes: number;
  cleanedPaths: string[];
  freedDiskBytes: number;
}

/** 读取路径元信息；路径不存在时返回 void 0。 */
async function tryLstat(path: string): Promise<Stats | undefined> {
  try {
    return await lstat(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return void 0;
    }
    throw error;
  }
}

/** 读取指定路径所在卷的当前可用字节数。 */
async function readAvailableDiskBytes(path: string): Promise<number> {
  const volumeStats = await statfs(path);
  return Number(volumeStats.bavail) * Number(volumeStats.bsize);
}

/** 将字节数格式化为易读的二进制单位文本。 */
function formatBytes(bytes: number): string {
  if (bytes >= GIBIBYTE) {
    return `${(bytes / GIBIBYTE).toFixed(2)} GiB`;
  }
  return `${(bytes / MEBIBYTE).toFixed(2)} MiB`;
}

/** 在 WSL 共享工作区中识别独立 ext4 bind-mount 目录，避免直接删除挂载点本身。 */
function isWslBindMount(pathStat: Stats, projectDevice: number | bigint) {
  return process.platform === "linux" && pathStat.dev !== projectDevice;
}

/** 清空目录内容，可选保留指定顶层文件名（如互斥锁文件）。 */
async function emptyDirectory(directory: string, keepName?: string) {
  const entries = await readdir(directory);
  let removedAny = false;

  for (const entry of entries) {
    if (keepName && entry === keepName) {
      continue;
    }

    await rm(resolve(directory, entry), { force: true, recursive: true });
    removedAny = true;
  }

  return removedAny;
}

/** 删除非挂载点输出路径；若为 WSL 挂载点则仅清空内部内容。 */
async function removeOutputPath(
  fullPath: string,
  projectDevice: number | bigint,
) {
  const pathStat = await tryLstat(fullPath);
  if (!pathStat) {
    return false;
  }

  if (pathStat.isDirectory() && isWslBindMount(pathStat, projectDevice)) {
    return emptyDirectory(fullPath);
  }

  await rm(fullPath, { force: true, recursive: true });
  return true;
}

/** 清理本仓库全部可再生的编译缓存与打包输出物，并返回空间变化报告。 */
export async function cleanAllOutputsWithReport(
  projectRoot: string,
): Promise<CleanOutputsReport> {
  const projectStat = await lstat(projectRoot);
  const availableBeforeBytes = await readAvailableDiskBytes(projectRoot);
  const targetRoot = resolve(projectRoot, TARGET_ROOT_RELATIVE_PATH);
  const cleanedPaths: string[] = [];

  const buildLock = await acquireAndroidBuildLock(
    projectRoot,
    "outputs cleanup",
  );
  try {
    const targetCleaned = await emptyDirectory(
      targetRoot,
      ANDROID_BUILD_LOCK_FILENAME,
    );
    if (targetCleaned) {
      cleanedPaths.push(TARGET_ROOT_RELATIVE_PATH);
    }

    for (const relativePath of REPRODUCIBLE_OUTPUT_RELATIVE_PATHS) {
      const removed = await removeOutputPath(
        resolve(projectRoot, relativePath),
        projectStat.dev,
      );
      if (removed) {
        cleanedPaths.push(relativePath);
      }
    }
  } finally {
    await buildLock.release();
  }

  const targetStatAfterRelease = await tryLstat(targetRoot);
  if (
    targetStatAfterRelease?.isDirectory() &&
    !isWslBindMount(targetStatAfterRelease, projectStat.dev)
  ) {
    const remainingEntries = await readdir(targetRoot);
    if (remainingEntries.length === 0) {
      await rmdir(targetRoot);
    }
  }

  const availableAfterBytes = await readAvailableDiskBytes(projectRoot);
  const freedDiskBytes = Math.max(
    0,
    availableAfterBytes - availableBeforeBytes,
  );

  return {
    availableAfterBytes,
    availableBeforeBytes,
    cleanedPaths,
    freedDiskBytes,
  };
}

/** 清理本仓库全部可再生的编译缓存与打包输出物，返回清理路径列表。 */
export async function cleanAllOutputs(projectRoot: string): Promise<string[]> {
  const report = await cleanAllOutputsWithReport(projectRoot);
  return report.cleanedPaths;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const projectRoot = resolve(import.meta.dirname, "..");
  const report = await cleanAllOutputsWithReport(projectRoot);
  if (report.cleanedPaths.length === 0) {
    process.stdout.write(
      `No reproducible outputs to clean (available: ${formatBytes(report.availableAfterBytes)}).\n`,
    );
  } else {
    process.stdout.write(
      `Cleaned outputs:\n- ${report.cleanedPaths.join("\n- ")}\nFreed disk space: ${formatBytes(report.freedDiskBytes)} (available: ${formatBytes(report.availableBeforeBytes)} -> ${formatBytes(report.availableAfterBytes)})\n`,
    );
  }
}
