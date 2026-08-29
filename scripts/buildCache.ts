import { spawnSync } from "node:child_process";
import type { Stats } from "node:fs";
import { lstat, mkdir, mkdtemp, opendir, rename, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { resolve } from "node:path";

const GIBIBYTE = 1024 ** 3;
const DESKTOP_CACHE_LIMIT = 6 * GIBIBYTE;
const ANDROID_CACHE_LIMIT = 3 * GIBIBYTE;

/** 统计目录的逻辑大小；忽略符号链接，避免重复计算链接目标。 */
async function getPathSize(path: string): Promise<number> {
  let pathStat: Stats;
  try {
    pathStat = await lstat(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return 0;
    }
    throw error;
  }

  if (pathStat.isSymbolicLink()) {
    return 0;
  }
  if (!pathStat.isDirectory()) {
    return pathStat.size;
  }

  let size = 0;
  const directory = await opendir(path);
  for await (const entry of directory) {
    size += await getPathSize(resolve(path, entry.name));
  }

  return size;
}

/** 汇总多个缓存目录，供清理前决策和清理失败后的结果复核共用。 */
async function getPathsSize(paths: string[]): Promise<number> {
  let size = 0;
  for (const path of paths) {
    size += await getPathSize(path);
  }

  return size;
}

/** 检查构建缓存是否越过阈值，并输出本次决策。 */
async function isCacheLimitExceeded(
  label: string,
  paths: string[],
  limit: number,
) {
  const size = await getPathsSize(paths);

  const sizeGiB = (size / GIBIBYTE).toFixed(2);
  const limitGiB = (limit / GIBIBYTE).toFixed(0);
  if (size <= limit) {
    process.stdout.write(
      `${label} build cache: ${sizeGiB} GiB / ${limitGiB} GiB; keeping cache.\n`,
    );
    return false;
  }

  process.stdout.write(
    `${label} build cache: ${sizeGiB} GiB / ${limitGiB} GiB; cleaning before build.\n`,
  );
  return true;
}

/** 使用 rustup 管理的 Cargo 清理指定 profile，避免清到共享下载缓存。 */
function cleanCargoProfiles(
  projectRoot: string,
  profiles: string[],
  target?: string,
) {
  const cargoHome = process.env.CARGO_HOME ?? resolve(homedir(), ".cargo");
  const cargo = resolve(
    cargoHome,
    "bin",
    process.platform === "win32" ? "cargo.exe" : "cargo",
  );

  for (const profile of profiles) {
    const args = [
      "clean",
      "--profile",
      profile,
      "--manifest-path",
      "src-tauri/Cargo.toml",
    ];
    if (target) {
      args.push("--target", target);
    }

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
}

/** 桌面缓存超过 6 GiB 时，在构建前清理 host dev/release 并保留旧 bundle。 */
export async function cleanDesktopCacheIfNeeded(projectRoot: string) {
  const targetRoot = resolve(projectRoot, "src-tauri/target");
  const cachePaths = [
    resolve(targetRoot, "debug"),
    resolve(targetRoot, "release"),
  ];
  const shouldClean = await isCacheLimitExceeded(
    "Desktop",
    cachePaths,
    DESKTOP_CACHE_LIMIT,
  );
  if (!shouldClean) {
    return;
  }

  const bundle = resolve(targetRoot, "release/bundle");
  let bundleBackupRoot: string | undefined;
  try {
    await lstat(bundle);
    bundleBackupRoot = await mkdtemp(resolve(targetRoot, ".bundle-backup-"));
    await rename(bundle, resolve(bundleBackupRoot, "bundle"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }
  }

  let cargoError: unknown;
  try {
    cleanCargoProfiles(projectRoot, ["dev", "release"]);
  } catch (error) {
    cargoError = error;
  } finally {
    if (bundleBackupRoot) {
      await mkdir(resolve(targetRoot, "release"), { recursive: true });
      await rename(
        resolve(bundleBackupRoot, "bundle"),
        resolve(targetRoot, "release/bundle"),
      );
      await rm(bundleBackupRoot, { recursive: true });
    }
  }

  if (cargoError) {
    const remainingSize = await getPathsSize(cachePaths);
    if (remainingSize > DESKTOP_CACHE_LIMIT) {
      throw cargoError;
    }
    process.stderr.write(
      `Cargo cache cleanup was interrupted, but the remaining desktop cache is ${(remainingSize / GIBIBYTE).toFixed(2)} GiB; continuing build.\n`,
    );
  }
}

/** Android 缓存超过 3 GiB 时，在构建前清理 arm64 Cargo 与 Gradle 产物。 */
export async function cleanAndroidCacheIfNeeded(
  projectRoot: string,
  cleanGradle: () => Promise<void>,
) {
  const cachePaths = [
    resolve(projectRoot, "src-tauri/target/aarch64-linux-android"),
    resolve(projectRoot, "src-tauri/gen/android"),
  ];
  const shouldClean = await isCacheLimitExceeded(
    "Android arm64",
    cachePaths,
    ANDROID_CACHE_LIMIT,
  );
  if (!shouldClean) {
    return;
  }

  let gradleError: unknown;
  try {
    await cleanGradle();
  } catch (error) {
    gradleError = error;
  }

  let cargoError: unknown;
  try {
    cleanCargoProfiles(
      projectRoot,
      ["dev", "release"],
      "aarch64-linux-android",
    );
  } catch (error) {
    cargoError = error;
  }

  if (cargoError) {
    const remainingSize = await getPathsSize(cachePaths);
    if (remainingSize <= ANDROID_CACHE_LIMIT) {
      process.stderr.write(
        `Cargo cache cleanup was interrupted, but the remaining Android cache is ${(remainingSize / GIBIBYTE).toFixed(2)} GiB; continuing build.\n`,
      );
      cargoError = void 0;
    }
  }

  if (gradleError && cargoError) {
    throw new AggregateError(
      [gradleError, cargoError],
      "Android Gradle and Cargo cache cleanup both failed",
    );
  }
  if (gradleError) {
    throw gradleError;
  }
  if (cargoError) {
    throw cargoError;
  }
}
