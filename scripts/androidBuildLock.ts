import { randomUUID } from "node:crypto";
import { mkdir, open, readFile, rm, stat } from "node:fs/promises";
import { resolve } from "node:path";

const LOCK_TOKEN_ENV = "ECOPASTE_ANDROID_BUILD_LOCK_TOKEN";
const LOCK_INITIALIZATION_GRACE_MS = 5_000;

interface BuildLockOwner {
  pid: number;
  operation: string;
  startedAt: string;
  token: string;
}

export interface AndroidBuildLock {
  childEnv: NodeJS.ProcessEnv;
  release: () => Promise<void>;
}

/** 检查锁记录的进程是否仍存在；权限不足时按仍在运行处理。 */
function isProcessRunning(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

/** 读取锁持有者；写入尚未完成时由调用方结合文件时间判断。 */
async function readLockOwner(lockPath: string) {
  try {
    return JSON.parse(await readFile(lockPath, "utf8")) as BuildLockOwner;
  } catch {
    return void 0;
  }
}

/** 原子获取 Android 构建锁，允许持锁构建调用受控的嵌套清理命令。 */
export async function acquireAndroidBuildLock(
  projectRoot: string,
  operation: string,
): Promise<AndroidBuildLock> {
  const targetRoot = resolve(projectRoot, "src-tauri/target");
  const lockPath = resolve(targetRoot, ".android-build.lock");
  const inheritedToken = process.env[LOCK_TOKEN_ENV];
  await mkdir(targetRoot, { recursive: true });

  if (inheritedToken) {
    const owner = await readLockOwner(lockPath);
    if (
      owner?.token === inheritedToken &&
      Number.isInteger(owner.pid) &&
      isProcessRunning(owner.pid)
    ) {
      return {
        childEnv: process.env,
        release: async () => {},
      };
    }
  }

  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const lock = await open(lockPath, "wx");
      const owner: BuildLockOwner = {
        operation,
        pid: process.pid,
        startedAt: new Date().toISOString(),
        token: randomUUID(),
      };
      try {
        await lock.writeFile(JSON.stringify(owner));
      } catch (error) {
        await lock.close();
        await rm(lockPath, { force: true });
        throw error;
      }
      await lock.close();

      return {
        childEnv: { ...process.env, [LOCK_TOKEN_ENV]: owner.token },
        release: async () => {
          const currentOwner = await readLockOwner(lockPath);
          if (currentOwner?.token === owner.token) {
            await rm(lockPath, { force: true });
          }
        },
      };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
        throw error;
      }

      const owner = await readLockOwner(lockPath);
      if (!owner) {
        const lockStat = await stat(lockPath);
        if (Date.now() - lockStat.mtimeMs < LOCK_INITIALIZATION_GRACE_MS) {
          throw new Error(
            "Another Android operation is acquiring the build lock.",
          );
        }
      }

      if (owner && isProcessRunning(owner.pid)) {
        throw new Error(
          `Another Android ${owner.operation} is running (PID ${owner.pid}, started ${owner.startedAt}).`,
        );
      }

      await rm(lockPath, { force: true });
    }
  }

  throw new Error("Could not acquire the Android build lock.");
}
