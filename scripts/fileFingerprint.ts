import { createHash } from "node:crypto";
import { lstat, opendir, readFile, utimes } from "node:fs/promises";
import { relative, resolve } from "node:path";

export interface FileFingerprint {
  atimeSeconds: number;
  hash: string;
  mtimeSeconds: number;
}

/** 为文件快照生成与遍历顺序无关的摘要。 */
export function hashFileSnapshot(
  snapshot: Map<string, FileFingerprint>,
): string {
  const hash = createHash("sha256");
  const entries = [...snapshot.entries()].sort(([left], [right]) => {
    return left.localeCompare(right);
  });

  for (const [path, fingerprint] of entries) {
    hash.update(`${Buffer.byteLength(path)}:`);
    hash.update(path);
    hash.update(fingerprint.hash);
  }

  return hash.digest("hex");
}

/** 计算文件内容摘要，不依赖文件时间戳。 */
export async function hashFile(path: string): Promise<string> {
  return createHash("sha256")
    .update(await readFile(path))
    .digest("hex");
}

/** 递归记录目录内普通文件的内容摘要与时间戳，忽略符号链接。 */
export async function snapshotFiles(
  root: string,
  include: (path: string) => boolean = () => {
    return true;
  },
): Promise<Map<string, FileFingerprint>> {
  const snapshot = new Map<string, FileFingerprint>();

  async function visit(path: string) {
    let pathStat: Awaited<ReturnType<typeof lstat>>;
    try {
      pathStat = await lstat(path);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return;
      }
      throw error;
    }

    if (pathStat.isSymbolicLink()) {
      return;
    }
    if (pathStat.isDirectory()) {
      const directory = await opendir(path);
      for await (const entry of directory) {
        await visit(resolve(path, entry.name));
      }
      return;
    }
    if (!pathStat.isFile() || !include(path)) {
      return;
    }

    snapshot.set(relative(root, path), {
      atimeSeconds: pathStat.atimeMs / 1000,
      hash: await hashFile(path),
      mtimeSeconds: pathStat.mtimeMs / 1000,
    });
  }

  await visit(root);
  return snapshot;
}

/** 为内容未变化的构建产物恢复原时间戳，避免下游仅因重写文件而失效。 */
export async function restoreUnchangedFileTimes(
  root: string,
  before: Map<string, FileFingerprint>,
): Promise<number> {
  const after = await snapshotFiles(root);
  let restored = 0;

  for (const [path, current] of after) {
    const previous = before.get(path);
    if (!previous || previous.hash !== current.hash) {
      continue;
    }

    await utimes(
      resolve(root, path),
      previous.atimeSeconds,
      previous.mtimeSeconds,
    );
    restored += 1;
  }

  return restored;
}
