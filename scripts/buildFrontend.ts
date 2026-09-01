import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { hashFileSnapshot, snapshotFiles } from "./fileFingerprint";
import {
  iconOutputs,
  iconSources,
  manifestPath,
  projectRoot,
} from "./iconAssets";

interface FrontendBuildCache {
  inputHash: string;
  outputHash: string;
  schemaVersion: number;
}

interface FrontendOutput {
  fileCount: number;
  hash: string;
}

const CACHE_SCHEMA_VERSION = 1;
const cachePath = resolve(
  projectRoot,
  "node_modules/.cache/ecopaste/frontend-build.json",
);
const distRoot = resolve(projectRoot, "dist");
const buildInputPaths = [
  ".env",
  ".env.local",
  ".env.production",
  ".env.production.local",
  "index.html",
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "public",
  "scripts/buildFrontend.ts",
  "scripts/buildVite.ts",
  "scripts/checkIcons.ts",
  "scripts/fileFingerprint.ts",
  "scripts/iconAssets.ts",
  "src",
  "tsconfig.json",
  "tsconfig.node.json",
  "uno.config.ts",
  "vite.config.ts",
  resolve(projectRoot, manifestPath),
  ...Object.values(iconSources),
  ...iconOutputs.map((output) => {
    return output.targetPath;
  }),
];

/** 给摘要输入增加长度边界，避免不同字段组合产生相同字节流。 */
function updateHash(hash: ReturnType<typeof createHash>, value: string) {
  hash.update(`${Buffer.byteLength(value)}:`);
  hash.update(value);
}

/** 只纳入会影响前端工具链或输出的 package.json 字段，版本号不影响 Web 资源。 */
async function readFrontendPackageConfig(): Promise<string> {
  const packageJson = JSON.parse(
    await readFile(resolve(projectRoot, "package.json"), "utf8"),
  ) as Record<string, unknown>;
  const scripts = packageJson.scripts as Record<string, string>;

  return JSON.stringify({
    dependencies: packageJson.dependencies,
    devDependencies: packageJson.devDependencies,
    engines: packageJson.engines,
    packageManager: packageJson.packageManager,
    pnpm: packageJson.pnpm,
    scripts: {
      "build:vite": scripts["build:vite"],
      "icons:check": scripts["icons:check"],
      tsc: scripts.tsc,
    },
    type: packageJson.type,
  });
}

/** 计算源码、配置、图标和构建环境的联合摘要。 */
async function createBuildInputHash(): Promise<string> {
  const hash = createHash("sha256");
  const uniqueInputPaths = [...new Set(buildInputPaths)].sort();
  for (const inputPath of uniqueInputPaths) {
    const absolutePath = resolve(projectRoot, inputPath);
    const snapshot = await snapshotFiles(absolutePath);

    updateHash(hash, inputPath);
    updateHash(hash, hashFileSnapshot(snapshot));
  }

  updateHash(hash, await readFrontendPackageConfig());
  updateHash(hash, process.platform);
  updateHash(hash, process.arch);
  updateHash(hash, process.version);

  const buildEnvironment = Object.entries(process.env)
    .filter(([name]) => {
      return (
        name === "NODE_ENV" ||
        name === "TAURI_DEV_HOST" ||
        name.startsWith("VITE_")
      );
    })
    .sort(([left], [right]) => {
      return left.localeCompare(right);
    });
  for (const [name, value] of buildEnvironment) {
    updateHash(hash, name);
    updateHash(hash, value ?? "");
  }

  return hash.digest("hex");
}

/** 读取并验证现有 dist；缺少入口文件时视为无可复用产物。 */
async function createFrontendOutput(): Promise<FrontendOutput | null> {
  const snapshot = await snapshotFiles(distRoot);
  if (!snapshot.has("index.html")) {
    return null;
  }

  return {
    fileCount: snapshot.size,
    hash: hashFileSnapshot(snapshot),
  };
}

/** 无缓存或缓存损坏时返回空，让正常构建自动修复。 */
async function readBuildCache(): Promise<FrontendBuildCache | null> {
  try {
    const cache = JSON.parse(
      await readFile(cachePath, "utf8"),
    ) as Partial<FrontendBuildCache>;
    if (
      cache.schemaVersion !== CACHE_SCHEMA_VERSION ||
      typeof cache.inputHash !== "string" ||
      typeof cache.outputHash !== "string"
    ) {
      return null;
    }

    return cache as FrontendBuildCache;
  } catch (error) {
    const errorCode = (error as NodeJS.ErrnoException).code;
    if (errorCode === "ENOENT" || error instanceof SyntaxError) {
      return null;
    }
    throw error;
  }
}

/** 顺序执行构建阶段并输出独立耗时，方便定位后续性能回退。 */
function runBuildStep(label: string, script: string) {
  const startedAt = performance.now();
  const result = spawnSync("pnpm", ["run", script], {
    cwd: projectRoot,
    shell: process.platform === "win32",
    stdio: "inherit",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`pnpm run ${script} exited with ${result.status}`);
  }

  process.stdout.write(
    `${label}: ${((performance.now() - startedAt) / 1000).toFixed(2)}s\n`,
  );
}

const startedAt = performance.now();
const [inputHash, cache, currentOutput] = await Promise.all([
  createBuildInputHash(),
  readBuildCache(),
  createFrontendOutput(),
]);
if (
  cache?.inputHash === inputHash &&
  currentOutput?.hash === cache.outputHash
) {
  process.stdout.write(
    `Frontend build cache hit: verified ${currentOutput.fileCount} dist files in ${((performance.now() - startedAt) / 1000).toFixed(2)}s.\n`,
  );
} else {
  runBuildStep("TypeScript check", "tsc");
  runBuildStep("Icon check", "icons:check");
  runBuildStep("Vite build", "build:vite");

  const output = await createFrontendOutput();
  if (!output) {
    throw new Error("Vite build completed without dist/index.html");
  }

  const nextCache: FrontendBuildCache = {
    inputHash,
    outputHash: output.hash,
    schemaVersion: CACHE_SCHEMA_VERSION,
  };
  await mkdir(dirname(cachePath), { recursive: true });
  await writeFile(cachePath, `${JSON.stringify(nextCache, null, 2)}\n`);
  process.stdout.write(
    `Frontend build cache updated: ${output.fileCount} dist files, ${((performance.now() - startedAt) / 1000).toFixed(2)}s total.\n`,
  );
}
