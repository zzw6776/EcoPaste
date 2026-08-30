import { spawnSync } from "node:child_process";
import {
  existsSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const androidRoot = resolve(projectRoot, "src-tauri/gen/android");
const gradleWrapper =
  process.platform === "win32" ? "gradlew.bat" : "./gradlew";
const androidManifest = resolve(
  androidRoot,
  "app/src/main/AndroidManifest.xml",
);
const wslBuildDirectories = [
  resolve(androidRoot, ".gradle"),
  resolve(androidRoot, "build"),
  resolve(androidRoot, "buildSrc/.gradle"),
  resolve(androidRoot, "buildSrc/build"),
  resolve(androidRoot, "app/build"),
  resolve(androidRoot, "hidden-api-stubs/build"),
  resolve(androidRoot, "app/src/main/jniLibs"),
];

/** 移除 Tauri 文件关联占位符生成的行尾空白，避免构建污染工作区。 */
function normalizeGeneratedManifest() {
  const content = readFileSync(androidManifest, "utf8");
  const normalized = content.replace(/[\t ]+$/gm, "");

  if (content === normalized) {
    return;
  }

  writeFileSync(androidManifest, normalized);
}

/** 在 WSL 中清空隔离目录内容，同时保留不能删除的 bind mount 挂载点。 */
function cleanWslBuildDirectories() {
  if (process.platform !== "linux") {
    return false;
  }

  const projectDevice = statSync(projectRoot).dev;
  if (projectDevice === statSync("/").dev) {
    return false;
  }

  const sharedPaths = wslBuildDirectories.filter((path) => {
    return !existsSync(path) || statSync(path).dev === projectDevice;
  });
  if (sharedPaths.length > 0) {
    throw new Error(
      `WSL Android build directories are not isolated with bind mounts:\n${sharedPaths.join("\n")}\nRun "mount -a" in WSL before cleaning.`,
    );
  }

  for (const directory of wslBuildDirectories) {
    for (const entry of readdirSync(directory)) {
      rmSync(resolve(directory, entry), { force: true, recursive: true });
    }
  }

  return true;
}

if (cleanWslBuildDirectories()) {
  normalizeGeneratedManifest();
  process.exit(0);
}

const result = spawnSync(gradleWrapper, ["clean", "--no-daemon"], {
  cwd: androidRoot,
  shell: process.platform === "win32",
  stdio: "inherit",
});

if (result.error) {
  throw result.error;
}

normalizeGeneratedManifest();
process.exitCode = result.status ?? 1;
