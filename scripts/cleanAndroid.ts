import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
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

/** 移除 Tauri 文件关联占位符生成的行尾空白，避免构建污染工作区。 */
function normalizeGeneratedManifest() {
  const content = readFileSync(androidManifest, "utf8");
  const normalized = content.replace(/[\t ]+$/gm, "");

  if (content === normalized) {
    return;
  }

  writeFileSync(androidManifest, normalized);
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
