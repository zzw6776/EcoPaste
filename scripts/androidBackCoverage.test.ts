import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

const SOURCE_ROOT = path.resolve("src");
const RAW_OVERLAY_IMPORT =
  /import\s*{[^}]*\b(?:Drawer|Dropdown|Image|Modal|Popover|Select)\b[^}]*}\s*from\s*["']antd["']/s;
const RAW_OVERLAY_IMPORT_ALLOWLIST = new Set([
  "components/Drawer/index.tsx",
  "components/Dropdown/index.tsx",
  "components/Image/index.tsx",
  "components/Modal/index.tsx",
  "components/Popover/index.tsx",
  "components/Select/index.tsx",
  "utils/feedback.ts",
]);

/** 生成跨 Windows 与 POSIX 一致的源码相对路径。 */
function relativeSourcePath(file: string): string {
  return path.relative(SOURCE_ROOT, file).split(path.sep).join("/");
}

/** 递归列出目录中的 TypeScript 源文件。 */
function listTypeScriptFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) return listTypeScriptFiles(target);
    if (!entry.isFile() || !/\.tsx?$/.test(entry.name)) return [];

    return [target];
  });
}

test("interactive Ant Design overlays use Android-back-aware wrappers", () => {
  const violations = listTypeScriptFiles(SOURCE_ROOT)
    .filter((file) => {
      const relativeFile = relativeSourcePath(file);
      if (RAW_OVERLAY_IMPORT_ALLOWLIST.has(relativeFile)) return false;

      return RAW_OVERLAY_IMPORT.test(readFileSync(file, "utf8"));
    })
    .map((file) => {
      return relativeSourcePath(file);
    });

  assert.deepEqual(violations, []);
});
