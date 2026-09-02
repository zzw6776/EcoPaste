import { readFileSync, writeFileSync } from "node:fs";

const TAURI_FILE_ASSOCIATIONS_MARKER =
  "<!-- tauri-file-associations. AUTO-GENERATED. DO NOT REMOVE. -->";

/**
 * 清理由 Tauri 文件关联生成器写入的纯空白行，不触碰生成区外的 Manifest 内容。
 */
export function normalizeTauriFileAssociationWhitespace(manifest: string) {
  const lines = manifest.split("\n");
  const markerIndexes = lines.flatMap((line, index) => {
    return line.includes(TAURI_FILE_ASSOCIATIONS_MARKER) ? [index] : [];
  });
  if (markerIndexes.length !== 2) return manifest;

  const [startIndex, endIndex] = markerIndexes;
  return lines
    .map((line, index) => {
      if (index <= startIndex || index >= endIndex) return line;

      return line.trim().length === 0 ? "" : line;
    })
    .join("\n");
}

/** 构建结束后仅在内容确实变化时回写 Android Manifest。 */
export function normalizeAndroidManifestFile(manifestPath: string) {
  const manifest = readFileSync(manifestPath, "utf8");
  const normalized = normalizeTauriFileAssociationWhitespace(manifest);
  if (normalized === manifest) return false;

  writeFileSync(manifestPath, normalized);
  return true;
}
