export type FileCollectionKind = "files" | "folders" | "items";

export interface FileCollectionMeta {
  count: number;
  kind: FileCollectionKind;
}

/** 根据完整的顶层路径类型序列生成文件卡片的数量分类。 */
export function getFileCollectionMeta(
  content: string,
  fileTypes: string | null,
): FileCollectionMeta {
  const count = content.split("\n").filter((path) => path.length > 0).length;
  const types = fileTypes?.split(",") ?? [];
  const hasCompleteTypes =
    count > 0 &&
    types.length === count &&
    types.every((type) => type === "d" || type === "f");

  if (!hasCompleteTypes) return { count, kind: "items" };
  if (types.every((type) => type === "d")) {
    return { count, kind: "folders" };
  }
  if (types.every((type) => type === "f")) {
    return { count, kind: "files" };
  }

  return { count, kind: "items" };
}
