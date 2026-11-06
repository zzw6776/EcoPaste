interface StorageTone {
  stroke: string;
  text: string;
}

/**
 * 把字节数格式化成侧栏里的紧凑存储标签。
 */
export function formatBytes(bytes: number) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }

  const digits = unitIndex === 0 || size >= 10 ? 0 : 1;

  return `${size.toFixed(digits)} ${units[unitIndex]}`;
}

/**
 * 返回当前总量相对展示目标的实际百分比。
 */
export function storageMeterPercent(totalBytes: number, limitBytes: number) {
  if (limitBytes <= 0 || totalBytes <= 0) return 0;

  return Math.min((totalBytes / limitBytes) * 100, 100);
}

/**
 * 按本地数据总量返回状态色：低占用绿色、中等黄色、高占用红色。
 */
export function storageTone(percent: number): StorageTone {
  if (percent >= 100) {
    return {
      stroke: "var(--ant-color-error)",
      text: "text-ant-error",
    };
  }

  if (percent >= 80) {
    return {
      stroke: "var(--ant-color-warning)",
      text: "text-ant-warning",
    };
  }

  return {
    stroke: "var(--ant-color-success)",
    text: "text-ant-success",
  };
}
