import { convertFileSrc } from "@tauri-apps/api/core";
import type { FC, ImgHTMLAttributes } from "react";
import { useState } from "react";
import { cn } from "@/utils/cn";
import { isTauri } from "@/utils/is";

interface AssetImageProps
  extends Omit<ImgHTMLAttributes<HTMLImageElement>, "src"> {
  src?: string | null;
  protocol?: string;
  fallbackSrc?: string;
}

/**
 * 统一渲染 Tauri 本地文件图片：输入本地绝对文件路径，内部转成 asset:// 可加载 URL。
 */
const AssetImage: FC<AssetImageProps> = (props) => {
  const { alt, protocol, src, fallbackSrc, className, ...rest } = props;
  const [loadFailed, setLoadFailed] = useState(false);

  if (!src) return null;

  const resolvedUrl = loadFailed
    ? fallbackSrc || ""
    : toAssetUrl(src, protocol);

  if (!resolvedUrl) return null;

  return (
    <img
      alt={alt}
      className={cn("pointer-events-none", className)}
      onError={() => setLoadFailed(true)}
      src={resolvedUrl}
      {...rest}
    />
  );
};

/**
 * 把本地文件路径转为 webview 可访问地址；空路径返回空字符串以避免异常。
 */
export const toAssetUrl = (filePath?: string | null, protocol?: string) => {
  if (!filePath) return "";

  if (
    filePath.startsWith("http://") ||
    filePath.startsWith("https://") ||
    filePath.startsWith("data:")
  ) {
    return filePath;
  }

  if (isTauri) {
    try {
      if (!protocol) return convertFileSrc(filePath);

      return convertFileSrc(filePath, protocol);
    } catch {
      return filePath;
    }
  }

  return filePath;
};

export default AssetImage;
