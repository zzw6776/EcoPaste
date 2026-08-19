import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { platform } from "@tauri-apps/plugin-os";
import { WINDOW_LABEL } from "@/constants/windows";

/**
 * 当前是否运行在 Tauri 桌面客户端容器中。
 */
export const isTauri =
  typeof window !== "undefined" &&
  Boolean(
    (window as unknown as { __TAURI_INTERNALS__?: unknown })
      .__TAURI_INTERNALS__,
  );

let currentPlatform = "macos";
if (isTauri) {
  try {
    currentPlatform = platform();
  } catch {
    // fallback
  }
}

/**
 * 当前是否运行在 macOS 平台。
 */
export const isMac = currentPlatform === "macos";

/**
 * 当前是否运行在 Windows 平台。
 */
export const isWin = currentPlatform === "windows";

/**
 * 当前是否运行在 Android 平台。
 */
export const isAndroid = currentPlatform === "android";

/**
 * 当前是否为移动端环境（Android、iOS 或移动视口宽度）。
 */
export const isMobile = () => {
  if (typeof window === "undefined") return false;
  return (
    isAndroid ||
    window.innerWidth < 640 ||
    /Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent)
  );
};

/**
 * 当前是否为 Vite dev 构建（开发模式）。生产构建为 false。
 */
export const isDev = import.meta.env.DEV;

/**
 * 当前是否为 Windows 平台的剪贴板窗口（focusable=false，需要低级键盘钩子）。
 */
export const isWinClipboardWindow = () => {
  if (!isTauri) return false;
  try {
    return isWin && getCurrentWebviewWindow().label === WINDOW_LABEL.CLIPBOARD;
  } catch {
    return false;
  }
};

/**
 * 判断路径/文件名是否为常见图片类型（按扩展名匹配，大小写不敏感）。
 */
export const isImage = (value: string) => {
  const regex = /\.(jpe?g|png|webp|avif|gif|svg|bmp|ico|tiff?|heic|apng)$/i;

  return regex.test(value);
};
