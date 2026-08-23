import { invoke } from "@tauri-apps/api/core";

export interface AndroidPermissionsStatus {
  overlayGranted: boolean;
  accessibilityGranted: boolean;
  notificationGranted: boolean;
  batteryIgnored: boolean;
  rootAvailable: boolean;
  rootClipboardGranted: boolean;
  overlayServiceRunning: boolean;
  engineMode: string;
}

export interface AndroidEngineResult {
  success: boolean;
  mode: string;
  rootClipboardGranted: boolean;
  message: string;
}

/**
 * 获取 Android 端各项系统权限与服务开启状态
 */
export const getAndroidPermissionsStatus =
  async (): Promise<AndroidPermissionsStatus> => {
    return await invoke<AndroidPermissionsStatus>(
      "get_android_permissions_status",
    );
  };

/**
 * 请求跳转系统权限设置页或触发授权
 */
export const requestAndroidPermission = async (
  kind: "overlay" | "accessibility" | "battery" | "notification",
): Promise<void> => {
  await invoke("request_android_permission", { kind });
};

/**
 * 启停屏幕底角上滑手势悬浮服务
 */
export const toggleAndroidOverlayService = async (
  enabled: boolean,
): Promise<void> => {
  await invoke("toggle_android_overlay_service", { enabled });
};

/**
 * 最小化退回后台
 */
export const minimizeAndroidApp = async (): Promise<void> => {
  await invoke("minimize_android_app");
};

/**
 * 切换剪贴板监听引擎模式
 */
export const setAndroidEngineMode = async (
  mode: "accessibility" | "root" | "foreground",
): Promise<AndroidEngineResult> => {
  return await invoke<AndroidEngineResult>("set_android_engine_mode", { mode });
};
