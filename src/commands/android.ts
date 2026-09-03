import { invoke } from "@tauri-apps/api/core";

export type AndroidMode = "basic" | "full";
export type AndroidRootStatus = "authorized" | "unavailable" | "unknown";

export interface AndroidPermissionsStatus {
  overlayGranted: boolean;
  notificationGranted: boolean;
  batteryIgnored: boolean;
  rootStatus: AndroidRootStatus;
  overlayServiceRunning: boolean;
  clipboardMonitorRunning: boolean;
  gestureMonitorRunning: boolean;
  foregroundCaptureRunning: boolean;
  mode: AndroidMode;
  modeSelected: boolean;
}

export interface AndroidRootResult {
  success: boolean;
  rootStatus: AndroidRootStatus;
  message: string;
}

export interface AndroidModeResult {
  success: boolean;
  mode: AndroidMode;
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
  kind: "overlay" | "battery" | "notification",
): Promise<void> => {
  await invoke("request_android_permission", { kind });
};

/**
 * 最小化退回后台
 */
export const minimizeAndroidApp = async (): Promise<void> => {
  await invoke("minimize_android_app");
};

/** 仅在用户明确点击时请求 Root 管理器授权。 */
export const authorizeAndroidRoot = async (): Promise<AndroidRootResult> => {
  return await invoke<AndroidRootResult>("authorize_android_root");
};

/** 切换 Android 完整/基础运行模式。 */
export const setAndroidMode = async (
  mode: AndroidMode,
): Promise<AndroidModeResult> => {
  return await invoke<AndroidModeResult>("set_android_mode", { mode });
};
