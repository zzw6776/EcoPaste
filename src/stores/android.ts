import { proxy } from "valtio";
import {
  type AndroidPermissionsStatus,
  getAndroidPermissionsStatus,
} from "@/commands/android";
import { isAndroid } from "@/utils/is";

interface AndroidState {
  permissionsModalOpen: boolean;
  status: AndroidPermissionsStatus | null;
}

export const androidState = proxy<AndroidState>({
  permissionsModalOpen: false,
  status: null,
});

export const openAndroidPermissionsModal = () => {
  androidState.permissionsModalOpen = true;
};

export const closeAndroidPermissionsModal = () => {
  androidState.permissionsModalOpen = false;
};

/** 刷新 Android 原生权限与服务状态，并写入共享 UI 镜像。 */
export const refreshAndroidPermissionsStatus = async () => {
  if (!isAndroid) return null;

  try {
    const res = await getAndroidPermissionsStatus();
    androidState.status = res;
    return res;
  } catch {
    return null;
  }
};

/** 只在用户尚未明确选择完整或基础模式时自动展示一次引导。 */
export const checkAndAutoPromptAndroidPermissions = async () => {
  const res = await refreshAndroidPermissionsStatus();
  if (res && !res.modeSelected) {
    androidState.permissionsModalOpen = true;
  }
};
