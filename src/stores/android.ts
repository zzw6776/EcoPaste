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

export const checkAndAutoPromptAndroidPermissions = async () => {
  if (!isAndroid) return;
  try {
    const res = await getAndroidPermissionsStatus();
    androidState.status = res;
    const enginePermissionMissing =
      (res.engineMode === "accessibility" && !res.accessibilityGranted) ||
      (res.engineMode === "root" && !res.rootClipboardGranted);
    if (!res.overlayGranted || enginePermissionMissing) {
      androidState.permissionsModalOpen = true;
    }
  } catch {
    // ignore
  }
};
