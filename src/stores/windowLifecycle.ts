import { listen } from "@tauri-apps/api/event";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { proxy } from "valtio";

import { getWindowLifecycleSnapshot } from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { isAndroid, isTauri } from "@/utils/is";
import { log } from "@/utils/log";

/**
 * 窗口生命周期阶段，与 Rust `LifecyclePhase` 一一对应（camelCase）。
 */
export type LifecyclePhase =
  | "notCreated"
  | "created"
  | "ready"
  | "visible"
  | "hiddenWarm"
  | "dormant"
  | "destroyPending"
  | "destroyed";

/**
 * Rust `window://lifecycle` 事件 payload。
 */
interface LifecyclePayload {
  label: string;
  phase: LifecyclePhase;
  generation: number;
  reason: string;
  visible: boolean;
}

const initialAndroidVisible =
  typeof document === "undefined" ? true : !document.hidden;

/**
 * 当前 WebView 的生命周期镜像。桌面端真相源在 Rust（`WindowLifecycleManager`），
 * Android 单 Activity 按前台可见性与焦点推进。
 */
export const windowLifecycleState = proxy<{
  phase: LifecyclePhase;
  visible: boolean;
  generation: number;
  initialized: boolean;
}>({
  generation: !isTauri || (isAndroid && initialAndroidVisible) ? 1 : 0,
  initialized: !isTauri || isAndroid,
  phase: !isTauri
    ? "visible"
    : isAndroid
      ? initialAndroidVisible
        ? "visible"
        : "hiddenWarm"
      : "created",
  visible: !isTauri ? true : isAndroid ? initialAndroidVisible : false,
});

const currentLabel = isTauri
  ? getCurrentWebviewWindow().label
  : WINDOW_LABEL.CLIPBOARD;

/** 读取当前窗口是否可见。 */
export const isCurrentWindowVisible = () => {
  return windowLifecycleState.visible;
};

/** 读取当前窗口是否已进入深度休眠（隐藏超过宽限期，后台应暂停非必要轮询/刷新）。 */
export const isCurrentWindowDormant = () => {
  return windowLifecycleState.phase === "dormant";
};

function installAndroidLifecycleListeners() {
  if (typeof window === "undefined" || typeof document === "undefined") return;

  const markVisible = () => {
    const nextGeneration = windowLifecycleState.visible
      ? windowLifecycleState.generation
      : windowLifecycleState.generation + 1;
    Object.assign(windowLifecycleState, {
      generation: nextGeneration,
      initialized: true,
      phase: "visible" as const,
      visible: true,
    });
  };

  const handleVisibilityChange = () => {
    if (!document.hidden) {
      markVisible();
      return;
    }

    Object.assign(windowLifecycleState, {
      initialized: true,
      phase: "hiddenWarm" as const,
      visible: false,
    });
  };

  window.addEventListener("focus", markVisible);
  document.addEventListener("visibilitychange", handleVisibilityChange);
}

/**
 * 启动期一次性订阅 `window://lifecycle` 并用 Rust 快照校准冷启动初始状态，
 * 防止桌面端首屏显示早于 JS 注册监听时漏掉首次显隐事件。
 */
export const windowLifecycleReady: Promise<void> = (async () => {
  if (!isTauri) return;
  if (isAndroid) {
    installAndroidLifecycleListeners();
    return;
  }

  let observedEvent = false;
  try {
    await listen<LifecyclePayload>(TAURI_EVENT.WINDOW_LIFECYCLE, (event) => {
      const { generation, label, phase, visible } = event.payload;
      if (label !== currentLabel) return;

      observedEvent = true;
      Object.assign(windowLifecycleState, {
        generation,
        initialized: true,
        phase,
        visible,
      });
    });
  } catch (error) {
    log.error("Failed to listen window lifecycle event", error);
  }

  if (observedEvent) return;

  try {
    const rows = await getWindowLifecycleSnapshot();
    if (observedEvent) return;

    const current = rows.find((row) => {
      return row.label === currentLabel;
    });
    if (current) {
      const nextPhase: LifecyclePhase = current.visible
        ? "visible"
        : current.phase === "visible"
          ? "hiddenWarm"
          : current.phase;
      Object.assign(windowLifecycleState, {
        generation: current.visible
          ? Math.max(1, current.generation)
          : current.generation,
        initialized: true,
        phase: nextPhase,
        visible: current.visible,
      });
      return;
    }

    const visible = await getCurrentWebviewWindow().isVisible();
    if (observedEvent) return;

    Object.assign(windowLifecycleState, {
      generation: visible ? 1 : 0,
      initialized: true,
      phase: visible ? "visible" : "hiddenWarm",
      visible,
    });
  } catch (error) {
    log.warn("Failed to hydrate initial window lifecycle snapshot", error);
    if (!observedEvent) {
      windowLifecycleState.initialized = true;
    }
  }
})();
