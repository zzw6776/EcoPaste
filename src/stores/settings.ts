import { listen } from "@tauri-apps/api/event";
import { proxy } from "valtio";

import {
  getSettings,
  resetSettings as invokeResetSettings,
  updateSettings as invokeUpdateSettings,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import type { Settings, SettingsPatch } from "@/types/settings";
import { isTauri } from "@/utils/is";
import { log } from "@/utils/log";
import { DEFAULT_MOCK_SETTINGS } from "@/utils/mockData";

const SETTINGS_STARTUP_TIMEOUT_MS = 1500;

export type SettingsInitializationStatus = "loading" | "ready" | "degraded";

/**
 * 设置的本地镜像，真相源在 Rust（`SettingsStore`）。
 *
 * 数据流向是**严格单向**的：
 *   组件 → `updateSettings(patch)` → Rust 落盘 → Rust `emit("settings://updated")`
 *   → 全部窗口的 `listen` → `Object.assign(settingsState, payload)` → 组件重渲染
 *
 * `updateSettings` 使用 Rust 返回的权威快照立即回填，事件广播继续负责同步其它窗口；
 * 两条路径的数据都来自同一个 Rust `SettingsStore`，不会产生本地乐观状态漂移。
 *
 * 首屏先使用完整默认快照，Rust 设置到达后再原子覆盖。任何启动期 IPC 异常都不能阻塞渲染。
 */
export const settingsState = proxy<Settings>(
  structuredClone(DEFAULT_MOCK_SETTINGS),
);

export const settingsInitializationState = proxy<{
  status: SettingsInitializationStatus;
}>({
  status: isTauri ? "loading" : "ready",
});

let settingsUpdateSequence = 0;
let settingsHydrationCompleted = false;

/**
 * 启动期一次性初始化：事件监听与首屏快照独立建立，二者都不会阻塞 React 根节点。
 * 每个 WebView 加载本模块一次，因此事件订阅天然单例。
 */
async function startSettingsListener() {
  try {
    await listen<Settings>(TAURI_EVENT.SETTINGS_UPDATED, (event) => {
      settingsUpdateSequence += 1;
      Object.assign(settingsState, event.payload);
      settingsInitializationState.status = "ready";
    });
    // 首次快照若先于监听注册完成，注册后再校准一次，封住二者之间的丢事件窗口。
    if (settingsHydrationCompleted) void hydrateSettingsFromRust();
  } catch (error) {
    log.error("settings event listener initialization failed", error);
  }
}

/** 拉取权威设置；若订阅期间已有更新事件，保留更新事件携带的更新快照。 */
async function hydrateSettingsFromRust() {
  const requestSequence = settingsUpdateSequence;
  try {
    const initial = await getSettings();
    if (settingsUpdateSequence === requestSequence) {
      Object.assign(settingsState, initial);
    }
    settingsInitializationState.status = "ready";
  } catch (error) {
    settingsInitializationState.status = "degraded";
    log.error("initial settings snapshot failed", error);
  } finally {
    settingsHydrationCompleted = true;
  }
}

/** 给首屏等待设置一个上限；真实快照稍后到达时仍会从降级态恢复。 */
async function initializeSettings() {
  if (!isTauri) return;

  void startSettingsListener();
  const hydration = hydrateSettingsFromRust();
  await Promise.race([hydration, wait(SETTINGS_STARTUP_TIMEOUT_MS)]);

  if (settingsInitializationState.status !== "loading") return;

  settingsInitializationState.status = "degraded";
  log.warn("initial settings snapshot timed out", {
    timeoutMs: SETTINGS_STARTUP_TIMEOUT_MS,
  });
}

/** 保留可观测的初始化 Promise，供测试或诊断等待；应用渲染不再依赖它。 */
export const settingsReady: Promise<void> = initializeSettings();

function wait(milliseconds: number) {
  return new Promise<void>((resolve) => {
    window.setTimeout(resolve, milliseconds);
  });
}

/**
 * 提交设置补丁并回填 Rust 返回的权威快照；广播负责同步其它窗口。
 */
export async function updateSettings(patch: SettingsPatch): Promise<Settings> {
  const requestSequence = settingsUpdateSequence;
  const settings = await invokeUpdateSettings(patch);
  applyCommandSnapshot(settings, requestSequence);
  return settings;
}

/**
 * 恢复所有设置默认值并回填 Rust 返回的权威快照。
 */
export async function resetSettings(): Promise<Settings> {
  const requestSequence = settingsUpdateSequence;
  const settings = await invokeResetSettings();
  applyCommandSnapshot(settings, requestSequence);
  return settings;
}

/** 命令等待期间若已收到更新事件，不允许较早的命令响应覆盖较新的事件快照。 */
function applyCommandSnapshot(settings: Settings, requestSequence: number) {
  if (settingsUpdateSequence !== requestSequence) return;

  settingsUpdateSequence += 1;
  Object.assign(settingsState, settings);
  settingsInitializationState.status = "ready";
}
