import { log } from "./log";

const recordedStages = new Set<string>();

/** 每个 WebView 的启动阶段只记录一次，耗时从页面导航开始计算。 */
export function recordStartupTiming(
  stage: string,
  details: Record<string, string | number | boolean> = {},
) {
  if (recordedStages.has(stage)) return;

  recordedStages.add(stage);
  log.info("startup timing", {
    atMs: Date.now(),
    navigationStartedAtMs: Math.round(performance.timeOrigin),
    sinceNavigationMs: Math.round(performance.now()),
    stage,
    ...details,
  });
}
