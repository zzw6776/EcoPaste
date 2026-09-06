import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useEffect } from "react";
import { notifyWindowReady } from "@/commands";
import { isTauri } from "@/utils/is";

let reported = false;

/** 当前路由和首屏数据提交到 DOM 后再上报；隐藏窗口也能完成预加载。 */
export function useWindowReady(ready = true) {
  useEffect(() => {
    if (!ready || !isTauri || reported) return;
    let active = true;
    queueMicrotask(() => {
      if (!active || reported) return;
      reported = true;
      void notifyWindowReady(getCurrentWebviewWindow().label);
    });
    return () => {
      active = false;
    };
  }, [ready]);
}
