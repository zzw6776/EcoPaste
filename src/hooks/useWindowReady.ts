import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useEffect } from "react";
import { notifyWindowReady } from "@/commands";
import { isTauri } from "@/utils/is";

let reported = false;

/** 页面可展示时上报就绪；剪贴板外壳可以先于卡片数据完成，隐藏窗口也能预加载。 */
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
