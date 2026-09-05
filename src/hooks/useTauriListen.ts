import {
  type EventCallback,
  type EventName,
  listen,
} from "@tauri-apps/api/event";
import { useMount, useUnmount } from "ahooks";
import { useRef } from "react";
import { isTauri } from "@/utils/is";
import { log } from "@/utils/log";

type SharedEvent = Parameters<EventCallback<unknown>>[0];
type SharedEventCallback = (event: SharedEvent) => void;

interface SharedEventSubscription {
  callbacks: Set<SharedEventCallback>;
  starting: boolean;
  unlisten?: () => void;
}

const sharedEventSubscriptions = new Map<EventName, SharedEventSubscription>();

/**
 * 订阅 Tauri 事件，组件卸载时自动取消监听；同一 WebView 内相同事件共用原生监听。
 */
export const useTauriListen = <T>(
  event: EventName,
  handler: EventCallback<T>,
) => {
  const unlistenRef = useRef<(() => void) | undefined>(void 0);
  const handlerRef = useRef(handler);
  handlerRef.current = handler;

  useMount(() => {
    if (!isTauri) return;

    unlistenRef.current = subscribeSharedEvent(event, (payload) => {
      handlerRef.current(payload as Parameters<EventCallback<T>>[0]);
    });
  });

  useUnmount(() => {
    unlistenRef.current?.();
  });
};

/** 注册共享事件回调，并在最后一个调用方离开时释放底层 Tauri 监听。 */
function subscribeSharedEvent(event: EventName, callback: SharedEventCallback) {
  let subscription = sharedEventSubscriptions.get(event);
  if (!subscription) {
    subscription = {
      callbacks: new Set(),
      starting: false,
    };
    sharedEventSubscriptions.set(event, subscription);
  }

  subscription.callbacks.add(callback);
  if (!subscription.starting && !subscription.unlisten) {
    subscription.starting = true;
    void startSharedEventListener(event, subscription);
  }

  return () => {
    subscription.callbacks.delete(callback);
    if (subscription.callbacks.size > 0) return;

    if (sharedEventSubscriptions.get(event) === subscription) {
      sharedEventSubscriptions.delete(event);
    }
    subscription.unlisten?.();
    subscription.unlisten = void 0;
  };
}

/** 建立单个底层监听；异步创建完成前若已无人订阅则立即释放。 */
async function startSharedEventListener(
  event: EventName,
  subscription: SharedEventSubscription,
) {
  try {
    const unlisten = await listen<unknown>(event, (payload) => {
      for (const callback of [...subscription.callbacks]) {
        try {
          callback(payload);
        } catch (error) {
          log.error(`Tauri event handler failed: ${event}`, error);
        }
      }
    });

    if (
      sharedEventSubscriptions.get(event) !== subscription ||
      subscription.callbacks.size === 0
    ) {
      unlisten();
      return;
    }

    subscription.unlisten = unlisten;
  } catch (error) {
    log.error(`Failed to listen tauri event: ${event}`, error);
  } finally {
    subscription.starting = false;
  }
}
