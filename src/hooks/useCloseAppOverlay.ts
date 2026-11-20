import { useEffect, useRef, useState } from "react";

const CLOSE_APP_OVERLAYS_EVENT = "ecopaste://close-app-overlays";

/** 通知当前 WebView 中的全部临时浮层关闭。 */
export function closeAppOverlays() {
  window.dispatchEvent(new Event(CLOSE_APP_OVERLAYS_EVENT));
}

/** 浮层打开时监听统一关闭通知，调用受控关闭并返回用于立即销毁残留 DOM 的递增 key。 */
export function useCloseAppOverlay(open: boolean, onClose: () => void): number {
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const [resetKey, setResetKey] = useState(0);

  useEffect(() => {
    if (!open) return;

    const handleClose = () => {
      setResetKey((current) => {
        return current + 1;
      });
      onCloseRef.current();
    };
    window.addEventListener(CLOSE_APP_OVERLAYS_EVENT, handleClose);

    return () => {
      window.removeEventListener(CLOSE_APP_OVERLAYS_EVENT, handleClose);
    };
  }, [open]);

  return resetKey;
}
