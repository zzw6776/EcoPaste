import { useEffect, useRef } from "react";

const CLOSE_APP_OVERLAYS_EVENT = "ecopaste://close-app-overlays";

/** 通知当前 WebView 中的全部临时浮层关闭。 */
export function closeAppOverlays() {
  window.dispatchEvent(new Event(CLOSE_APP_OVERLAYS_EVENT));
}

/** 浮层打开时监听统一关闭通知，并调用自身的受控关闭入口。 */
export function useCloseAppOverlay(open: boolean, onClose: () => void) {
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useEffect(() => {
    if (!open) return;

    const handleClose = () => {
      onCloseRef.current();
    };
    window.addEventListener(CLOSE_APP_OVERLAYS_EVENT, handleClose);

    return () => {
      window.removeEventListener(CLOSE_APP_OVERLAYS_EVENT, handleClose);
    };
  }, [open]);
}
