import { useEffect, useRef } from "react";
import {
  type AndroidBackScope,
  registerAndroidBackHandler,
} from "@/utils/androidBack";
import { isAndroid } from "@/utils/is";

/** 让受控交互层或页面接入 Android 系统返回栈。 */
export function useAndroidBack(
  active: boolean,
  onBack: () => void,
  scope: AndroidBackScope = "layer",
) {
  const onBackRef = useRef(onBack);
  onBackRef.current = onBack;

  useEffect(() => {
    if (!isAndroid || !active) return;

    return registerAndroidBackHandler(() => {
      onBackRef.current();
    }, scope);
  }, [active, scope]);
}
