import { message as staticMessage, Modal as staticModal } from "antd";
import type { MessageInstance } from "antd/es/message/interface";
import { registerAndroidBackHandler } from "@/utils/androidBack";
import { isAndroid } from "@/utils/is";

type ModalConfirmApi = Pick<typeof staticModal, "confirm">;
type ModalConfirmConfig = Parameters<ModalConfirmApi["confirm"]>[0];
type ModalConfirmResult = ReturnType<ModalConfirmApi["confirm"]>;
type ModalConfirmUpdate = Parameters<ModalConfirmResult["update"]>[0];

let messageApi: MessageInstance = staticMessage;
let rawModalApi: ModalConfirmApi = staticModal;
const activeConfirmCancels = new Set<() => void>();

/**
 * 统一登记命令式确认框；Android 系统返回与主窗口隐藏都等价于取消确认框。
 */
function confirmWithLifecycle(
  initialConfig: ModalConfirmConfig,
): ModalConfirmResult {
  let currentConfig = initialConfig;
  let unregister = () => {};
  let closed = false;

  const cleanup = () => {
    if (closed) return false;

    closed = true;
    unregister();
    activeConfirmCancels.delete(cancel);

    return true;
  };
  const decorateConfig = (config: ModalConfirmConfig): ModalConfirmConfig => {
    const { afterClose, ...rest } = config;

    return {
      ...rest,
      afterClose: () => {
        cleanup();
        afterClose?.();
      },
    };
  };
  const dialog = rawModalApi.confirm(decorateConfig(currentConfig));

  function cancel() {
    if (!cleanup()) return;

    try {
      currentConfig.onCancel?.(() => {});
    } finally {
      dialog.destroy();
    }
  }

  activeConfirmCancels.add(cancel);
  if (isAndroid) unregister = registerAndroidBackHandler(cancel);

  return {
    destroy: () => {
      if (!cleanup()) return;

      dialog.destroy();
    },
    update: (configUpdate: ModalConfirmUpdate) => {
      currentConfig =
        typeof configUpdate === "function"
          ? configUpdate(currentConfig)
          : { ...currentConfig, ...configUpdate };
      dialog.update(decorateConfig(currentConfig));
    },
  };
}

const modalApi: ModalConfirmApi = {
  confirm: confirmWithLifecycle,
};

export function setMessageApi(api: MessageInstance): void {
  messageApi = api;
}

export function getMessageApi(): MessageInstance {
  return messageApi;
}

export function setModalApi(api: ModalConfirmApi): void {
  rawModalApi = api;
}

export function getModalApi(): ModalConfirmApi {
  return modalApi;
}

/** 以取消语义关闭当前窗口中的全部命令式确认框。 */
export function closeModalDialogs(): void {
  for (const cancel of [...activeConfirmCancels]) cancel();
}
