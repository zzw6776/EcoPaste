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

/**
 * Android 上把命令式确认框也登记到统一返回栈；系统返回等价于取消当前确认框。
 */
function confirmWithAndroidBack(
  initialConfig: ModalConfirmConfig,
): ModalConfirmResult {
  if (!isAndroid) return rawModalApi.confirm(initialConfig);

  let currentConfig = initialConfig;
  let unregister = () => {};
  const decorateConfig = (config: ModalConfirmConfig): ModalConfirmConfig => {
    const { afterClose, ...rest } = config;

    return {
      ...rest,
      afterClose: () => {
        unregister();
        afterClose?.();
      },
    };
  };
  const dialog = rawModalApi.confirm(decorateConfig(currentConfig));

  unregister = registerAndroidBackHandler(() => {
    unregister();
    try {
      currentConfig.onCancel?.(() => {});
    } finally {
      dialog.destroy();
    }
  });

  return {
    destroy: () => {
      unregister();
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
  confirm: confirmWithAndroidBack,
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
