import { Modal as AntdModal, type ModalProps } from "antd";
import type { FC } from "react";
import { useAndroidBack } from "@/hooks/useAndroidBack";

export interface AppModalProps extends Omit<ModalProps, "onCancel"> {
  onCancel: () => void;
}

/** 统一接入 Android 系统返回栈的受控 Modal。 */
const Modal: FC<AppModalProps> = (props) => {
  const { onCancel, open, ...rest } = props;

  useAndroidBack(open === true, onCancel);

  return <AntdModal {...rest} onCancel={onCancel} open={open} />;
};

export default Modal;
