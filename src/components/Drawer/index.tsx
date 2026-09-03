import { Drawer as AntdDrawer, type DrawerProps } from "antd";
import type { FC } from "react";
import { useAndroidBack } from "@/hooks/useAndroidBack";

export interface AppDrawerProps extends Omit<DrawerProps, "onClose"> {
  onClose: () => void;
}

/** 统一接入 Android 系统返回栈的受控 Drawer。 */
const Drawer: FC<AppDrawerProps> = (props) => {
  const { onClose, open, ...rest } = props;

  useAndroidBack(open === true, onClose);

  return <AntdDrawer {...rest} onClose={onClose} open={open} />;
};

export default Drawer;
