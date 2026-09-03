import { Image as AntdImage, type ImageProps } from "antd";
import type { FC } from "react";
import { useState } from "react";
import { useAndroidBack } from "@/hooks/useAndroidBack";
import { isAndroid } from "@/utils/is";

/** 统一接入 Android 系统返回栈的图片预览。 */
const Image: FC<ImageProps> = (props) => {
  const { preview, ...rest } = props;
  const [innerOpen, setInnerOpen] = useState(false);
  const previewEnabled = preview !== false;
  const previewConfig = typeof preview === "object" ? preview : {};
  const {
    onOpenChange,
    onVisibleChange,
    open: controlledOpen,
    visible: controlledVisible,
    ...previewRest
  } = previewConfig;
  const mergedOpen = controlledOpen ?? controlledVisible ?? innerOpen;

  const handleOpenChange = (nextOpen: boolean) => {
    setInnerOpen(nextOpen);
    onOpenChange?.(nextOpen);
    onVisibleChange?.(nextOpen, mergedOpen);
  };

  useAndroidBack(previewEnabled && mergedOpen, () => {
    handleOpenChange(false);
  });

  return (
    <AntdImage
      {...rest}
      preview={
        !isAndroid
          ? preview
          : previewEnabled
            ? {
                ...previewRest,
                onOpenChange: handleOpenChange,
                open: mergedOpen,
              }
            : false
      }
    />
  );
};

export default Image;
