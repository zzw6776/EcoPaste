import { Select as AntdSelect } from "antd";
import type {
  BaseOptionType,
  DefaultOptionType,
  SelectProps,
} from "antd/es/select";
import { useState } from "react";
import { useAndroidBack } from "@/hooks/useAndroidBack";
import { isAndroid } from "@/utils/is";

/** 统一接入 Android 系统返回栈的 Select。 */
const Select = <
  ValueType = unknown,
  OptionType extends BaseOptionType | DefaultOptionType = DefaultOptionType,
>(
  props: SelectProps<ValueType, OptionType>,
) => {
  const { onDropdownVisibleChange, onOpenChange, open, ...rest } = props;
  const [innerOpen, setInnerOpen] = useState(false);
  const mergedOpen = open ?? innerOpen;

  const handleOpenChange = (nextOpen: boolean) => {
    setInnerOpen(nextOpen);
    onOpenChange?.(nextOpen);
    onDropdownVisibleChange?.(nextOpen);
  };

  useAndroidBack(mergedOpen, () => {
    handleOpenChange(false);
  });

  return (
    <AntdSelect<ValueType, OptionType>
      {...rest}
      onOpenChange={handleOpenChange}
      open={isAndroid ? mergedOpen : open}
    />
  );
};

export default Select;
