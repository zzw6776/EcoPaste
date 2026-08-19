import { Input, type InputProps, type InputRef } from "antd";
import type { ChangeEvent, FC } from "react";
import { useCallback, useEffect, useRef } from "react";
import { prepareClipboardWindowEditableFocus } from "@/hooks/useClipboardWindowEditableFocus";

interface SearchInputProps extends Omit<InputProps, "prefix"> {
  blurToken?: number;
  clearToken?: number;
  focusToken?: number;
}

/**
 * 搜索输入框，支持 ⌘F / Ctrl+F 聚焦与流畅中文 / 英文输入。
 */
const SearchInput: FC<SearchInputProps> = (props) => {
  const {
    blurToken = 0,
    clearToken = 0,
    focusToken = 0,
    onChange,
    ...rest
  } = props;

  const inputRef = useRef<InputRef>(null);

  /**
   * 聚焦搜索框并选中已有内容，便于直接覆盖输入。
   */
  const focusSearch = useCallback(async () => {
    if (!inputRef.current) return;

    await prepareClipboardWindowEditableFocus();
    const isAlreadyFocused = document.activeElement === inputRef.current.input;
    inputRef.current?.focus({ cursor: isAlreadyFocused ? "end" : "all" });
  }, []);

  useEffect(() => {
    if (blurToken <= 0) return;

    inputRef.current?.blur();
  }, [blurToken]);

  useEffect(() => {
    if (focusToken <= 0) return;

    const frame = requestAnimationFrame(() => {
      void focusSearch();
    });

    return () => {
      cancelAnimationFrame(frame);
    };
  }, [focusToken, focusSearch]);

  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange?.(event);
  };

  return (
    <Input
      autoCapitalize="off"
      autoCorrect="off"
      data-allow-global-keyboard="true"
      key={clearToken}
      onChange={handleChange}
      prefix={<i className="i-lucide:search size-3.5 text-neutral-400" />}
      ref={inputRef}
      spellCheck={false}
      {...rest}
    />
  );
};

export default SearchInput;
