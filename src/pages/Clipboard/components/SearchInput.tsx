import { Input, type InputProps, type InputRef } from "antd";
import type { ChangeEvent, FC } from "react";
import { useCallback, useEffect, useRef } from "react";
import {
  cancelClipboardSearchHandoff,
  confirmClipboardSearchHandoff,
  prepareClipboardSearchHandoff,
} from "@/commands";
import { EDITABLE_GLOBAL_KEYBOARD_PROPS } from "@/constants/keyboard";
import {
  SEARCH_HANDOFF_EDITABLE_ATTRIBUTE,
  SEARCH_HANDOFF_EDITING_EVENT,
} from "@/constants/searchHandoff";
import { prepareClipboardWindowEditableFocus } from "@/hooks/useClipboardWindowEditableFocus";

const IME_FOCUS_SETTLE_FRAMES = 2;

interface SearchInputProps extends Omit<InputProps, "prefix"> {
  blurToken?: number;
  clearToken?: number;
  focusCursor?: "auto" | "end";
  focusToken?: number;
  handoffSessionId?: number | null;
}

/**
 * 搜索输入框，支持 ⌘F / Ctrl+F 聚焦与流畅中文 / 英文输入。
 */
const SearchInput: FC<SearchInputProps> = (props) => {
  const {
    blurToken = 0,
    clearToken = 0,
    focusCursor = "auto",
    focusToken = 0,
    handoffSessionId = null,
    onChange,
    ...rest
  } = props;

  const inputRef = useRef<InputRef>(null);
  const handledHandoffSessionRef = useRef<number | null>(null);

  /**
   * 聚焦搜索框并选中已有内容，便于直接覆盖输入。
   */
  const focusSearch = useCallback(async () => {
    const input = inputRef.current?.input;
    const pendingHandoff =
      handoffSessionId !== null &&
      handledHandoffSessionRef.current !== handoffSessionId;
    if (!input) {
      if (pendingHandoff) {
        handledHandoffSessionRef.current = handoffSessionId;
        await cancelClipboardSearchHandoff(handoffSessionId);
      }
      return;
    }

    if (pendingHandoff) {
      handledHandoffSessionRef.current = handoffSessionId;
      const prepared = await prepareClipboardSearchHandoff(handoffSessionId);
      if (!prepared) return;

      input.setAttribute(SEARCH_HANDOFF_EDITABLE_ATTRIBUTE, "true");
      input.focus();
      if (document.activeElement !== input) {
        input.removeAttribute(SEARCH_HANDOFF_EDITABLE_ATTRIBUTE);
        await cancelClipboardSearchHandoff(handoffSessionId);
        return;
      }

      await waitForImeFocus();
      if (!document.contains(input) || document.activeElement !== input) {
        input.removeAttribute(SEARCH_HANDOFF_EDITABLE_ATTRIBUTE);
        await cancelClipboardSearchHandoff(handoffSessionId);
        return;
      }

      const confirmed = await confirmClipboardSearchHandoff(handoffSessionId);
      if (confirmed) {
        window.dispatchEvent(new Event(SEARCH_HANDOFF_EDITING_EVENT));
      }
      input.removeAttribute(SEARCH_HANDOFF_EDITABLE_ATTRIBUTE);
      if (!confirmed) input.blur();
      return;
    }

    await prepareClipboardWindowEditableFocus();
    const isAlreadyFocused = document.activeElement === input;
    const cursor = focusCursor === "end" || isAlreadyFocused ? "end" : "all";
    inputRef.current?.focus({ cursor });
  }, [focusCursor, handoffSessionId]);

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
      {...EDITABLE_GLOBAL_KEYBOARD_PROPS}
      key={clearToken}
      onChange={handleChange}
      prefix={<i className="i-lucide:search size-3.5 text-neutral-400" />}
      ref={inputRef}
      spellCheck={false}
      {...rest}
    />
  );
};

/** 等待 WebView 提交原生焦点与 IME 上下文，再请求 Rust 重放首批物理按键。 */
async function waitForImeFocus() {
  for (let frame = 0; frame < IME_FOCUS_SETTLE_FRAMES; frame += 1) {
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => {
        resolve();
      });
    });
  }
}

export default SearchInput;
