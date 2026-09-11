export const EDITABLE_GLOBAL_KEYBOARD_ATTRIBUTE = "data-allow-global-keyboard";
export const EDITABLE_GLOBAL_KEYBOARD_SELECTOR = `[${EDITABLE_GLOBAL_KEYBOARD_ATTRIBUTE}="true"]`;
export const EDITABLE_GLOBAL_KEYBOARD_PROPS = {
  [EDITABLE_GLOBAL_KEYBOARD_ATTRIBUTE]: "true",
} as const;

/** 从卡片键盘导航返回剪贴板搜索框。 */
export const CLIPBOARD_FOCUS_SEARCH_EVENT = "ecopaste:focus-search";

/** 在列表未处于编辑态时，直接输入字符并切换到剪贴板搜索框。 */
export const CLIPBOARD_TYPE_TO_SEARCH_EVENT = "ecopaste:type-to-search";
