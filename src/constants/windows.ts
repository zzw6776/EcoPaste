/**
 * Tauri 窗口 label 常量。
 */
export const WINDOW_LABEL = {
  /**
   * 剪贴板窗口（剪贴板历史列表）。
   */
  CLIPBOARD: "clipboard",
  /**
   * 云端节点与网络详情弹窗窗口。
   */
  CLOUD_DETAILS: "cloud-details",
  /**
   * 云端设备管理弹窗窗口。
   */
  CLOUD_DEVICES: "cloud-devices",
  /**
   * 云端剪贴板记录弹窗窗口。
   */
  CLOUD_RECORDS: "cloud-records",
  /**
   * 自定义右键菜单窗口（仅 Windows，绕开 muda `TrackPopupMenu` 抢焦点的问题）。
   */
  CONTEXT_MENU: "context-menu",
  /**
   * Custom context submenu window on Windows.
   */
  CONTEXT_SUBMENU: "context-submenu",
  /**
   * 局域网节点与设备详情弹窗窗口。
   */
  LAN_DETAILS: "lan-details",
  /**
   * 首次启动引导窗口。
   */
  ONBOARDING: "onboarding",
  /**
   * 偏好设置窗口。
   */
  PREFERENCE: "preference",
  /**
   * 剪贴板系统级预览 overlay 窗口。
   */
  PREVIEW: "clipboard-preview",
  /**
   * 软件更新窗口。
   */
  UPDATE: "update",
} as const;
