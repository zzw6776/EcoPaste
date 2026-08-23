import { useInterval, useMount } from "ahooks";
import type { RadioChangeEvent } from "antd";
import { Button, Modal, Radio, Tag } from "antd";
import type { FC } from "react";
import { useState } from "react";
import {
  type AndroidPermissionsStatus,
  getAndroidPermissionsStatus,
  requestAndroidPermission,
  setAndroidEngineMode,
  toggleAndroidOverlayService,
} from "@/commands/android";
import { cn } from "@/utils/cn";
import { getMessageApi } from "@/utils/feedback";
import { isAndroid, isMobile } from "@/utils/is";

interface AndroidPermissionsModalProps {
  open: boolean;
  onClose?: () => void;
  onFinish?: () => void;
}

export const AndroidPermissionsModal: FC<AndroidPermissionsModalProps> = (
  props,
) => {
  const { open, onClose, onFinish } = props;
  const [status, setStatus] = useState<AndroidPermissionsStatus>({
    accessibilityGranted: false,
    batteryIgnored: false,
    engineMode: "accessibility",
    notificationGranted: false,
    overlayGranted: false,
    overlayServiceRunning: false,
    rootAvailable: false,
    rootClipboardGranted: false,
  });
  const [engineMode, setEngineMode] = useState<
    "accessibility" | "root" | "foreground"
  >("accessibility");
  const [loading, setLoading] = useState(false);
  const [engineLoading, setEngineLoading] = useState(false);
  const mobile = isAndroid || isMobile();

  const fetchStatus = async () => {
    if (!isAndroid) return;
    try {
      const res = await getAndroidPermissionsStatus();
      setStatus(res);
      if (
        res.engineMode === "accessibility" ||
        res.engineMode === "root" ||
        res.engineMode === "foreground"
      ) {
        setEngineMode(res.engineMode);
      }
    } catch {
      // ignore
    }
  };

  useMount(() => {
    void fetchStatus();
  });

  // 当弹窗打开时，每 1.5 秒自动刷新一次权限状态，方便用户从系统设置返回时自动更新
  useInterval(() => {
    if (open) {
      void fetchStatus();
    }
  }, 1500);

  const handleRequest = async (
    kind: "overlay" | "accessibility" | "battery" | "notification",
  ) => {
    await requestAndroidPermission(kind);
    void fetchStatus();
  };

  const handleEngineChange = async (
    mode: "accessibility" | "root" | "foreground",
  ) => {
    setEngineLoading(true);
    try {
      const result = await setAndroidEngineMode(mode);
      if (!result.success) {
        getMessageApi().error(result.message || "切换剪贴板引擎失败");
        return;
      }

      setEngineMode(mode);
      if (mode === "root") {
        getMessageApi().success("Root 剪贴板权限已自动授权");
      }
      await fetchStatus();
    } catch {
      getMessageApi().error("切换剪贴板引擎失败");
    } finally {
      setEngineLoading(false);
    }
  };

  const handleEngineRadioChange = (event: RadioChangeEvent) => {
    void handleEngineChange(event.target.value);
  };

  const handleFinish = async () => {
    setLoading(true);
    try {
      if (status.overlayGranted) {
        await toggleAndroidOverlayService(true);
      }
      onFinish?.();
      onClose?.();
    } finally {
      setLoading(false);
    }
  };

  const isAllEssentialReady =
    status.overlayGranted &&
    (engineMode === "accessibility"
      ? status.accessibilityGranted
      : engineMode === "root"
        ? status.rootAvailable && status.rootClipboardGranted
        : true);
  const permissionCardClass = cn(
    "flex rounded-xl border border-ant-border-secondary bg-ant-container p-3 shadow-xs",
    mobile
      ? "flex-col items-stretch gap-2"
      : "items-center justify-between gap-3",
  );
  const permissionActionClass = cn(mobile && "self-end");
  const modalTitle = (
    <div className="flex items-center gap-2 font-semibold text-base text-neutral-800 dark:text-white">
      <i
        aria-hidden="true"
        className="i-lucide:smartphone text-ant-primary text-lg"
      />
      <span>Android 核心权限与引擎配置</span>
    </div>
  );
  const modalFooter = (
    <div
      className={cn(
        "flex gap-3",
        mobile ? "items-stretch" : "items-center justify-between",
      )}
    >
      <Button block={mobile} onClick={() => void fetchStatus()} size="small">
        🔄 刷新状态
      </Button>

      <Button
        block={mobile}
        className={cn(
          "font-medium",
          isAllEssentialReady && "bg-emerald-600 hover:bg-emerald-500",
        )}
        loading={loading}
        onClick={() => void handleFinish()}
        type="primary"
      >
        {isAllEssentialReady ? "✓ 启动服务并进入应用" : "暂不开启，进入应用"}
      </Button>
    </div>
  );

  return (
    <Modal
      centered
      closable={mobile}
      footer={modalFooter}
      onCancel={onClose}
      open={open}
      style={mobile ? { paddingBottom: 0 } : void 0}
      styles={{
        body: mobile
          ? {
              flex: "1 1 auto",
              minHeight: 0,
              overflowY: "auto",
              paddingInline: 0,
            }
          : void 0,
        container: mobile
          ? {
              display: "flex",
              flexDirection: "column",
              height: "82dvh",
              paddingBottom: 0,
            }
          : void 0,
        footer: mobile
          ? {
              borderTop: "1px solid var(--ant-color-border-secondary)",
              flex: "0 0 auto",
              marginTop: 0,
              paddingBottom: "var(--mobile-safe-area-bottom)",
              paddingTop: 12,
            }
          : void 0,
      }}
      title={modalTitle}
      width={mobile ? "calc(100vw - 1.5rem)" : 480}
    >
      <div
        className={cn(
          "flex select-none flex-col pt-2 text-xs",
          mobile ? "gap-3" : "gap-4",
        )}
      >
        <p className="text-neutral-500 leading-relaxed dark:text-neutral-400">
          为实现<b>后台剪贴板实时捕获</b>与<b>屏幕底部上滑呼出</b>
          ，请根据指引开启以下系统能力：
        </p>

        {/* 剪贴板监听方案选择 */}
        <div className="rounded-xl border border-ant-border-secondary bg-ant-fill-quaternary p-3">
          <div className="mb-2 font-medium text-neutral-700 dark:text-neutral-200">
            剪贴板监听引擎
          </div>
          <Radio.Group
            className="flex flex-col gap-2"
            disabled={engineLoading}
            onChange={handleEngineRadioChange}
            value={engineMode}
          >
            <Radio
              className="m-0 rounded-lg border border-ant-border-secondary bg-ant-container p-2.5"
              value="accessibility"
            >
              <div className="flex flex-wrap items-center gap-1.5">
                <span className="font-medium text-neutral-800 dark:text-neutral-200">
                  无障碍服务模式
                </span>
                <Tag className="m-0" color="blue">
                  推荐 / 免Root
                </Tag>
              </div>
              <div className="mt-0.5 text-[11px] text-neutral-400">
                后台实时采集剪贴板，支持选择记录后自动模拟粘贴
              </div>
            </Radio>

            <Radio
              className="m-0 rounded-lg border border-ant-border-secondary bg-ant-container p-2.5"
              disabled={!status.rootAvailable}
              value="root"
            >
              <div className="flex flex-wrap items-center gap-1.5">
                <span className="font-medium text-neutral-800 dark:text-neutral-200">
                  Root 授权模式
                </span>
                <Tag
                  className="m-0"
                  color={
                    status.rootClipboardGranted
                      ? "green"
                      : status.rootAvailable
                        ? "blue"
                        : "default"
                  }
                >
                  {status.rootClipboardGranted
                    ? "剪贴板已授权"
                    : status.rootAvailable
                      ? "选择后自动授权"
                      : "未检测到 Root"}
                </Tag>
              </div>
              <div className="mt-0.5 text-[11px] text-neutral-400">
                通过系统 AppOps 权限直接解锁后台剪贴板访问
              </div>
            </Radio>

            <Radio
              className="m-0 rounded-lg border border-ant-border-secondary bg-ant-container p-2.5"
              value="foreground"
            >
              <span className="font-medium text-neutral-800 dark:text-neutral-200">
                前台常驻轮询模式
              </span>
              <div className="mt-0.5 text-[11px] text-neutral-400">
                仅在前台或常驻前台服务时轮询剪贴板
              </div>
            </Radio>
          </Radio.Group>
        </div>

        {/* 权限列表 */}
        <div className="flex flex-col gap-2.5">
          {/* 1. 悬浮窗权限 */}
          <div className={permissionCardClass}>
            <div className="flex min-w-0 flex-1 flex-col gap-0.5">
              <div className="flex flex-wrap items-center gap-1.5 font-medium text-neutral-800 dark:text-neutral-200">
                <span>悬浮窗权限 (Overlay)</span>
                {status.overlayGranted ? (
                  <Tag color="success">已开启</Tag>
                ) : (
                  <Tag color="warning">待开启</Tag>
                )}
              </div>
              <span className="text-[11px] text-neutral-400">
                用于在屏幕底角监听向上滑动手势，随时呼出抽屉
              </span>
            </div>
            <Button
              className={permissionActionClass}
              disabled={status.overlayGranted}
              onClick={() => void handleRequest("overlay")}
              size="small"
              type={status.overlayGranted ? "default" : "primary"}
            >
              {status.overlayGranted ? "已就绪" : "去授权"}
            </Button>
          </div>

          {/* 2. 无障碍服务 */}
          {engineMode === "accessibility" && (
            <div className={permissionCardClass}>
              <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                <div className="flex flex-wrap items-center gap-1.5 font-medium text-neutral-800 dark:text-neutral-200">
                  <span>无障碍服务 (Accessibility)</span>
                  {status.accessibilityGranted ? (
                    <Tag color="success">已开启</Tag>
                  ) : (
                    <Tag color="warning">待开启</Tag>
                  )}
                </div>
                <span className="text-[11px] text-neutral-400">
                  在设置中找到「EcoPaste」并打开，用于后台采集与自动粘贴
                </span>
              </div>
              <Button
                className={permissionActionClass}
                disabled={status.accessibilityGranted}
                onClick={() => void handleRequest("accessibility")}
                size="small"
                type={status.accessibilityGranted ? "default" : "primary"}
              >
                {status.accessibilityGranted ? "已就绪" : "去开启"}
              </Button>
            </div>
          )}

          {/* 3. 忽略电池优化 */}
          <div className={permissionCardClass}>
            <div className="flex min-w-0 flex-1 flex-col gap-0.5">
              <div className="flex flex-wrap items-center gap-1.5 font-medium text-neutral-800 dark:text-neutral-200">
                <span>忽略电池优化 (后台保活)</span>
                {status.batteryIgnored ? (
                  <Tag color="success">已加白</Tag>
                ) : (
                  <Tag color="default">建议开启</Tag>
                )}
              </div>
              <span className="text-[11px] text-neutral-400">
                防止手机系统切后台后将剪贴板后台服务自动休眠或杀死
              </span>
            </div>
            <Button
              className={permissionActionClass}
              disabled={status.batteryIgnored}
              onClick={() => void handleRequest("battery")}
              size="small"
            >
              {status.batteryIgnored ? "已就绪" : "去加白"}
            </Button>
          </div>

          {/* 5. 通知权限 */}
          <div className={permissionCardClass}>
            <div className="flex min-w-0 flex-1 flex-col gap-0.5">
              <div className="flex flex-wrap items-center gap-1.5 font-medium text-neutral-800 dark:text-neutral-200">
                <span>通知权限</span>
                {status.notificationGranted ? (
                  <Tag color="success">已允许</Tag>
                ) : (
                  <Tag color="default">建议允许</Tag>
                )}
              </div>
              <span className="text-[11px] text-neutral-400">
                用于常驻前台服务，保障屏幕底角手势稳定响应
              </span>
            </div>
            <Button
              className={permissionActionClass}
              disabled={status.notificationGranted}
              onClick={() => void handleRequest("notification")}
              size="small"
            >
              {status.notificationGranted ? "已允许" : "去允许"}
            </Button>
          </div>
        </div>
      </div>
    </Modal>
  );
};
