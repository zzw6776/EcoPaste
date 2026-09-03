import { useInterval, useMount } from "ahooks";
import { Button, Tag } from "antd";
import type { TFunction } from "i18next";
import type { FC, ReactNode } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useSnapshot } from "valtio";
import {
  type AndroidMode,
  type AndroidPermissionsStatus,
  authorizeAndroidRoot,
  requestAndroidPermission,
  setAndroidMode,
} from "@/commands/android";
import Modal from "@/components/Modal";
import {
  androidState,
  refreshAndroidPermissionsStatus,
} from "@/stores/android";
import { settingsState, updateSettings } from "@/stores/settings";
import { cn } from "@/utils/cn";
import { getMessageApi } from "@/utils/feedback";
import { isAndroid, isMobile } from "@/utils/is";

interface AndroidPermissionsModalProps {
  open: boolean;
  onClose?: () => void;
  onFinish?: () => void;
}

const INITIAL_STATUS: AndroidPermissionsStatus = {
  batteryIgnored: false,
  clipboardMonitorRunning: false,
  foregroundCaptureRunning: false,
  gestureMonitorRunning: false,
  mode: "basic",
  modeSelected: false,
  notificationGranted: false,
  overlayGranted: false,
  overlayServiceRunning: false,
  rootStatus: "unknown",
};

export const AndroidPermissionsModal: FC<AndroidPermissionsModalProps> = (
  props,
) => {
  const { open, onClose, onFinish } = props;
  const { t } = useTranslation("common");
  const androidSnapshot = useSnapshot(androidState);
  const status = androidSnapshot.status ?? INITIAL_STATUS;
  const [activeAction, setActiveAction] = useState<AndroidMode | "root" | null>(
    null,
  );
  const fetchingRef = useRef(false);
  const mobile = isAndroid || isMobile();

  const fetchStatus = useCallback(async () => {
    if (!isAndroid || fetchingRef.current) return;

    fetchingRef.current = true;
    try {
      await refreshAndroidPermissionsStatus();
    } finally {
      fetchingRef.current = false;
    }
  }, []);

  useMount(() => {
    void fetchStatus();
  });

  useInterval(
    () => {
      if (open) {
        void fetchStatus();
      }
    },
    open ? 1_500 : void 0,
  );

  useEffect(() => {
    if (!open) return;

    const handleResume = () => {
      if (document.visibilityState === "visible") {
        void fetchStatus();
      }
    };
    handleResume();
    window.addEventListener("focus", handleResume);
    document.addEventListener("visibilitychange", handleResume);

    return () => {
      window.removeEventListener("focus", handleResume);
      document.removeEventListener("visibilitychange", handleResume);
    };
  }, [fetchStatus, open]);

  const handleRequest = async (
    kind: "overlay" | "battery" | "notification",
  ) => {
    await requestAndroidPermission(kind);
    void fetchStatus();
  };

  const handleAuthorizeRoot = async () => {
    setActiveAction("root");
    try {
      const result = await authorizeAndroidRoot();
      if (!result.success) {
        getMessageApi().error(
          result.message || t("androidPermissions.messages.rootFailed"),
        );
        return;
      }

      getMessageApi().success(t("androidPermissions.messages.rootReady"));
      await fetchStatus();
    } catch {
      getMessageApi().error(t("androidPermissions.messages.rootFailed"));
    } finally {
      setActiveAction(null);
    }
  };

  /** 同步设置真相源后切换原生运行模式；失败时恢复手势开关。 */
  const handleSelectMode = async (mode: AndroidMode) => {
    const previousGestureEnabled = settingsState.android.gesture.enabled;
    const gestureEnabled = mode === "full";
    let gestureSettingUpdated = false;
    setActiveAction(mode);

    try {
      await updateSettings({
        android: {
          gesture: {
            enabled: gestureEnabled,
          },
        },
      });
      gestureSettingUpdated = true;
      const result = await setAndroidMode(mode);
      if (!result.success) {
        await updateSettings({
          android: {
            gesture: {
              enabled: previousGestureEnabled,
            },
          },
        });
        gestureSettingUpdated = false;
        getMessageApi().error(
          result.message || t("androidPermissions.messages.modeFailed"),
        );
        return;
      }
      gestureSettingUpdated = false;

      await fetchStatus();
      getMessageApi().success(
        t(
          mode === "full"
            ? "androidPermissions.messages.fullEnabled"
            : "androidPermissions.messages.basicEnabled",
        ),
      );
      onFinish?.();
      onClose?.();
    } catch {
      if (gestureSettingUpdated) {
        try {
          await updateSettings({
            android: {
              gesture: {
                enabled: previousGestureEnabled,
              },
            },
          });
        } catch {
          // 设置命令会显示具体错误；这里仍需结束本次模式切换。
        }
      }
      getMessageApi().error(t("androidPermissions.messages.modeFailed"));
    } finally {
      setActiveAction(null);
    }
  };

  const handleCancel = () => {
    onClose?.();
  };

  const rootAuthorized = status.rootStatus === "authorized";
  const fullModeReady =
    status.modeSelected &&
    status.mode === "full" &&
    status.overlayGranted &&
    status.clipboardMonitorRunning &&
    status.gestureMonitorRunning;
  const basicModeActive = status.modeSelected && status.mode === "basic";
  const busy = activeAction !== null;
  const permissionCardClass = cn(
    "flex rounded-xl border border-ant-border-secondary bg-ant-container p-3 shadow-xs",
    mobile
      ? "flex-col items-stretch gap-2"
      : "items-center justify-between gap-3",
  );
  const permissionActionClass = cn(mobile && "self-end");
  const modalTitle = (
    <div className="flex items-center gap-2 font-semibold text-ant-text text-base">
      <i
        aria-hidden="true"
        className="i-lucide:smartphone text-ant-primary text-lg"
      />
      <span>{t("androidPermissions.title")}</span>
    </div>
  );
  const modalFooter = (
    <div
      className={cn(
        "flex gap-2",
        mobile ? "flex-col items-stretch" : "items-center justify-end",
      )}
    >
      <Button
        block={mobile}
        disabled={busy || basicModeActive}
        loading={activeAction === "basic"}
        onClick={() => void handleSelectMode("basic")}
      >
        {basicModeActive
          ? t("androidPermissions.actions.basicActive")
          : t("androidPermissions.actions.useBasic")}
      </Button>
      <Button
        block={mobile}
        disabled={
          busy || !rootAuthorized || !status.overlayGranted || fullModeReady
        }
        loading={activeAction === "full"}
        onClick={() => void handleSelectMode("full")}
        type="primary"
      >
        {fullModeReady
          ? t("androidPermissions.actions.fullActive")
          : t("androidPermissions.actions.enableFull")}
      </Button>
    </div>
  );

  return (
    <Modal
      centered
      closable={mobile}
      footer={modalFooter}
      onCancel={handleCancel}
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
      width={mobile ? "calc(100vw - 1.5rem)" : 520}
    >
      <div
        className={cn(
          "flex select-none flex-col pt-2 text-xs",
          mobile ? "gap-3" : "gap-4",
        )}
      >
        <p className="m-0 text-ant-secondary leading-relaxed">
          {t("androidPermissions.description")}
        </p>

        <div className="grid gap-2 sm:grid-cols-2">
          <ModeCard
            description={t("androidPermissions.modes.full.description")}
            icon="i-lucide:zap"
            selected={status.modeSelected && status.mode === "full"}
            title={t("androidPermissions.modes.full.title")}
          />
          <ModeCard
            description={t("androidPermissions.modes.basic.description")}
            icon="i-lucide:panel-top"
            selected={status.modeSelected && status.mode === "basic"}
            title={t("androidPermissions.modes.basic.title")}
          />
        </div>

        <section className="flex flex-col gap-2.5">
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium text-ant-text">
              {t("androidPermissions.sections.required")}
            </span>
            <Button onClick={() => void fetchStatus()} size="small" type="text">
              {t("androidPermissions.actions.refresh")}
            </Button>
          </div>

          <div className={permissionCardClass}>
            <div className="flex min-w-0 flex-1 flex-col gap-1">
              <div className="flex flex-wrap items-center gap-1.5 font-medium text-ant-text">
                <span>{t("androidPermissions.permissions.root.title")}</span>
                <Tag color={rootAuthorized ? "success" : "warning"}>
                  {t(`androidPermissions.rootStatus.${status.rootStatus}`)}
                </Tag>
              </div>
              <span className="text-ant-secondary text-xs">
                {t("androidPermissions.permissions.root.description")}
              </span>
            </div>
            <Button
              className={permissionActionClass}
              disabled={rootAuthorized || busy}
              loading={activeAction === "root"}
              onClick={() => void handleAuthorizeRoot()}
              size="small"
              type={rootAuthorized ? "default" : "primary"}
            >
              {rootAuthorized
                ? t("androidPermissions.actions.ready")
                : t("androidPermissions.actions.authorizeRoot")}
            </Button>
          </div>

          <div className={permissionCardClass}>
            <div className="flex min-w-0 flex-1 flex-col gap-1">
              <div className="flex flex-wrap items-center gap-1.5 font-medium text-ant-text">
                <span>{t("androidPermissions.permissions.overlay.title")}</span>
                <Tag color={status.overlayGranted ? "success" : "warning"}>
                  {status.overlayGranted
                    ? t("androidPermissions.status.enabled")
                    : t("androidPermissions.status.pending")}
                </Tag>
              </div>
              <span className="text-ant-secondary text-xs">
                {t("androidPermissions.permissions.overlay.description")}
              </span>
            </div>
            <Button
              className={permissionActionClass}
              disabled={status.overlayGranted || busy}
              onClick={() => void handleRequest("overlay")}
              size="small"
              type={status.overlayGranted ? "default" : "primary"}
            >
              {status.overlayGranted
                ? t("androidPermissions.actions.ready")
                : t("androidPermissions.actions.openSettings")}
            </Button>
          </div>
        </section>

        {status.modeSelected && (
          <section className="rounded-xl border border-ant-border-secondary bg-ant-fill-quaternary p-3">
            <div className="mb-2 font-medium text-ant-text">
              {t("androidPermissions.sections.runtime")}
            </div>
            {status.mode === "full" ? (
              <>
                <RuntimeRow
                  label={t("androidPermissions.runtime.capture")}
                  running={status.clipboardMonitorRunning}
                  t={t}
                />
                <RuntimeRow
                  label={t("androidPermissions.runtime.gesture")}
                  running={status.gestureMonitorRunning}
                  t={t}
                />
              </>
            ) : (
              <RuntimeRow
                label={t("androidPermissions.runtime.foregroundCapture")}
                running={status.foregroundCaptureRunning}
                t={t}
              />
            )}
          </section>
        )}

        <section className="flex flex-col gap-2.5">
          <span className="font-medium text-ant-text">
            {t("androidPermissions.sections.recommended")}
          </span>

          <PermissionCard
            action={
              <Button
                disabled={status.batteryIgnored || busy}
                onClick={() => void handleRequest("battery")}
                size="small"
              >
                {status.batteryIgnored
                  ? t("androidPermissions.actions.ready")
                  : t("androidPermissions.actions.openSettings")}
              </Button>
            }
            description={t(
              "androidPermissions.permissions.battery.description",
            )}
            ready={status.batteryIgnored}
            title={t("androidPermissions.permissions.battery.title")}
          />

          <PermissionCard
            action={
              <Button
                disabled={status.notificationGranted || busy}
                onClick={() => void handleRequest("notification")}
                size="small"
              >
                {status.notificationGranted
                  ? t("androidPermissions.actions.ready")
                  : t("androidPermissions.actions.openSettings")}
              </Button>
            }
            description={t(
              "androidPermissions.permissions.notification.description",
            )}
            ready={status.notificationGranted}
            title={t("androidPermissions.permissions.notification.title")}
          />
        </section>
      </div>
    </Modal>
  );
};

interface ModeCardProps {
  description: string;
  icon: string;
  selected: boolean;
  title: string;
}

const ModeCard: FC<ModeCardProps> = (props) => {
  const { description, icon, selected, title } = props;
  const { t } = useTranslation("common");

  return (
    <div
      className={cn(
        "rounded-xl border p-3",
        selected
          ? "border-ant-primary bg-ant-primary-bg"
          : "border-ant-border-secondary bg-ant-container",
      )}
    >
      <div className="flex items-center gap-2">
        <i
          aria-hidden="true"
          className={cn(icon, "text-ant-primary text-lg")}
        />
        <span className="font-medium text-ant-text">{title}</span>
        {selected && (
          <Tag className="m-0" color="blue">
            {t("androidPermissions.status.current")}
          </Tag>
        )}
      </div>
      <p className="mt-2 mb-0 text-ant-secondary text-xs leading-relaxed">
        {description}
      </p>
    </div>
  );
};

interface PermissionCardProps {
  action: ReactNode;
  description: string;
  ready: boolean;
  title: string;
}

const PermissionCard: FC<PermissionCardProps> = (props) => {
  const { action, description, ready, title } = props;
  const { t } = useTranslation("common");

  return (
    <div className="flex flex-col items-stretch gap-2 rounded-xl border border-ant-border-secondary bg-ant-container p-3 shadow-xs sm:flex-row sm:items-center sm:justify-between">
      <div className="flex min-w-0 flex-1 flex-col gap-1">
        <div className="flex flex-wrap items-center gap-1.5 font-medium text-ant-text">
          <span>{title}</span>
          <Tag color={ready ? "success" : "default"}>
            {ready
              ? t("androidPermissions.status.enabled")
              : t("androidPermissions.status.recommended")}
          </Tag>
        </div>
        <span className="text-ant-secondary text-xs">{description}</span>
      </div>
      <div className="self-end">{action}</div>
    </div>
  );
};

interface RuntimeRowProps {
  label: string;
  running: boolean;
  t: TFunction<"common">;
}

const RuntimeRow: FC<RuntimeRowProps> = (props) => {
  const { label, running, t } = props;

  return (
    <div className="flex items-center justify-between gap-3 py-1">
      <span className="text-ant-secondary">{label}</span>
      <Tag color={running ? "success" : "default"}>
        {running
          ? t("androidPermissions.status.running")
          : t("androidPermissions.status.stopped")}
      </Tag>
    </div>
  );
};
