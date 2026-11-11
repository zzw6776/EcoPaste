import { onBackButtonPress } from "@tauri-apps/api/app";
import type { PluginListener } from "@tauri-apps/api/core";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useMount, useUnmount } from "ahooks";
import type { ConfigProviderProps } from "antd";
import { App as AntdApp, ConfigProvider } from "antd";
import enUS from "antd/locale/en_US";
import zhCN from "antd/locale/zh_CN";
import type { FC } from "react";
import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { RouterProvider } from "react-router";
import { useSnapshot } from "valtio";
import { minimizeAndroidApp } from "@/commands/android";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useAppTheme } from "@/hooks/useAppTheme";
import { closeAppOverlays } from "@/hooks/useCloseAppOverlay";
import { useTauriListen } from "@/hooks/useTauriListen";
import { router } from "./router";
import { settingsInitializationState, settingsState } from "./stores/settings";
import type { Language } from "./types/settings";
import { handleAndroidBack } from "./utils/androidBack";
import {
  closeModalDialogs,
  setMessageApi,
  setModalApi,
} from "./utils/feedback";
import { isAndroid, isTauri } from "./utils/is";
import { log } from "./utils/log";

const ANTD_DRAWER_CONFIG = isAndroid
  ? ({
      classNames: ({ props }) => {
        const placement = props.placement ?? "right";
        const reachesTop = placement !== "bottom";
        const reachesBottom = placement !== "top";

        return {
          body: reachesBottom ? "android-safe-drawer-body" : "",
          header: reachesTop ? "android-safe-drawer-header" : "",
        };
      },
    } satisfies ConfigProviderProps["drawer"])
  : void 0;

const ANTD_MODAL_CONFIG = {
  centered: true,
  classNames: isAndroid
    ? {
        body: "android-safe-modal-body",
        container: "android-safe-modal-container",
        wrapper: "android-safe-modal-wrapper",
      }
    : void 0,
} satisfies ConfigProviderProps["modal"];

const ANTD_MESSAGE_TOP = isAndroid
  ? "calc(var(--mobile-safe-area-top) + 1rem)"
  : 56;
const APP_OVERLAY_ROOT_ID = "app-overlay-root";

interface WindowVisibilityPayload {
  label: string;
  visible: boolean;
}

interface AppContentProps {
  windowLabel: string;
}

/** 将临时浮层限制在固定的应用视口内。 */
const getPopupContainer: NonNullable<
  ConfigProviderProps["getPopupContainer"]
> = () => {
  return document.getElementById(APP_OVERLAY_ROOT_ID) ?? document.body;
};

/**
 * 把设置语言映射到 Ant Design 内置 locale。
 */
const resolveAntdLocale = (language: Language) => {
  if (language === "en-US") return enUS;

  return zhCN;
};

import { AndroidPermissionsModal } from "@/components/AndroidPermissionsModal";
import SyncJoinApproval from "@/components/SyncJoinApproval";
import {
  androidState,
  checkAndAutoPromptAndroidPermissions,
  closeAndroidPermissionsModal,
} from "@/stores/android";

const AppContent: FC<AppContentProps> = (props) => {
  const { windowLabel } = props;
  const { message, modal } = AntdApp.useApp();
  const androidSnapshot = useSnapshot(androidState);
  const androidBackListenerRef = useRef<PluginListener | null>(null);
  const mountedRef = useRef(false);

  async function initializeAndroidBackListener() {
    try {
      const listener = await onBackButtonPress(({ canGoBack }) => {
        if (handleAndroidBack()) return;

        if (canGoBack) {
          window.history.back();
          return;
        }

        void minimizeAndroidApp();
      });
      if (!mountedRef.current) {
        await listener.unregister();
        return;
      }

      androidBackListenerRef.current = listener;
    } catch (error) {
      log.error("register Android back listener failed", error);
    }
  }

  useEffect(() => {
    setMessageApi(message);
    setModalApi(modal);
  }, [message, modal]);

  useMount(() => {
    mountedRef.current = true;
    if (!isAndroid) return;

    void initializeAndroidBackListener();
  });

  useUnmount(() => {
    mountedRef.current = false;
    const listener = androidBackListenerRef.current;
    androidBackListenerRef.current = null;
    if (!listener) return;

    void listener.unregister();
  });

  useTauriListen<WindowVisibilityPayload>(
    TAURI_EVENT.WINDOW_VISIBILITY,
    (event) => {
      if (
        windowLabel !== WINDOW_LABEL.CLIPBOARD ||
        event.payload.label !== WINDOW_LABEL.CLIPBOARD ||
        event.payload.visible
      ) {
        return;
      }

      closeAppOverlays();
      message.destroy();
      closeModalDialogs();
    },
  );

  return (
    <>
      <RouterProvider router={router} />
      <SyncJoinApproval />
      {isAndroid && (
        <AndroidPermissionsModal
          onClose={closeAndroidPermissionsModal}
          open={androidSnapshot.permissionsModalOpen}
        />
      )}
    </>
  );
};

const App: FC = () => {
  const { i18n } = useTranslation();
  const settings = useSnapshot(settingsState);
  const settingsInitialization = useSnapshot(settingsInitializationState);
  const windowLabel = isTauri
    ? getCurrentWebviewWindow().label
    : WINDOW_LABEL.CLIPBOARD;
  const mode =
    windowLabel === WINDOW_LABEL.ONBOARDING
      ? "dark"
      : settings.appearance.theme;
  const language = settings.appearance.language;
  const antdTheme = useAppTheme(mode);
  const locale = resolveAntdLocale(language);
  const androidInitializationRef = useRef(false);

  useEffect(() => {
    document.documentElement.lang = language;

    if (i18n.language === language) return;

    void i18n.changeLanguage(language);
  }, [i18n, language]);

  useEffect(() => {
    if (!isAndroid || settingsInitialization.status !== "ready") return;
    if (!settings.onboarding.completed) {
      void router.navigate("/onboarding", { replace: true });
      return;
    }
    if (androidInitializationRef.current) return;

    androidInitializationRef.current = true;
    void checkAndAutoPromptAndroidPermissions();
  }, [settings.onboarding.completed, settingsInitialization.status]);

  return (
    <ConfigProvider
      drawer={ANTD_DRAWER_CONFIG}
      getPopupContainer={getPopupContainer}
      locale={locale}
      modal={ANTD_MODAL_CONFIG}
      theme={antdTheme}
    >
      <AntdApp message={{ duration: 1.5, maxCount: 2, top: ANTD_MESSAGE_TOP }}>
        <div id={APP_OVERLAY_ROOT_ID} />
        {settingsInitialization.status === "loading" ? (
          <div
            aria-label="EcoPaste 正在启动"
            className="app-startup-shell flex size-full min-h-screen items-center justify-center"
            role="status"
          >
            <div className="size-8 animate-pulse rounded-2 bg-ant-primary" />
          </div>
        ) : (
          <AppContent windowLabel={windowLabel} />
        )}
      </AntdApp>
    </ConfigProvider>
  );
};

export default App;
