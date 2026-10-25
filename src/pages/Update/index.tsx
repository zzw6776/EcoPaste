import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useMount } from "ahooks";
import { Button, Progress } from "antd";
import type { FC } from "react";
import { useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type AppUpdateStatus,
  checkForUpdates,
  downloadUpdate,
  getUpdateStatus,
  hideWindow,
  installUpdate,
  openExternalUrl,
  skipUpdateVersion,
  type UpdateDownloadProgress,
  type UpdateMetadata,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { UPDATE_RELEASES_URL } from "@/constants/urls";
import { WINDOW_LABEL } from "@/constants/windows";
import { useTauriListen } from "@/hooks/useTauriListen";
import { useWindowReady } from "@/hooks/useWindowReady";
import { router } from "@/router";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";

type UpdateViewState =
  | "idle"
  | "checking"
  | "available"
  | "latest"
  | "downloading"
  | "downloaded"
  | "error";

type UpdateErrorPhase = "checking" | "updating";

interface WindowVisibilityPayload {
  label: string;
  visible: boolean;
}

const PROGRESS_INDETERMINATE_PERCENT = 82;

/**
 * 独立软件更新窗口。Rust updater 负责检查、签名校验和安装，React 只渲染状态。
 */
const Update: FC = () => {
  useWindowReady();
  const { t } = useTranslation(["update", "common"]);
  const [state, setState] = useState<UpdateViewState>("idle");
  const [currentVersion, setCurrentVersion] = useState("");
  const [update, setUpdate] = useState<UpdateMetadata | null>(null);
  const [progress, setProgress] = useState<UpdateDownloadProgress>({
    downloaded: 0,
    progress: null,
    total: null,
  });
  const operationRef = useRef(false);
  const [errorPhase, setErrorPhase] = useState<UpdateErrorPhase>("checking");
  const [errorMessage, setErrorMessage] = useState("");
  const statusText = resolveStatusText(t, state, errorPhase);
  const description = resolveDescription(
    t,
    state,
    currentVersion,
    update,
    errorPhase,
  );
  const progressPercent = resolveProgressPercent(state, progress.progress);
  const showReleaseNotesLink =
    update !== null && (state === "available" || state === "downloaded");
  const showProgress = state === "checking" || state === "downloading";

  const progressLabel = useMemo(() => {
    if (state !== "downloading") return "";
    if (!progress.total) return "";

    return `${formatBytes(progress.downloaded)} / ${formatBytes(progress.total)}`;
  }, [progress.downloaded, progress.total, state]);

  const checkUpdates = async () => {
    if (operationRef.current) return;

    operationRef.current = true;
    setUpdateState("checking");
    setErrorPhase("checking");
    setErrorMessage("");
    setUpdate(null);
    setProgress({ downloaded: 0, progress: null, total: null });

    try {
      const status = await checkForUpdates();
      applyUpdateStatus(status);
    } catch (error) {
      log.warn("check for updates failed", error);
      setErrorPhase("checking");
      setUpdateState("error");
      setErrorMessage(resolveUnknownError(error));
    } finally {
      operationRef.current = false;
    }
  };

  /**
   * 重新展示时恢复 Rust 中的更新状态，避免清空已下载包或重复启动下载。
   */
  const restoreUpdateStatus = async () => {
    if (operationRef.current) return;

    operationRef.current = true;

    try {
      const status = await getUpdateStatus();
      setCurrentVersion(status.currentVersion);

      if (status.update) {
        applyUpdateStatus(status);
        return;
      }
    } catch (error) {
      log.warn("restore update status failed", error);
      setErrorPhase("checking");
      setUpdateState("error");
      setErrorMessage(resolveUnknownError(error));
      return;
    } finally {
      operationRef.current = false;
    }

    await checkUpdates();
  };

  const downloadCurrentUpdate = async () => {
    if (!update || operationRef.current) return;

    operationRef.current = true;
    setUpdateState("downloading");
    setErrorPhase("updating");
    setErrorMessage("");
    setProgress({ downloaded: 0, progress: 0, total: null });

    try {
      const nextUpdate = await downloadUpdate(update.version);
      setUpdate(nextUpdate);
      setUpdateState("downloaded");
      setProgress({
        downloaded: progress.total ?? progress.downloaded,
        progress: 1,
        total: progress.total,
      });
    } catch (error) {
      log.warn("download update failed", error);
      setErrorPhase("updating");
      setUpdateState("error");
      setErrorMessage(resolveUnknownError(error));
    } finally {
      operationRef.current = false;
    }
  };

  const installCurrentUpdate = async () => {
    if (!update || operationRef.current) return;

    operationRef.current = true;
    try {
      await installUpdate(update.version);
    } catch (error) {
      log.warn("install update failed", error);
      setErrorPhase("updating");
      setUpdateState("error");
      setErrorMessage(resolveUnknownError(error));
    } finally {
      operationRef.current = false;
    }
  };

  const skipCurrentVersion = async () => {
    if (!update || operationRef.current) return;

    operationRef.current = true;

    try {
      const status = await skipUpdateVersion(update.version);
      setUpdate(status.update);
      setUpdateState("latest");
    } catch (error) {
      log.warn("skip update failed", error);
      setUpdateState("error");
      setErrorMessage(resolveUnknownError(error));
    } finally {
      operationRef.current = false;
    }
  };

  const closeWindow = async () => {
    if (isAndroid) {
      await router.navigate(-1);
      return;
    }

    await hideWindow(getCurrentWebviewWindow().label);
  };

  const openReleaseNotes = async () => {
    if (!update) return;

    try {
      await openExternalUrl(resolveReleaseNotesUrl(update.version));
    } catch (error) {
      log.warn("open update release notes failed", error);
    }
  };

  const setUpdateState = (nextState: UpdateViewState) => {
    setState(nextState);
  };

  /**
   * 同步 Rust 更新状态，确保缓存下载完成后仍显示安装入口。
   */
  const applyUpdateStatus = (status: AppUpdateStatus) => {
    setCurrentVersion(status.currentVersion);
    setUpdate(status.update);

    if (!status.update) {
      setUpdateState("latest");
      return;
    }

    if (status.update.downloaded) {
      setUpdateState("downloaded");
      return;
    }

    setUpdateState(isAndroid ? "downloading" : "available");
  };

  useMount(async () => {
    await restoreUpdateStatus();
  });

  useTauriListen<WindowVisibilityPayload>(
    TAURI_EVENT.WINDOW_VISIBILITY,
    (event) => {
      if (event.payload.label !== WINDOW_LABEL.UPDATE) return;
      if (!event.payload.visible) return;

      void restoreUpdateStatus();
    },
  );

  useTauriListen<UpdateDownloadProgress>(
    TAURI_EVENT.UPDATE_PROGRESS,
    (event) => {
      setProgress(event.payload);
    },
  );

  useTauriListen<AppUpdateStatus>(TAURI_EVENT.UPDATE_STATUS, (event) => {
    applyUpdateStatus(event.payload);
  });

  useTauriListen<string>(TAURI_EVENT.UPDATE_ERROR, (event) => {
    setErrorPhase("updating");
    setUpdateState("error");
    setErrorMessage(event.payload);
  });

  return (
    <div className="relative flex h-screen bg-ant-container text-ant-text">
      <div className="absolute inset-x-0 top-0 h-10" data-tauri-drag-region />
      <main className="flex min-h-0 flex-1 flex-col px-7 pt-12 pb-5">
        <div className="grid min-h-0 grid-cols-[4.5rem_minmax(0,1fr)] gap-5">
          <div className="flex justify-center">
            <img
              alt=""
              className="size-14 shrink-0 rounded-3 object-contain shadow-sm"
              src="/logo.png"
            />
          </div>

          <div className="min-w-0">
            <h1 className="m-0 font-semibold text-ant-text text-base leading-snug">
              {statusText}
            </h1>
            <p className="mt-2 mb-0 text-ant-secondary text-sm leading-5">
              {description}
            </p>

            {showReleaseNotesLink ? (
              <div className="mt-1 flex items-center gap-3">
                <Button
                  className="h-auto p-0 text-sm"
                  onClick={openReleaseNotes}
                  type="link"
                >
                  {t("update:actions.releaseNotes")}
                </Button>
                <Button
                  className="h-auto p-0 text-sm"
                  onClick={checkUpdates}
                  type="link"
                >
                  {t("update:actions.checkAgain")}
                </Button>
              </div>
            ) : null}

            {state === "error" && errorMessage ? (
              <p className="mt-2 mb-0 text-ant-error text-sm leading-5">
                {errorMessage}
              </p>
            ) : null}

            {showProgress ? (
              <div className="mt-4">
                <Progress
                  percent={progressPercent}
                  showInfo={false}
                  size={["100%", 10]}
                  status={state === "checking" ? "active" : "normal"}
                />
                {progressLabel ? (
                  <p className="mt-2 mb-0 text-ant-secondary text-xs">
                    {progressLabel}
                  </p>
                ) : null}
              </div>
            ) : null}
          </div>
        </div>

        <footer className="mt-auto flex shrink-0 items-center gap-3 pt-4">
          {state === "available" ? (
            <Button onClick={skipCurrentVersion} shape="round">
              {t("update:actions.skip")}
            </Button>
          ) : null}

          <span className="mr-auto" />

          {state === "available" ? (
            <Button onClick={closeWindow} shape="round">
              {t("update:actions.later")}
            </Button>
          ) : null}

          {state === "checking" || state === "downloading" ? (
            <Button onClick={closeWindow} shape="round">
              {t("update:actions.cancel")}
            </Button>
          ) : null}

          {state === "idle" || state === "latest" || state === "error" ? (
            <Button onClick={checkUpdates} shape="round">
              {t("update:actions.checkAgain")}
            </Button>
          ) : null}

          {state === "latest" || state === "error" ? (
            <Button onClick={closeWindow} shape="round" type="primary">
              {t("update:actions.ok")}
            </Button>
          ) : null}

          {state === "available" ? (
            <Button
              onClick={downloadCurrentUpdate}
              shape="round"
              type="primary"
            >
              {t("update:actions.download")}
            </Button>
          ) : null}

          {state === "downloaded" ? (
            <Button onClick={installCurrentUpdate} shape="round" type="primary">
              {t(
                isAndroid
                  ? "update:actions.openInstaller"
                  : "update:actions.install",
              )}
            </Button>
          ) : null}
        </footer>
      </main>
    </div>
  );
};

function resolveStatusText(
  t: ReturnType<typeof useTranslation<["update", "common"]>>["t"],
  state: UpdateViewState,
  errorPhase: UpdateErrorPhase,
) {
  if (state === "error") {
    return t(
      errorPhase === "checking"
        ? "update:status.checkError"
        : "update:status.updateError",
    );
  }

  return t(`update:status.${state}`);
}

function resolveDescription(
  t: ReturnType<typeof useTranslation<["update", "common"]>>["t"],
  state: UpdateViewState,
  currentVersion: string,
  update: UpdateMetadata | null,
  errorPhase: UpdateErrorPhase,
) {
  if (state === "available" && update) {
    return t("update:description.available", {
      currentVersion,
      version: update.version,
    });
  }

  if (state === "checking") return t("update:description.checking");
  if (state === "downloading") {
    return t("update:description.downloading", {
      version: update?.version ?? "",
    });
  }
  if (state === "downloaded") {
    return t(
      isAndroid
        ? "update:description.downloadedAndroid"
        : "update:description.downloaded",
    );
  }
  if (state === "error") {
    return t(
      errorPhase === "checking"
        ? "update:description.checkError"
        : "update:description.updateError",
    );
  }
  if (state === "latest") {
    return t("update:description.latest", { currentVersion });
  }

  return t("update:description.idle");
}

function resolveProgressPercent(
  state: UpdateViewState,
  progress: number | null,
) {
  if (state === "checking") return PROGRESS_INDETERMINATE_PERCENT;
  if (state !== "downloading") return 0;
  if (progress === null) return 12;

  return Math.round(progress * 100);
}

function resolveReleaseNotesUrl(version: string) {
  const tag = version.startsWith("v") ? version : `v${version}`;

  return `${UPDATE_RELEASES_URL}/tag/${tag}`;
}

function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;

  const kb = value / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;

  return `${(kb / 1024).toFixed(1)} MB`;
}

function resolveUnknownError(error: unknown) {
  if (
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
  ) {
    return error.message;
  }

  return String(error);
}

export default Update;
