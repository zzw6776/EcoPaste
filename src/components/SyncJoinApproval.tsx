import { listen } from "@tauri-apps/api/event";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useMount, useUnmount } from "ahooks";
import type { FC } from "react";
import { useRef } from "react";
import { useTranslation } from "react-i18next";
import {
  type IncomingJoinRequest,
  listIncomingSyncJoinRequests,
  respondIncomingSyncJoinRequest,
  showWindow,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { getModalApi } from "@/utils/feedback";
import { isTauri } from "@/utils/is";
import { log } from "@/utils/log";

/** Presents encrypted LAN join requests even when the sync settings page is closed. */
const SyncJoinApproval: FC = () => {
  const { t } = useTranslation("preferences");
  const activeRef = useRef(new Set<string>());
  const unlistenRef = useRef<null | (() => void)>(null);
  const timersRef = useRef(new Set<ReturnType<typeof setTimeout>>());

  async function showRequest(request: IncomingJoinRequest) {
    if (activeRef.current.has(request.requestId)) return;

    const remaining = new Date(request.expiresAt).getTime() - Date.now();
    if (remaining <= 0) return;

    activeRef.current.add(request.requestId);
    if (isTauri) {
      try {
        await showWindow(WINDOW_LABEL.CLIPBOARD);
      } catch (error) {
        log.error("show join approval window failed", error);
      }
    }
    const dialog = getModalApi().confirm({
      cancelText: t("sync.nearby.reject"),
      centered: true,
      content: (
        <div className="flex flex-col gap-3">
          <p className="m-0 text-ant-secondary text-sm">
            {t("sync.nearby.approvalDescription", {
              device: request.deviceName,
            })}
          </p>
          <div className="rounded-2 bg-ant-fill-quaternary p-3 text-center">
            <div className="text-ant-secondary text-xs">
              {t("sync.nearby.comparisonCode")}
            </div>
            <div className="mt-1 font-mono font-semibold text-2xl tracking-widest">
              {request.comparisonCode}
            </div>
          </div>
          {request.previouslyRemoved ? (
            <p className="m-0 text-ant-warning text-xs">
              {t("sync.nearby.removedDeviceWarning")}
            </p>
          ) : null}
        </div>
      ),
      okText: t("sync.nearby.approve"),
      onCancel: async () => {
        activeRef.current.delete(request.requestId);
        await respondIncomingSyncJoinRequest(request.requestId, false);
      },
      onOk: async () => {
        await respondIncomingSyncJoinRequest(request.requestId, true);
        activeRef.current.delete(request.requestId);
      },
      title: t("sync.nearby.approvalTitle", { device: request.deviceName }),
    });
    const timer = setTimeout(() => {
      dialog.destroy();
      activeRef.current.delete(request.requestId);
      timersRef.current.delete(timer);
    }, remaining);
    timersRef.current.add(timer);
  }

  async function initialize() {
    if (isTauri && getCurrentWebviewWindow().label !== WINDOW_LABEL.CLIPBOARD) {
      return;
    }
    try {
      const requests = await listIncomingSyncJoinRequests();
      for (const request of requests) await showRequest(request);
      unlistenRef.current = await listen<IncomingJoinRequest>(
        TAURI_EVENT.SYNC_JOIN_REQUESTED,
        (event) => {
          void showRequest(event.payload);
        },
      );
    } catch (error) {
      log.error("initialize LAN join approval failed", error);
    }
  }

  useMount(() => {
    void initialize();
  });

  useUnmount(() => {
    unlistenRef.current?.();
    for (const timer of timersRef.current) clearTimeout(timer);
  });

  return null;
};

export default SyncJoinApproval;
