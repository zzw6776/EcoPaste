import { useMount } from "ahooks";
import { Spin } from "antd";
import { motion } from "motion/react";
import { type FC, useRef, useState } from "react";
import { useSnapshot } from "valtio";
import {
  type ClipboardPreviewState,
  getClipboardPreviewState,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useTauriListen } from "@/hooks/useTauriListen";
import { settingsState } from "@/stores/settings";
import { cn } from "@/utils/cn";
import { log } from "@/utils/log";
import { cacheKey } from "./cache";
import {
  PreviewContent,
  PreviewFooter,
  PreviewHeader,
} from "./components/PreviewContent";
import PreviewContentTransition from "./components/PreviewContentTransition";
import { PREVIEW_PANEL_TRANSITION, PREVIEW_PANEL_VARIANTS } from "./constants";
import { resolveConnector } from "./geometry";
import { usePreviewPayload, usePreviewRenderState } from "./hooks";
import {
  clamp,
  resolveDynamicPanelRect,
  resolveEffectivePanelSize,
  resolveMeasurePanelStyle,
} from "./layout";
import { hasMeasuredPanelSize, useMeasuredPanelSize } from "./measurement";
import { usePreviewMotion } from "./motion";

const EMPTY_RECT = {
  height: 1,
  left: 0,
  top: 0,
  width: 1,
};
const EMPTY_POINT = { x: 0, y: 0 };
const EMPTY_CONNECTOR = {
  control1: EMPTY_POINT,
  control2: EMPTY_POINT,
  path: "M 0 0 C 0 0 0 0 0 0",
  source: EMPTY_POINT,
  sourceDot: EMPTY_POINT,
  sourceSide: "right",
  target: EMPTY_POINT,
  targetDot: EMPTY_POINT,
  targetSide: "left",
} as const;

interface BeforeDestroyPayload {
  label: string;
}

/**
 * 系统级剪贴板预览窗口。
 * 预览窗口自身常驻透明 overlay，按 `itemId + updatedAt` 缓存最近内容并渲染基础 Content Viewer。
 */
const Preview: FC = () => {
  const [previewState, setPreviewState] =
    useState<ClipboardPreviewState | null>(null);
  const [payloadResetToken, setPayloadResetToken] = useState(0);
  const panelMeasureRef = useRef<HTMLDivElement>(null);
  const { clipboard } = useSnapshot(settingsState);
  const redactSecrets = clipboard.sensitive.redactSecrets;
  const renderState = usePreviewRenderState(previewState);
  const { loadingItemId, payload } = usePreviewPayload(
    previewState,
    payloadResetToken,
  );
  const measuredPanelSize = useMeasuredPanelSize(panelMeasureRef);
  const active = previewState !== null;
  const visibleState = previewState ?? renderState;
  const effectivePanelSize = visibleState
    ? resolveEffectivePanelSize(visibleState.layout, measuredPanelSize, payload)
    : measuredPanelSize;
  const panelRect = visibleState
    ? resolveDynamicPanelRect(visibleState.layout, effectivePanelSize)
    : EMPTY_RECT;
  const panelMeasureStyle = visibleState
    ? resolveMeasurePanelStyle(visibleState.layout)
    : void 0;
  const connector = visibleState
    ? resolveConnector(visibleState.layout.sourceRect, panelRect)
    : EMPTY_CONNECTOR;
  const motionLayout = usePreviewMotion(
    active,
    visibleState?.sessionId ?? null,
    hasMeasuredPanelSize(effectivePanelSize),
    panelRect,
    connector,
  );

  useMount(async () => {
    try {
      const state = await getClipboardPreviewState();
      setPreviewState(state);
    } catch (error) {
      log.error("load preview state failed", error);
    }
  });

  useTauriListen<ClipboardPreviewState | null>(
    TAURI_EVENT.PREVIEW_UPDATED,
    (event) => {
      setPreviewState(event.payload);
    },
  );

  const handleBeforeDestroy = (event: { payload: BeforeDestroyPayload }) => {
    if (event.payload.label !== WINDOW_LABEL.PREVIEW) return;

    setPreviewState(null);
    setPayloadResetToken((current) => {
      return current + 1;
    });
  };

  useTauriListen<BeforeDestroyPayload>(
    TAURI_EVENT.WINDOW_BEFORE_DESTROY,
    handleBeforeDestroy,
  );

  if (!visibleState) {
    return <div className="fixed inset-0 overflow-hidden bg-transparent" />;
  }

  const isLoading = loadingItemId !== null;
  const payloadKey = payload ? cacheKey(payload, redactSecrets) : "empty";
  const sourceRect = visibleState.layout.sourceRect;
  const arrowLeft =
    panelRect && panelRect.width > 0
      ? clamp(
          sourceRect.left + sourceRect.width / 2 - panelRect.left,
          24,
          panelRect.width - 24,
        )
      : "50%";

  return (
    <div className="fixed inset-0 overflow-hidden bg-transparent">
      <div
        aria-hidden="true"
        className="pointer-events-none invisible absolute top-0 left-0 z-0 flex w-fit min-w-[460px] max-w-[680px] flex-col overflow-visible rounded-[20px] bg-white dark:bg-[#1E1E1E]"
        ref={panelMeasureRef}
        style={panelMeasureStyle}
      >
        <PreviewHeader payload={payload} />

        {shouldRenderMeasuredContent(payload) && (
          <div className="min-h-0">
            <PreviewContent payload={payload} />
          </div>
        )}

        <PreviewFooter payload={payload} />
      </div>

      <motion.div
        animate={active ? "open" : "closed"}
        className="absolute z-10 flex max-h-[600px] min-h-[260px] min-w-[460px] max-w-[680px] flex-col overflow-visible rounded-[22px] border-0 bg-white shadow-[0_25px_65px_-10px_rgba(0,0,0,0.28),0_10px_25px_-5px_rgba(0,0,0,0.12),0_0_1px_rgba(0,0,0,0.18)] dark:bg-[#1E1E1E] dark:shadow-[0_25px_65px_-10px_rgba(0,0,0,0.65),0_0_1px_rgba(255,255,255,0.15)]"
        initial="closed"
        style={motionLayout.panelStyle}
        transition={PREVIEW_PANEL_TRANSITION}
        variants={PREVIEW_PANEL_VARIANTS}
      >
        <div className="relative flex size-full flex-col overflow-hidden rounded-[22px]">
          <PreviewContentTransition contentKey={payloadKey}>
            <PreviewHeader payload={payload} />

            <div
              className={cn(
                "min-h-0 flex-1 overflow-hidden transition-opacity",
                {
                  "opacity-60": isLoading && payload !== null,
                },
              )}
            >
              <PreviewContent payload={payload} />
            </div>

            <PreviewFooter payload={payload} />
          </PreviewContentTransition>

          {isLoading && (
            <div className="pointer-events-none absolute inset-0 flex items-center justify-center bg-black/10">
              <Spin size="small" />
            </div>
          )}
        </div>

        {/* 底部居中下指小箭头（精确指向当前选中卡片的水平中心点） */}
        <div
          className="pointer-events-none absolute -bottom-1.5 size-3.5 -translate-x-1/2 rotate-45 bg-white shadow-[2px_2px_4px_rgba(0,0,0,0.06)] dark:bg-[#1E1E1E]"
          style={{ left: arrowLeft }}
        />
      </motion.div>
    </div>
  );
};

function shouldRenderMeasuredContent(
  payload: ReturnType<typeof usePreviewPayload>["payload"],
) {
  if (!payload) return true;

  return payload.kind === "image";
}

export default Preview;
