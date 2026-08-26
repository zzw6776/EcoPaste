import { useMount, useUnmount } from "ahooks";
import { Alert, Spin, Typography } from "antd";
import type QrScanner from "qr-scanner";
import type { FC, Ref } from "react";
import { useImperativeHandle, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { isSyncPairingCode } from "@/constants/sync";
import { isAndroid } from "@/utils/is";
import { log } from "@/utils/log";

interface SyncQrScannerProps {
  onDetected: (pairingCode: string) => void;
  ref?: Ref<SyncQrScannerHandle>;
}

export interface SyncQrScannerHandle {
  stop: () => void;
}

type QrScannerRuntimeCompatibility = {
  _disableBarcodeDetector: boolean;
};

/** 扫描完整相机画面，避免近距离二维码超出库默认的中央 2/3 裁剪区域。 */
function calculateScanRegion(video: HTMLVideoElement): QrScanner.ScanRegion {
  const width = video.videoWidth;
  const height = video.videoHeight;
  const scale = Math.min(1, 800 / Math.max(width, height));

  return {
    downScaledHeight: Math.max(1, Math.round(height * scale)),
    downScaledWidth: Math.max(1, Math.round(width * scale)),
    height,
    width,
    x: 0,
    y: 0,
  };
}

/** Android WebView 偶尔会拒绝 play() Promise，但视频流已经正常播放。 */
function isVideoRendering(video: HTMLVideoElement): boolean {
  const stream = video.srcObject;
  if (!(stream instanceof MediaStream)) return false;

  const hasLiveVideoTrack = stream
    .getVideoTracks()
    .some((track) => track.readyState === "live");

  return (
    hasLiveVideoTrack &&
    !video.paused &&
    video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA
  );
}

/** 显式释放视频轨道，避免 Android WebView 在扫码弹窗关闭后继续占用相机。 */
function releaseVideoStream(video: HTMLVideoElement | null): void {
  if (!video) return;

  const stream = video.srcObject;
  if (!(stream instanceof MediaStream)) return;

  for (const track of stream.getTracks()) {
    track.stop();
  }
  video.srcObject = null;
}

const SyncQrScanner: FC<SyncQrScannerProps> = (props) => {
  const { onDetected, ref } = props;
  const { t } = useTranslation("preferences");
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const scannerRef = useRef<QrScanner | null>(null);
  const detectedRef = useRef(false);
  const [starting, setStarting] = useState(true);
  const [errorMessage, setErrorMessage] = useState("");

  function stopScanner() {
    releaseVideoStream(videoRef.current);
    scannerRef.current?.destroy();
    scannerRef.current = null;
  }

  useImperativeHandle(ref, () => ({ stop: stopScanner }));

  function handleDecoded(result: QrScanner.ScanResult) {
    if (detectedRef.current) return;

    const pairingCode = result.data.trim();
    if (!isSyncPairingCode(pairingCode)) {
      setErrorMessage(t("sync.scanner.invalidCode"));
      return;
    }

    detectedRef.current = true;
    stopScanner();
    onDetected(pairingCode);
  }

  async function initializeScanner() {
    try {
      const video = videoRef.current;
      if (!video) return;

      const { default: QrScannerRuntime } = await import("qr-scanner");
      if (isAndroid) {
        // Android BarcodeDetector 无法解析当前高版本配对二维码，固定使用库内 Worker。
        (
          QrScannerRuntime as unknown as QrScannerRuntimeCompatibility
        )._disableBarcodeDetector = true;
      }
      if (!(await QrScannerRuntime.hasCamera())) {
        setErrorMessage(t("sync.scanner.noCamera"));
        return;
      }

      const scanner = new QrScannerRuntime(video, handleDecoded, {
        calculateScanRegion,
        highlightCodeOutline: true,
        highlightScanRegion: true,
        maxScansPerSecond: 10,
        preferredCamera: "environment",
        returnDetailedScanResult: true,
      });
      scannerRef.current = scanner;
      await scanner.start();
    } catch (error) {
      const video = videoRef.current;
      if (video && isVideoRendering(video)) {
        log.warn(
          "ignore sync QR scanner start rejection because camera is rendering",
          error,
        );
        return;
      }

      log.error("start sync QR scanner failed", error);
      const detail = String(error).toLowerCase();
      setErrorMessage(
        detail.includes("permission") || detail.includes("notallowed")
          ? t("sync.scanner.permissionDenied")
          : t("sync.scanner.unavailable"),
      );
    } finally {
      setStarting(false);
    }
  }

  useMount(() => {
    void initializeScanner();
  });

  useUnmount(() => {
    stopScanner();
  });

  return (
    <div className="flex flex-col gap-3">
      <div className="relative flex min-h-60 items-center justify-center overflow-hidden rounded-3 bg-black">
        <video
          aria-label={t("sync.scanner.videoLabel")}
          className="block max-h-96 w-full object-cover"
          muted
          playsInline
          ref={videoRef}
        />
        {starting ? (
          <Spin
            className="absolute"
            size="large"
            tip={t("sync.scanner.starting")}
          />
        ) : null}
      </div>
      <Typography.Text className="text-center text-xs" type="secondary">
        {t("sync.scanner.hint")}
      </Typography.Text>
      {errorMessage ? (
        <Alert message={errorMessage} showIcon type="warning" />
      ) : null}
    </div>
  );
};

export default SyncQrScanner;
