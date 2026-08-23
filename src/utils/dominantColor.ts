import { convertFileSrc } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import { isTauri } from "@/utils/is";

const DEFAULT_GRADIENT = "linear-gradient(135deg, #2B8FF7 0%, #1B80F4 100%)";
const colorCache = new Map<string, string>();

/**
 * 将本地文件路径转化为 webview 可加载的 asset URL
 */
export function toAssetUrl(filePath?: string | null): string {
  if (!filePath) return "";
  if (
    filePath.startsWith("http://") ||
    filePath.startsWith("https://") ||
    filePath.startsWith("data:") ||
    filePath.startsWith("asset:")
  ) {
    return filePath;
  }

  if (isTauri) {
    try {
      return convertFileSrc(filePath);
    } catch {
      return filePath;
    }
  }

  return filePath;
}

/**
 * RGB 转 HSL
 */
function rgbToHsl(r: number, g: number, b: number): [number, number, number] {
  const rNorm = r / 255;
  const gNorm = g / 255;
  const bNorm = b / 255;

  const max = Math.max(rNorm, gNorm, bNorm);
  const min = Math.min(rNorm, gNorm, bNorm);
  let h = 0;
  let s = 0;
  const l = (max + min) / 2;

  if (max !== min) {
    const d = max - min;
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
    switch (max) {
      case rNorm:
        h = (gNorm - bNorm) / d + (gNorm < bNorm ? 6 : 0);
        break;
      case gNorm:
        h = (bNorm - rNorm) / d + 2;
        break;
      case bNorm:
        h = (rNorm - gNorm) / d + 4;
        break;
    }
    h /= 6;
  }

  return [Math.round(h * 360), Math.round(s * 100), Math.round(l * 100)];
}

/**
 * HSL 转 Hex
 */
function hslToHex(h: number, s: number, l: number): string {
  const lNorm = l / 100;
  const a = (s * Math.min(lNorm, 1 - lNorm)) / 100;
  const f = (n: number) => {
    const k = (n + h / 30) % 12;
    const color = lNorm - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
    return Math.round(255 * color)
      .toString(16)
      .padStart(2, "0");
  };
  return `#${f(0)}${f(8)}${f(4)}`;
}

/**
 * 将提取到的 RGB 色彩进行苹果质感调和规范化
 */
function harmonizeColor(r: number, g: number, b: number): string {
  let [h, s, _l] = rgbToHsl(r, g, b);

  // 饱和度锁定在 80%~95%，确保颜色饱满有活力
  s = Math.max(78, Math.min(95, s));

  // 黄色区间 (38~65 deg) 自动向温暖琥珀橙金微调，避免暗黄色变泥巴色
  if (h >= 38 && h <= 65) {
    h = 36;
  }

  // 提升明度至 58%，确保通透轻盈、明亮清爽的 Apple 质感
  const l = 58;
  const mainHex = hslToHex(h, s, l);
  const endHex = hslToHex(h, s, Math.max(48, l - 7));

  return `linear-gradient(135deg, ${mainHex} 0%, ${endHex} 100%)`;
}

/**
 * 100% 纯算法从图标像素中动态提取主色调
 * - 彩色图标：提取主导色相并调和为鲜亮渐变；
 * - 黑白/单色图标（如 ChatGPT、GitHub、终端）：自动识别并生成深邃黑曜石/高级暗夜渐变。
 */
export function extractDominantGradient(rawSrc: string): Promise<string> {
  if (!rawSrc) return Promise.resolve(DEFAULT_GRADIENT);
  const imageSrc = toAssetUrl(rawSrc);

  const cached = colorCache.get(rawSrc);
  if (cached) {
    return Promise.resolve(cached);
  }

  return new Promise((resolve) => {
    const img = new Image();
    img.crossOrigin = "anonymous";

    img.onload = () => {
      try {
        const canvas = document.createElement("canvas");
        const ctx = canvas.getContext("2d", { willReadFrequently: true });
        if (!ctx) {
          resolve(DEFAULT_GRADIENT);
          return;
        }

        const size = 32;
        canvas.width = size;
        canvas.height = size;
        ctx.drawImage(img, 0, 0, size, size);

        const data = ctx.getImageData(0, 0, size, size).data;

        // 12 个色相区间桶
        const buckets = Array.from({ length: 12 }, () => {
          return { b: 0, count: 0, g: 0, r: 0, totalWeight: 0 };
        });

        let totalColoredPixels = 0;
        let totalDarkNeutralPixels = 0;
        let totalSampledPixels = 0;

        for (let i = 0; i < data.length; i += 4) {
          const r = data[i];
          const g = data[i + 1];
          const b = data[i + 2];
          const a = data[i + 3];

          if (a < 100) continue; // 忽略透明像素
          totalSampledPixels++;

          const max = Math.max(r, g, b);
          const min = Math.min(r, g, b);
          const delta = max - min;
          const brightness = (r * 299 + g * 587 + b * 114) / 1000;

          // 判断是否为黑白/深色单色系
          if (delta < 22) {
            if (brightness < 120) {
              totalDarkNeutralPixels++;
            }
            continue;
          }

          // 彩色像素
          const [h, s] = rgbToHsl(r, g, b);
          if (s < 20) continue;

          totalColoredPixels++;
          const bucketIndex = Math.floor(h / 30) % 12;
          const weight = (s / 100) * 2 + delta / 255;

          const bucket = buckets[bucketIndex];
          bucket.count++;
          bucket.r += r * weight;
          bucket.g += g * weight;
          bucket.b += b * weight;
          bucket.totalWeight += weight;
        }

        // 1. 如果彩色像素极少，而深色/黑白像素占主导（如 ChatGPT、终端、GitHub）
        if (
          totalColoredPixels < 8 &&
          totalDarkNeutralPixels > totalSampledPixels * 0.1
        ) {
          const darkGradient =
            "linear-gradient(135deg, #1E293B 0%, #0F172A 100%)";
          colorCache.set(rawSrc, darkGradient);
          resolve(darkGradient);
          return;
        }

        // 2. 找出权重最大的彩色桶
        let bestBucket = buckets[0];
        for (const bucket of buckets) {
          if (bucket.totalWeight > bestBucket.totalWeight) {
            bestBucket = bucket;
          }
        }

        let gradient = DEFAULT_GRADIENT;
        if (bestBucket.totalWeight > 0) {
          const avgR = Math.round(bestBucket.r / bestBucket.totalWeight);
          const avgG = Math.round(bestBucket.g / bestBucket.totalWeight);
          const avgB = Math.round(bestBucket.b / bestBucket.totalWeight);
          gradient = harmonizeColor(avgR, avgG, avgB);
        }

        colorCache.set(rawSrc, gradient);
        resolve(gradient);
      } catch {
        resolve(DEFAULT_GRADIENT);
      }
    };

    img.onerror = () => {
      resolve(DEFAULT_GRADIENT);
    };

    img.src = imageSrc;
  });
}

/**
 * React Hook：根据图标 URL 100% 纯算法动态提取主色调渐变
 */
export function useDynamicHeaderBg(iconSrc?: string | null): string {
  const [bg, setBg] = useState<string>(() => {
    if (iconSrc) {
      const cached = colorCache.get(iconSrc);
      if (cached) return cached;
    }
    return DEFAULT_GRADIENT;
  });

  useEffect(() => {
    if (!iconSrc) {
      setBg(DEFAULT_GRADIENT);
      return;
    }

    const cached = colorCache.get(iconSrc);
    if (cached) {
      setBg(cached);
      return;
    }

    let isMounted = true;
    void extractDominantGradient(iconSrc).then((gradient) => {
      if (!isMounted) return;
      setBg(gradient);
    });

    return () => {
      isMounted = false;
    };
  }, [iconSrc]);

  return bg;
}
