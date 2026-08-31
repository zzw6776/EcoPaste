import { spawnSync } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { hashFile } from "./fileFingerprint";

export interface IconManifest {
  outputs: Record<string, string>;
  sources: Record<IconSource, string>;
  tauriVersion: string;
}

export interface IconOutput {
  generatedPath: string;
  source: IconSource;
  targetPath: string;
}

export type IconSource = "default" | "macos";

export const projectRoot = resolve(import.meta.dirname, "..");
export const manifestPath = resolve(
  projectRoot,
  "src-tauri/assets/icon-manifest.json",
);
export const iconSources: Record<IconSource, string> = {
  default: "src-tauri/assets/logo.png",
  macos: "src-tauri/assets/logo-mac.png",
};

const androidResourceRoot = "src-tauri/gen/android/app/src/main/res";
const androidDensities = ["hdpi", "mdpi", "xhdpi", "xxhdpi", "xxxhdpi"];
const androidFileNames = [
  "ic_launcher.png",
  "ic_launcher_foreground.png",
  "ic_launcher_round.png",
];

export const iconOutputs: IconOutput[] = [
  {
    generatedPath: "32x32.png",
    source: "default",
    targetPath: "src-tauri/icons/32x32.png",
  },
  {
    generatedPath: "128x128.png",
    source: "default",
    targetPath: "src-tauri/icons/128x128.png",
  },
  {
    generatedPath: "128x128@2x.png",
    source: "default",
    targetPath: "src-tauri/icons/128x128@2x.png",
  },
  {
    generatedPath: "icon.ico",
    source: "default",
    targetPath: "src-tauri/icons/icon.ico",
  },
  {
    generatedPath: "icon.icns",
    source: "macos",
    targetPath: "src-tauri/icons/icon.icns",
  },
  {
    generatedPath: "android/mipmap-anydpi-v26/ic_launcher.xml",
    source: "default",
    targetPath: `${androidResourceRoot}/mipmap-anydpi-v26/ic_launcher.xml`,
  },
  ...androidDensities.flatMap((density) => {
    return androidFileNames.map((fileName) => {
      return {
        generatedPath: `android/mipmap-${density}/${fileName}`,
        source: "default" as const,
        targetPath: `${androidResourceRoot}/mipmap-${density}/${fileName}`,
      };
    });
  }),
  {
    generatedPath: "android/values/ic_launcher_background.xml",
    source: "default",
    targetPath: `${androidResourceRoot}/values/ic_launcher_background.xml`,
  },
];

/** 返回当前 Tauri CLI 版本，确保生成器升级后必须刷新图标。 */
export function getTauriVersion(): string {
  const result = spawnSync("tauri", ["--version"], {
    cwd: projectRoot,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`tauri --version exited with ${result.status}`);
  }

  return result.stdout.trim();
}

/** 根据工作区中的源文件和目标文件创建只包含内容摘要的图标清单。 */
export async function createIconManifest(): Promise<IconManifest> {
  const sources = {} as Record<IconSource, string>;
  for (const [name, path] of Object.entries(iconSources) as [
    IconSource,
    string,
  ][]) {
    sources[name] = await hashFile(resolve(projectRoot, path));
  }

  const outputs: Record<string, string> = {};
  for (const output of iconOutputs) {
    outputs[output.targetPath] = await hashFile(
      resolve(projectRoot, output.targetPath),
    );
  }

  return {
    outputs,
    sources,
    tauriVersion: getTauriVersion(),
  };
}

/** 读取仓库中记录的图标生成清单。 */
export async function readIconManifest(): Promise<IconManifest> {
  return JSON.parse(await readFile(manifestPath, "utf8")) as IconManifest;
}

/** 更新仓库中的图标生成清单。 */
export async function writeIconManifest(manifest: IconManifest): Promise<void> {
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
}
