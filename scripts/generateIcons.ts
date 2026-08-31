import { spawnSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import {
  createIconManifest,
  iconOutputs,
  iconSources,
  projectRoot,
  readIconManifest,
  writeIconManifest,
} from "./iconAssets";

/** 在隔离目录中生成一套 Tauri 图标，避免 CLI 改写 Android 工程。 */
function generateSource(source: string, output: string): void {
  const result = spawnSync("tauri", ["icon", source, "--output", output], {
    cwd: projectRoot,
    shell: process.platform === "win32",
    stdio: "inherit",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`tauri icon exited with ${result.status}`);
  }
}

const sourceNames = Object.keys(iconSources) as (keyof typeof iconSources)[];
const sourcesToGenerate = new Set(sourceNames);
try {
  const expected = await readIconManifest();
  const current = await createIconManifest();

  if (expected.tauriVersion === current.tauriVersion) {
    sourcesToGenerate.clear();
    for (const name of sourceNames) {
      if (expected.sources[name] !== current.sources[name]) {
        sourcesToGenerate.add(name);
      }
    }
    for (const output of iconOutputs) {
      if (
        expected.outputs[output.targetPath] !==
        current.outputs[output.targetPath]
      ) {
        sourcesToGenerate.add(output.source);
      }
    }
  }
} catch {
  // 清单或任一输出缺失时重建全部平台资源。
}

if (sourcesToGenerate.size === 0) {
  process.stdout.write("Icon assets are already up to date.\n");
} else {
  const temporaryRoot = await mkdtemp(resolve(tmpdir(), "ecopaste-icons-"));
  try {
    for (const name of sourcesToGenerate) {
      generateSource(iconSources[name], resolve(temporaryRoot, name));
    }

    for (const output of iconOutputs) {
      if (!sourcesToGenerate.has(output.source)) {
        continue;
      }

      const sourcePath = resolve(
        temporaryRoot,
        output.source,
        output.generatedPath,
      );
      const targetPath = resolve(projectRoot, output.targetPath);

      await mkdir(dirname(targetPath), { recursive: true });
      await copyFile(sourcePath, targetPath);
    }

    await writeIconManifest(await createIconManifest());
    process.stdout.write("Icon assets generated and manifest updated.\n");
  } finally {
    await rm(temporaryRoot, { force: true, recursive: true });
  }
}
