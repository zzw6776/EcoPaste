import assert from "node:assert/strict";
import { access, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test, { type TestContext } from "node:test";
import { acquireAndroidBuildLock } from "./androidBuildLock";
import { cleanAllOutputs, cleanAllOutputsWithReport } from "./cleanOutputs";

/** 创建独立临时工程验证全量输出物清理范围与保留边界。 */
async function createOutputsFixture(context: TestContext) {
  const root = await mkdtemp(resolve(tmpdir(), "ecopaste-clean-outputs-"));
  context.after(async () => {
    await rm(root, { force: true, recursive: true });
  });

  const filesToRemove = [
    "src-tauri/target/debug/deps/libeco_paste_lib.rlib",
    "src-tauri/target/aarch64-linux-android/release/lib.so",
    "src-tauri/gen/android/.gradle/cache.bin",
    "src-tauri/gen/android/app/build/outputs/app.apk",
    "src-tauri/gen/schemas/acl.json",
    "sync-server/target/debug/server",
    "src-tauri/vendor/swarm-discovery/target/debug/lib.rlib",
    "dist/index.html",
    "node_modules/.vite/deps/chunk.js",
    "artifacts/desktop/EcoPaste.dmg",
    "apk/EcoPaste-release-unsigned.apk",
  ];
  const filesToKeep = [
    "src-tauri/src/lib.rs",
    "src-tauri/gen/android/app/src/main/MainActivity.kt",
    "node_modules/react/index.js",
    "signing.properties",
  ];

  for (const relativePath of [...filesToRemove, ...filesToKeep]) {
    const fullPath = resolve(root, relativePath);
    await mkdir(resolve(fullPath, ".."), { recursive: true });
    await writeFile(fullPath, "fixture");
  }

  return { filesToKeep, filesToRemove, root };
}

test("cleanAllOutputs removes reproducible build outputs and preserves dependencies and signing files", async (context) => {
  const fixture = await createOutputsFixture(context);
  const report = await cleanAllOutputsWithReport(fixture.root);
  const cleaned = report.cleanedPaths;

  assert.ok(cleaned.includes("src-tauri/target"));
  assert.ok(cleaned.includes("artifacts"));
  assert.ok(cleaned.includes("dist"));
  assert.ok(report.availableBeforeBytes > 0);
  assert.ok(report.availableAfterBytes > 0);
  assert.ok(report.freedDiskBytes >= 0);

  for (const relativePath of fixture.filesToRemove) {
    await assert.rejects(access(resolve(fixture.root, relativePath)), {
      code: "ENOENT",
    });
  }
  await assert.rejects(access(resolve(fixture.root, "src-tauri/target")), {
    code: "ENOENT",
  });

  for (const relativePath of fixture.filesToKeep) {
    await access(resolve(fixture.root, relativePath));
  }
});

test("cleanAllOutputs aborts when an Android build owns the lock", async (context) => {
  const fixture = await createOutputsFixture(context);
  const lock = await acquireAndroidBuildLock(fixture.root, "release build");
  try {
    await assert.rejects(
      cleanAllOutputs(fixture.root),
      /Another Android release build is running/,
    );
    await access(
      resolve(
        fixture.root,
        "src-tauri/target/debug/deps/libeco_paste_lib.rlib",
      ),
    );
  } finally {
    await lock.release();
  }
});
