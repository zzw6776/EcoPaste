import assert from "node:assert/strict";
import {
  access,
  copyFile,
  mkdir,
  mkdtemp,
  rm,
  truncate,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test, { type TestContext } from "node:test";
import { acquireAndroidBuildLock } from "./androidBuildLock";
import { cleanDesktopCacheIfNeeded } from "./buildCache";

const sparseFileTest = {
  skip:
    process.platform === "win32"
      ? "The oversized cache fixture requires sparse-file allocation."
      : false,
};

/** 使用独立的空 Rust 工程验证清理边界，不触碰真实项目构建缓存。 */
async function createCacheFixture(context: TestContext, oversized: boolean) {
  const root = await mkdtemp(resolve(tmpdir(), "ecopaste-build-cache-"));
  context.after(async () => {
    await rm(root, { force: true, recursive: true });
  });

  await mkdir(resolve(root, "src-tauri/src"), { recursive: true });
  await writeFile(
    resolve(root, "src-tauri/Cargo.toml"),
    '[package]\nname = "build_cache_probe"\nversion = "0.0.0"\nedition = "2021"\n',
  );
  await writeFile(resolve(root, "src-tauri/src/lib.rs"), "");
  await copyFile(
    resolve(import.meta.dirname, "../rust-toolchain.toml"),
    resolve(root, "rust-toolchain.toml"),
  );

  const hostMacro = resolve(root, "src-tauri/target/release/deps/macro-marker");
  const androidArtifact = resolve(
    root,
    "src-tauri/target/aarch64-linux-android/release/deps/android-marker",
  );
  const bundle = resolve(root, "src-tauri/target/release/bundle/bundle-marker");
  for (const path of [hostMacro, androidArtifact, bundle]) {
    await mkdir(resolve(path, ".."), { recursive: true });
    await writeFile(path, "keep");
  }
  await writeFile(
    resolve(root, "src-tauri/target/CACHEDIR.TAG"),
    "Signature: 8a477f597d28d172789f06886806bc55\n",
  );
  if (oversized) {
    const cache = resolve(root, "src-tauri/target/debug/cache-marker");
    await mkdir(resolve(cache, ".."), { recursive: true });
    await writeFile(cache, "");
    await truncate(cache, 7 * 1024 ** 3);
  }

  return { androidArtifact, bundle, hostMacro, root };
}

test(
  "desktop cleanup preserves host macros while Android owns the lock",
  sparseFileTest,
  async (context) => {
    const fixture = await createCacheFixture(context, true);
    const lock = await acquireAndroidBuildLock(fixture.root, "release build");
    try {
      await assert.rejects(
        cleanDesktopCacheIfNeeded(fixture.root),
        /Another Android release build is running/,
      );
      await access(fixture.hostMacro);
      await access(fixture.androidArtifact);
      await access(fixture.bundle);
    } finally {
      await lock.release();
    }
  },
);

test(
  "idle desktop cleanup preserves the bundle and Android target, then releases the lock",
  sparseFileTest,
  async (context) => {
    const fixture = await createCacheFixture(context, true);
    await cleanDesktopCacheIfNeeded(fixture.root);

    await assert.rejects(access(fixture.hostMacro), { code: "ENOENT" });
    await access(fixture.androidArtifact);
    await access(fixture.bundle);
    const lock = await acquireAndroidBuildLock(fixture.root, "release build");
    await lock.release();
  },
);

test("desktop startup below the threshold leaves an active Android build alone", async (context) => {
  const fixture = await createCacheFixture(context, false);
  const lock = await acquireAndroidBuildLock(fixture.root, "release build");
  try {
    await cleanDesktopCacheIfNeeded(fixture.root);
    await access(fixture.hostMacro);
    await access(fixture.androidArtifact);
  } finally {
    await lock.release();
  }
});
