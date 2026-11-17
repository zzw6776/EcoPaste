import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { buildUpdaterManifest } from "./createUpdaterManifest.mjs";

const version = "1.2.3";
const bundles = [
  "EcoPaste_1.2.3_aarch64.app.tar.gz",
  "EcoPaste_1.2.3_x64.app.tar.gz",
  "EcoPaste_1.2.3_x64-setup.exe",
  "EcoPaste_1.2.3_arm64.apk",
  "EcoPaste_1.2.3_arm.apk",
  "EcoPaste_1.2.3_x86.apk",
  "EcoPaste_1.2.3_x86_64.apk",
];
const assets = bundles.map((name, index) => {
  return {
    browser_download_url: `https://github.com/example/EcoPaste/releases/download/untagged-draft/${name}`,
    digest: name.endsWith(".apk") ? `sha256:${"a".repeat(64)}` : null,
    name,
    url: `https://api.github.com/repos/example/EcoPaste/releases/assets/${index + 1}`,
  };
});
const signatures = Object.fromEntries(
  bundles.map((name, index) => {
    return [`${name}.sig`, `signature-${index + 1}`];
  }),
);

test("builds one updater manifest for every supported target", () => {
  const publishedAt = new Date("2026-10-20T12:00:00.000Z");
  const manifest = buildUpdaterManifest(version, assets, signatures, {
    publishedAt,
    releaseDownloadBaseUrl:
      "https://github.com/example/EcoPaste/releases/download/v1.2.3",
  });

  assert.equal(manifest.version, version);
  assert.equal(manifest.pub_date, publishedAt.toISOString());
  assert.deepEqual(Object.keys(manifest.platforms), [
    "darwin-aarch64",
    "darwin-aarch64-app",
    "darwin-x86_64",
    "darwin-x86_64-app",
    "windows-x86_64",
    "windows-x86_64-nsis",
    "android-aarch64",
    "android-armv7",
    "android-i686",
    "android-x86_64",
  ]);
  assert.equal(manifest.platforms["darwin-aarch64"].signature, "signature-1");
  assert.equal(manifest.platforms["windows-x86_64"].url, assets[2].url);
  assert.equal(
    manifest.platforms["android-aarch64"].url,
    "https://github.com/example/EcoPaste/releases/download/v1.2.3/EcoPaste_1.2.3_arm64.apk",
  );
  assert.equal(manifest.platforms["android-aarch64"].signature, "");
  assert.equal(manifest.platforms["android-aarch64"].sha256, "a".repeat(64));
});

test("rejects incomplete required updater assets", () => {
  const assetsWithoutWindows = assets.filter(
    (asset) => asset.name !== "EcoPaste_1.2.3_x64-setup.exe",
  );
  assert.throws(
    () =>
      buildUpdaterManifest(version, assetsWithoutWindows, signatures, {
        releaseDownloadBaseUrl:
          "https://github.com/example/EcoPaste/releases/download/v1.2.3",
      }),
    /Missing updater asset: EcoPaste_1\.2\.3_x64-setup\.exe/,
  );
});

test("allows missing macOS updater assets", () => {
  const assetsWithoutMac = assets.filter(
    (asset) => !asset.name.includes(".app.tar.gz"),
  );
  const manifest = buildUpdaterManifest(version, assetsWithoutMac, signatures, {
    publishedAt: new Date("2026-10-20T12:00:00.000Z"),
    releaseDownloadBaseUrl:
      "https://github.com/example/EcoPaste/releases/download/v1.2.3",
  });
  assert.equal(manifest.platforms["darwin-aarch64"], undefined);
  assert.equal(manifest.platforms["darwin-x86_64"], undefined);
  assert.ok(manifest.platforms["windows-x86_64"]);
  assert.ok(manifest.platforms["android-aarch64"]);
});

test("rejects Android updater assets without a SHA-256 digest", () => {
  const assetsWithoutAndroidDigest = assets.map((asset) => {
    if (asset.name !== "EcoPaste_1.2.3_arm64.apk") return asset;

    return { ...asset, digest: null };
  });

  assert.throws(
    () =>
      buildUpdaterManifest(version, assetsWithoutAndroidDigest, signatures, {
        releaseDownloadBaseUrl:
          "https://github.com/example/EcoPaste/releases/download/v1.2.3",
      }),
    /Missing APK SHA-256 digest: EcoPaste_1\.2\.3_arm64\.apk/,
  );
});

test("builds desktop targets in parallel and publishes one updater manifest", async () => {
  const workflow = await readFile(
    new URL("../.github/workflows/release.yml", import.meta.url),
    "utf8",
  );

  assert.doesNotMatch(workflow, /max-parallel:\s*1/);
  assert.match(
    workflow,
    /label: macOS Apple Silicon\n\s+os: macos-latest[\s\S]*?target: aarch64-apple-darwin/,
  );
  assert.match(
    workflow,
    /label: macOS Intel\n\s+os: macos-26-intel[\s\S]*?target: x86_64-apple-darwin/,
  );
  assert.match(
    workflow,
    /name: Build Windows x64[\s\S]*?runs-on: windows-latest/,
  );
  assert.match(workflow, /uploadUpdaterJson: false/);
  assert.match(workflow, /GITHUB_SERVER_URL: \$\{\{ github\.server_url }}/);
  assert.equal(
    workflow.match(/node scripts\/createUpdaterManifest\.mjs/g)?.length,
    1,
  );
});
