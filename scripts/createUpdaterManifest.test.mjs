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

test("rejects incomplete updater assets", () => {
  assert.throws(
    () =>
      buildUpdaterManifest(version, assets.slice(1), signatures, {
        releaseDownloadBaseUrl:
          "https://github.com/example/EcoPaste/releases/download/v1.2.3",
      }),
    /Missing updater asset: EcoPaste_1\.2\.3_aarch64\.app\.tar\.gz/,
  );
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

test("publishes the first parallel Apple Silicon candidate", async () => {
  const workflow = await readFile(
    new URL("../.github/workflows/release.yml", import.meta.url),
    "utf8",
  );
  const candidateWorkflow = await readFile(
    new URL("../.github/workflows/macos-apple-candidates.yml", import.meta.url),
    "utf8",
  );

  assert.doesNotMatch(workflow, /max-parallel:\s*1/);
  assert.match(
    workflow,
    /gh workflow run macos-apple-candidates\.yml[\s\S]*?-f "tag_name=\$\{TAG_NAME\}"/,
  );
  assert.match(
    workflow,
    /CANDIDATE_RUN_ID: \$\{\{ needs\.start-macos-apple-candidates\.outputs\.run_id }}/,
  );
  assert.ok(
    workflow.indexOf("artifact_name='macos-aarch64-native'") <
      workflow.indexOf("artifact_name='macos-aarch64-cross'"),
  );
  assert.match(workflow, /Neither Apple Silicon build route produced/);
  assert.match(workflow, /actions\/runs\/\$\{CANDIDATE_RUN_ID\}\/cancel/);
  const candidateStartJob = workflow.slice(
    workflow.indexOf("  start-macos-apple-candidates:"),
    workflow.indexOf("  upload-macos-apple:"),
  );
  assert.match(candidateStartJob, /GH_REPO: \$\{\{ github\.repository }}/);
  const appleUploadJob = workflow.slice(
    workflow.indexOf("  upload-macos-apple:"),
    workflow.indexOf("  build-android:"),
  );
  assert.match(appleUploadJob, /- build-desktop/);
  assert.match(appleUploadJob, /- start-macos-apple-candidates/);
  assert.equal(
    appleUploadJob.match(/GH_REPO: \$\{\{ github\.repository }}/g)?.length,
    2,
  );
  assert.match(candidateWorkflow, /continue-on-error: true/);
  assert.match(candidateWorkflow, /fail-fast: false/);
  assert.match(
    candidateWorkflow,
    /artifact_name: macos-aarch64-native[\s\S]*?runner: macos-15/,
  );
  assert.match(
    candidateWorkflow,
    /artifact_name: macos-aarch64-cross[\s\S]*?runner: macos-26-intel/,
  );
  assert.match(candidateWorkflow, /targets: aarch64-apple-darwin/);
  assert.match(
    candidateWorkflow,
    /run: bash scripts\/trustMacosReleaseCertificate\.sh/,
  );
  assert.match(workflow, /EcoPaste_\$\{RELEASE_VERSION}_aarch64\.app\.tar\.gz/);
  assert.match(
    workflow,
    /label: macOS Intel\n\s+os: macos-26-intel[\s\S]*?target: x86_64-apple-darwin/,
  );
  assert.match(workflow, /uploadUpdaterJson: false/);
  assert.match(workflow, /GITHUB_SERVER_URL: \$\{\{ github\.server_url }}/);
  assert.equal(
    workflow.match(/node scripts\/createUpdaterManifest\.mjs/g)?.length,
    1,
  );
});
