import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { buildUpdaterManifest } from "./createUpdaterManifest.mjs";

const version = "1.2.3";
const bundles = [
  "EcoPaste_1.2.3_aarch64.app.tar.gz",
  "EcoPaste_1.2.3_x64.app.tar.gz",
  "EcoPaste_1.2.3_x64-setup.exe",
];
const assets = bundles.map((name, index) => {
  return {
    name,
    url: `https://api.github.com/repos/example/EcoPaste/releases/assets/${index + 1}`,
  };
});
const signatures = Object.fromEntries(
  bundles.map((name, index) => {
    return [`${name}.sig`, `signature-${index + 1}`];
  }),
);

test("builds one updater manifest for every desktop target", () => {
  const publishedAt = new Date("2026-10-20T12:00:00.000Z");
  const manifest = buildUpdaterManifest(
    version,
    assets,
    signatures,
    publishedAt,
  );

  assert.equal(manifest.version, version);
  assert.equal(manifest.pub_date, publishedAt.toISOString());
  assert.deepEqual(Object.keys(manifest.platforms), [
    "darwin-aarch64",
    "darwin-aarch64-app",
    "darwin-x86_64",
    "darwin-x86_64-app",
    "windows-x86_64",
    "windows-x86_64-nsis",
  ]);
  assert.equal(manifest.platforms["darwin-aarch64"].signature, "signature-1");
  assert.equal(manifest.platforms["windows-x86_64"].url, assets[2].url);
});

test("rejects incomplete updater assets", () => {
  assert.throws(
    () => buildUpdaterManifest(version, assets.slice(1), signatures),
    /Missing updater asset: EcoPaste_1\.2\.3_aarch64\.app\.tar\.gz/,
  );
});

test("builds desktop targets in parallel and publishes one updater manifest", async () => {
  const workflow = await readFile(
    new URL("../.github/workflows/release.yml", import.meta.url),
    "utf8",
  );

  assert.doesNotMatch(workflow, /max-parallel:\s*1/);
  assert.equal(workflow.match(/target: aarch64-apple-darwin/g)?.length, 2);
  assert.match(
    workflow,
    /name: macos-aarch64-\$\{\{ matrix\.apple_candidate }}/,
  );
  assert.match(workflow, /name: Upload macOS Apple Silicon/);
  assert.match(workflow, /name: desktop-ready-\$\{\{ matrix\.target }}/);
  assert.match(
    workflow,
    /name: Wait for the first complete desktop release set/,
  );
  assert.match(
    workflow,
    /actions\/runs\/\$\{GITHUB_RUN_ID\}\/artifacts\?per_page=100/,
  );
  assert.match(
    workflow,
    /name: \$\{\{ steps\.release_set\.outputs\.candidate }}/,
  );
  const appleUploadJob = workflow.slice(
    workflow.indexOf("  upload-macos-apple:"),
    workflow.indexOf("  build-android:"),
  );
  assert.doesNotMatch(appleUploadJob, /build-desktop/);
  assert.match(workflow, /EcoPaste_\$\{RELEASE_VERSION}_aarch64\.app\.tar\.gz/);
  assert.match(
    workflow,
    /label: macOS Intel\n\s+os: macos-26-intel[\s\S]*?target: x86_64-apple-darwin/,
  );
  assert.match(workflow, /uploadUpdaterJson: false/);
  assert.equal(
    workflow.match(/node scripts\/createUpdaterManifest\.mjs/g)?.length,
    1,
  );
});
