import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { normalizeTauriFileAssociationWhitespace } from "./androidManifest";

const MARKER =
  "            <!-- tauri-file-associations. AUTO-GENERATED. DO NOT REMOVE. -->";

test("normalizes only whitespace-only lines inside the Tauri block", () => {
  const manifest = [
    "<manifest>  ",
    MARKER,
    "            <intent-filter />",
    "            ",
    MARKER,
    "</manifest>  ",
  ].join("\n");

  assert.equal(
    normalizeTauriFileAssociationWhitespace(manifest),
    [
      "<manifest>  ",
      MARKER,
      "            <intent-filter />",
      "",
      MARKER,
      "</manifest>  ",
    ].join("\n"),
  );
});

test("leaves manifests without one complete generated block unchanged", () => {
  const manifest = ["<manifest>", MARKER, "            ", "</manifest>"].join(
    "\n",
  );

  assert.equal(normalizeTauriFileAssociationWhitespace(manifest), manifest);
});

test("declares the Root overlay service without an Accessibility service", () => {
  const manifest = readFileSync(
    "src-tauri/gen/android/app/src/main/AndroidManifest.xml",
    "utf8",
  );

  assert.match(manifest, /android:name="\.EcoPasteOverlayService"/);
  assert.doesNotMatch(manifest, /BIND_ACCESSIBILITY_SERVICE/);
  assert.doesNotMatch(manifest, /EcoPasteAccessibilityService/);
});

test("accepts single and multiple shares of every MIME type outside the generated association block", () => {
  const manifest = readFileSync(
    "src-tauri/gen/android/app/src/main/AndroidManifest.xml",
    "utf8",
  );
  const manualManifest = manifest.split(MARKER)[0];
  const shareFilter = manualManifest.match(
    /<intent-filter>\s*<action android:name="android.intent.action.SEND"[\s\S]*?<\/intent-filter>/,
  )?.[0];

  assert.ok(shareFilter);
  assert.match(shareFilter, /android.intent.action.SEND_MULTIPLE/);
  assert.match(shareFilter, /android.intent.category.DEFAULT/);
  assert.match(shareFilter, /android:mimeType="\*\/\*"/);
  assert.doesNotMatch(shareFilter, /android.intent.action.VIEW/);
});
