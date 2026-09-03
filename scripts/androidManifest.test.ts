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
