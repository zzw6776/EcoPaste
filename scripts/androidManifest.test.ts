import assert from "node:assert/strict";
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
