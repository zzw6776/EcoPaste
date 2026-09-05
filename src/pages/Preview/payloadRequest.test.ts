import assert from "node:assert/strict";
import test from "node:test";
import type { ClipboardPreviewPayload } from "@/commands";
import { PreviewPayloadRequestPool } from "./payloadRequest";

const PAYLOAD: ClipboardPreviewPayload = {
  files: [],
  id: "item-1",
  imageExists: false,
  imageHeight: null,
  imagePath: null,
  imageWidth: null,
  isSensitive: false,
  kind: "text",
  size: 4,
  subKind: null,
  text: "test",
  totalFiles: 0,
  updatedAt: "2026-09-06T00:00:00Z",
};

test("deduplicates concurrent requests for the same content variant", async () => {
  const pool = new PreviewPayloadRequestPool();
  let loadCount = 0;
  let release!: (payload: ClipboardPreviewPayload) => void;
  const pending = new Promise<ClipboardPreviewPayload>((resolve) => {
    release = resolve;
  });
  const loader = async () => {
    loadCount += 1;
    return await pending;
  };

  const first = pool.request(PAYLOAD.id, false, loader);
  const second = pool.request(PAYLOAD.id, false, loader);

  assert.equal(loadCount, 1);
  assert.equal(first, second);
  release(PAYLOAD);
  assert.equal(await first, PAYLOAD);
  assert.equal(await second, PAYLOAD);
});

test("keeps redacted and full requests independent", async () => {
  const pool = new PreviewPayloadRequestPool();
  let loadCount = 0;
  const loader = async () => {
    loadCount += 1;
    return PAYLOAD;
  };

  await Promise.all([
    pool.request(PAYLOAD.id, false, loader),
    pool.request(PAYLOAD.id, true, loader),
  ]);

  assert.equal(loadCount, 2);
});

test("releases completed and failed requests", async () => {
  const pool = new PreviewPayloadRequestPool();
  let loadCount = 0;
  const loader = async () => {
    loadCount += 1;
    if (loadCount === 1) throw new Error("load failed");

    return PAYLOAD;
  };

  await assert.rejects(pool.request(PAYLOAD.id, false, loader), /load failed/);
  assert.equal(await pool.request(PAYLOAD.id, false, loader), PAYLOAD);
  assert.equal(loadCount, 2);
});
