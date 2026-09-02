import assert from "node:assert/strict";
import test from "node:test";
import { getFileCollectionMeta } from "./fileCollectionMeta";

test("distinguishes files, folders, and mixed selections", () => {
  assert.deepEqual(getFileCollectionMeta("/a.txt\n/b.txt\n/c.txt", "f,f,f"), {
    count: 3,
    kind: "files",
  });
  assert.deepEqual(getFileCollectionMeta("/a\n/b", "d,d"), {
    count: 2,
    kind: "folders",
  });
  assert.deepEqual(getFileCollectionMeta("/a\n/b.txt", "d,f"), {
    count: 2,
    kind: "items",
  });
});

test("does not mislabel incomplete legacy metadata as files", () => {
  assert.deepEqual(getFileCollectionMeta("/a\n/b", null), {
    count: 2,
    kind: "items",
  });
  assert.deepEqual(getFileCollectionMeta("/a\n/b", "f"), {
    count: 2,
    kind: "items",
  });
});
