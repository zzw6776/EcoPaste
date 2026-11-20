import assert from "node:assert/strict";
import test from "node:test";
import type { SyncPeerStatus } from "@/commands";
import {
  createSyncStatusRefresh,
  resolveUsagePercent,
  resolveUsageTone,
  shortenEndpointId,
  sortLanPeersForDisplay,
  summarizeLanPeers,
} from "./SyncStatusIcons.logic";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}

function makePeer(
  deviceId: string,
  deviceName: string,
  state: SyncPeerStatus["state"],
): SyncPeerStatus {
  return {
    connectedAddress: null,
    deviceId,
    deviceName,
    directAddresses: [],
    endpointId: deviceId,
    lastAttemptAt: null,
    lastError: null,
    lastSeenAt: null,
    lastSuccessAt: null,
    platform: "macos",
    relayUrls: [],
    state,
    transport: null,
  };
}

test("coalesces an event burst and reads the latest status once more", async () => {
  const reads = [deferred<number>(), deferred<number>()];
  const written: number[] = [];
  let readCount = 0;
  const refresh = createSyncStatusRefresh({
    isActive: () => {
      return true;
    },
    onError: (error) => {
      throw error;
    },
    read: () => {
      return reads[readCount++].promise;
    },
    write: (status) => {
      written.push(status);
    },
  });

  const first = refresh();
  assert.ok(first);
  await Promise.resolve();
  assert.equal(readCount, 1);

  const pending = refresh();
  assert.ok(pending);
  for (let index = 0; index < 100; index += 1) {
    assert.equal(refresh(), pending);
  }

  reads[0].resolve(1);
  await first;
  assert.equal(readCount, 2);
  assert.deepEqual(written, [1]);

  reads[1].resolve(2);
  await pending;
  assert.deepEqual(written, [1, 2]);
});

test("skips hidden updates and refreshes once after becoming active", async () => {
  const reads = [deferred<number>(), deferred<number>()];
  const written: number[] = [];
  let active = false;
  let readCount = 0;
  const refresh = createSyncStatusRefresh({
    isActive: () => {
      return active;
    },
    onError: (error) => {
      throw error;
    },
    read: () => {
      return reads[readCount++].promise;
    },
    write: (status) => {
      written.push(status);
    },
  });

  for (let index = 0; index < 100; index += 1) {
    assert.equal(refresh(), void 0);
  }
  assert.equal(readCount, 0);

  active = true;
  const visible = refresh();
  assert.ok(visible);
  await Promise.resolve();
  assert.equal(readCount, 1);

  active = false;
  reads[0].resolve(1);
  await visible;
  assert.deepEqual(written, []);

  active = true;
  const reopened = refresh();
  assert.ok(reopened);
  await Promise.resolve();
  assert.equal(readCount, 2);
  reads[1].resolve(2);
  await reopened;
  assert.deepEqual(written, [2]);
});

test("computes bounded quota usage percentages and alert tones", () => {
  assert.equal(resolveUsagePercent(50, 0), null);
  assert.equal(resolveUsagePercent(0, 1000), 0);
  assert.equal(resolveUsagePercent(744, 1000), 74);
  assert.equal(resolveUsagePercent(1500, 1000), 100);

  assert.equal(resolveUsageTone(null), "normal");
  assert.equal(resolveUsageTone(74), "normal");
  assert.equal(resolveUsageTone(75), "warning");
  assert.equal(resolveUsageTone(89), "warning");
  assert.equal(resolveUsageTone(90), "error");
});

test("sorts and summarizes LAN peers with online devices first", () => {
  const peers = [
    makePeer("3", "Windows", "idle"),
    makePeer("1", "Android", "error"),
    makePeer("2", "MacBook", "online"),
    makePeer("4", "iMac", "online"),
  ];

  const sorted = sortLanPeersForDisplay(peers);
  assert.deepEqual(
    sorted.map((peer) => {
      return peer.deviceName;
    }),
    ["iMac", "MacBook", "Android", "Windows"],
  );
  assert.deepEqual(summarizeLanPeers(peers), {
    connectingCount: 0,
    errorCount: 1,
    onlineCount: 2,
    totalCount: 4,
  });
});

test("shortens long endpoint identifiers cleanly", () => {
  assert.equal(shortenEndpointId("short-id"), "short-id");
  assert.equal(
    shortenEndpointId("abcdefghijklmnopqrstuvwxyz123456"),
    "abcdef…123456",
  );
});
