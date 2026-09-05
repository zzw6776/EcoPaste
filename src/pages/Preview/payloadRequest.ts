import type { ClipboardPreviewPayload } from "@/commands";

type PreviewPayloadLoader = (
  itemId: string,
) => Promise<ClipboardPreviewPayload | null>;

interface PreviewPayloadRequestEntry {
  promise: Promise<ClipboardPreviewPayload | null>;
}

/**
 * 合并相同条目与脱敏模式下的并发 payload 请求；请求结束后立即释放，不承担结果缓存职责。
 */
export class PreviewPayloadRequestPool {
  private readonly inFlight = new Map<string, PreviewPayloadRequestEntry>();

  request(
    itemId: string,
    redactSecrets: boolean,
    loader: PreviewPayloadLoader,
  ): Promise<ClipboardPreviewPayload | null> {
    const key = requestKey(itemId, redactSecrets);
    const existing = this.inFlight.get(key);

    if (existing) return existing.promise;

    const entry: PreviewPayloadRequestEntry = {
      promise: Promise.resolve(null),
    };
    this.inFlight.set(key, entry);
    entry.promise = this.loadAndRelease(key, entry, itemId, loader);

    return entry.promise;
  }

  clear() {
    this.inFlight.clear();
  }

  private async loadAndRelease(
    key: string,
    entry: PreviewPayloadRequestEntry,
    itemId: string,
    loader: PreviewPayloadLoader,
  ) {
    try {
      return await loader(itemId);
    } finally {
      if (this.inFlight.get(key) === entry) {
        this.inFlight.delete(key);
      }
    }
  }
}

function requestKey(itemId: string, redactSecrets: boolean) {
  return `${itemId}:${redactSecrets ? "redacted" : "full"}`;
}
