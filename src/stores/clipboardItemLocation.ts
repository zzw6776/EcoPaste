import { proxy } from "valtio";
import type { ClipboardCategory, ClipboardRange } from "@/types/clipboard";

export interface ClipboardItemLocationRequest {
  category: ClipboardCategory | null;
  groupId: string | null;
  itemId: string;
  range: ClipboardRange;
  requestId: number;
}

interface ClipboardItemLocationState {
  request: ClipboardItemLocationRequest | null;
}

export const clipboardItemLocationState = proxy<ClipboardItemLocationState>({
  request: null,
});

let nextRequestId = 0;

/** 发布一次列表定位请求；目标视图用于防止移动端非活动分页误消费。 */
export function requestClipboardItemLocation(
  request: Omit<ClipboardItemLocationRequest, "requestId">,
) {
  nextRequestId += 1;
  clipboardItemLocationState.request = {
    ...request,
    requestId: nextRequestId,
  };
}

export function consumeClipboardItemLocation(requestId: number) {
  if (clipboardItemLocationState.request?.requestId !== requestId) return;

  clipboardItemLocationState.request = null;
}
