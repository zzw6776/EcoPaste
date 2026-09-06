/** 合并尚未开始的读取；执行期间的新请求统一留给下一次读取，避免返回过期快照。 */
export function createCoalescedRequest<T>(read: () => Promise<T>) {
  type Batch = {
    promise: Promise<T>;
    resolve: (value: T) => void;
    reject: (reason: unknown) => void;
  };
  let pending: Batch | null = null;
  let running = false;

  async function flush() {
    while (pending) {
      const batch = pending;
      pending = null;
      try {
        batch.resolve(await read());
      } catch (error) {
        batch.reject(error);
      }
    }
    running = false;
  }

  return function request(): Promise<T> {
    if (!pending) {
      let resolve!: (value: T) => void;
      let reject!: (reason: unknown) => void;
      const promise = new Promise<T>((onResolve, onReject) => {
        resolve = onResolve;
        reject = onReject;
      });
      pending = { promise, reject, resolve };
    }
    if (!running) {
      running = true;
      queueMicrotask(() => {
        void flush();
      });
    }
    return pending.promise;
  };
}
