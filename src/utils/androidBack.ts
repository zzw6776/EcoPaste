type AndroidBackHandler = () => void;

export type AndroidBackScope = "layer" | "page";

interface AndroidBackEntry {
  handler: AndroidBackHandler;
  scope: AndroidBackScope;
  token: symbol;
}

const handlerStack: AndroidBackEntry[] = [];

/** 注册一个 Android 返回处理器；后注册的浮层优先关闭。 */
export function registerAndroidBackHandler(
  handler: AndroidBackHandler,
  scope: AndroidBackScope = "layer",
) {
  const entry = { handler, scope, token: Symbol("android-back-handler") };
  handlerStack.push(entry);

  return () => {
    const index = handlerStack.findIndex((current) => {
      return current.token === entry.token;
    });
    if (index >= 0) handlerStack.splice(index, 1);
  };
}

/** 返回指定作用域中最后注册的处理器。 */
function findLastHandler(scope: AndroidBackScope) {
  for (let index = handlerStack.length - 1; index >= 0; index -= 1) {
    const entry = handlerStack[index];
    if (entry?.scope === scope) return entry;
  }

  return null;
}

/**
 * 按“交互浮层 → 页面”顺序处理 Android 返回。
 * 页面处理器不会因为挂载较晚而越过仍打开的交互浮层。
 */
export function handleAndroidBack(): boolean {
  const layerEntry = findLastHandler("layer");
  if (layerEntry) {
    layerEntry.handler();
    return true;
  }

  const pageEntry = findLastHandler("page");
  if (!pageEntry) return false;

  pageEntry.handler();

  return true;
}
