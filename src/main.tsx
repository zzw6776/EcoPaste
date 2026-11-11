import { Suspense } from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./i18n";
import { log } from "./utils/log";

import "overlayscrollbars/overlayscrollbars.css";
import "virtual:uno.css";
import "./styles/global.scss";

/** React 挂载前安装兜底日志，确保启动阶段失败也能留下证据。 */
function installGlobalErrorLogging() {
  window.addEventListener("unhandledrejection", (event) => {
    const { reason } = event;

    log.error(
      "unhandled promise rejection",
      reason instanceof Error ? reason : { reason },
    );
  });

  window.addEventListener("error", (event) => {
    const { error, ...rest } = event;

    log.error("uncaught error", error instanceof Error ? error : rest);
  });
}

const StartupFallback = () => {
  return (
    <div
      aria-label="EcoPaste 正在启动"
      className="app-startup-shell flex size-full min-h-screen items-center justify-center"
      role="status"
    >
      <div className="size-8 animate-pulse rounded-2 bg-ant-primary" />
    </div>
  );
};

installGlobalErrorLogging();

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <Suspense fallback={<StartupFallback />}>
    <App />
  </Suspense>,
);
