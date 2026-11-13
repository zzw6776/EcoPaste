import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import UnoCSS from "unocss/vite";
import { defineConfig } from "vite";

const host = process.env.TAURI_DEV_HOST;

// https://vite.dev/config/
export default defineConfig(async () => ({
  build: {
    rollupOptions: {
      output: {
        manualChunks: splitSharedRuntime,
      },
    },
  },
  // Vite options tailored for Tauri development and only applied in `tauri dev` or `tauri build`
  //
  // 1. prevent Vite from obscuring rust errors
  clearScreen: false,

  plugins: [UnoCSS(), react()],

  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  // 2. tauri expects a fixed port, fail if that port is not available
  server: {
    hmr: host
      ? {
          host,
          port: 1421,
          protocol: "ws",
        }
      : undefined,
    host: host || false,
    port: 1420,
    strictPort: true,
    watch: {
      // 3. tell Vite to ignore watching `src-tauri`
      ignored: ["**/src-tauri/**"],
    },
  },
}));

/** 将体积较大的公共运行库独立缓存，控制页面入口脚本的体积。 */
function splitSharedRuntime(id: string) {
  const marker = "/node_modules/";
  const index = id.lastIndexOf(marker);
  if (index < 0) return void 0;

  const packagePath = id.slice(index + marker.length);
  if (/^(react|react-dom|scheduler)\//.test(packagePath)) return "vendor-react";
  if (packagePath.startsWith("react-router/")) return "vendor-router";
  if (/^(i18next|react-i18next)\//.test(packagePath)) return "vendor-i18n";
  if (packagePath.startsWith("@tauri-apps/api/")) return "vendor-tauri";
  if (/^(tailwind-merge|clsx)\//.test(packagePath)) return "vendor-style";

  return void 0;
}
