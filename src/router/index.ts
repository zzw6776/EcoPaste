import { createHashRouter } from "react-router";

export const router = createHashRouter([
  {
    lazy: async () => {
      const page = await import("@/pages/Clipboard");
      return { Component: page.default };
    },
    path: "/",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/Preference");
      return { Component: page.default };
    },
    path: "/preference",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/Onboarding");
      return { Component: page.default };
    },
    path: "/onboarding",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/ContextMenu");
      return { Component: page.default };
    },
    path: "/context-menu",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/ContextMenu");
      return { Component: page.ContextSubmenu };
    },
    path: "/context-submenu",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/Preview");
      return { Component: page.default };
    },
    path: "/preview",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/Update");
      return { Component: page.default };
    },
    path: "/update",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/SyncPopups");
      return { Component: page.CloudDevicesPage };
    },
    path: "/cloud-devices",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/SyncPopups");
      return { Component: page.CloudRecordsPage };
    },
    path: "/cloud-records",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/SyncPopups");
      return { Component: page.CloudDetailsPage };
    },
    path: "/cloud-details",
  },
  {
    lazy: async () => {
      const page = await import("@/pages/SyncPopups");
      return { Component: page.LanDetailsPage };
    },
    path: "/lan-details",
  },
]);
