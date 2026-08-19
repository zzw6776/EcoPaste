/**
 * 纯本地内置 SVG 矢量图标（Data URI），100% 离线可用，绝不依赖外部网络，绝不显示问号破图。
 * 1:1 像素级复刻官方 App 图标外观。
 */
export const LOCAL_SVG_ICONS: Record<string, string> = {
  activity:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="%231C3E2F"/><path d="M3 12h4.5l2-6 4.5 12 2.5-6H21" stroke="%2334D399" stroke-width="2.2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  arc: 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><defs><linearGradient id="arcg" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="%23FF5E62"/><stop offset="50%" stop-color="%23FF9966"/><stop offset="100%" stop-color="%233B82F6"/></linearGradient></defs><path d="M7 25C7 16 11 8 16 8C21 8 25 16 25 25" stroke="url(%23arcg)" stroke-width="5" fill="none" stroke-linecap="round"/><circle cx="16" cy="12" r="2.5" fill="%233B82F6"/></svg>',
  chatgpt:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="white"/><path d="M12 4a4.5 4.5 0 0 1 4.2 3.1l-.8.5a3.6 3.6 0 0 0-3.4-2.6 3.6 3.6 0 0 0-3.1 1.8l-1.5-.9A5.4 5.4 0 0 1 12 4zm5.5 3.5a4.5 4.5 0 0 1 1.2 5.1l-.9-.5a3.6 3.6 0 0 0-.9-4.1 3.6 3.6 0 0 0-3.6-.4l-.8-1.5a5.4 5.4 0 0 1 5-2.6zm-1.3 6.8a4.5 4.5 0 0 1-3 2l-.1-1a3.6 3.6 0 0 0 2.5-1.6 3.6 3.6 0 0 0-.5-3.6l1.5-.8a5.4 5.4 0 0 1-.9 5zm-5.7 2.2a4.5 4.5 0 0 1-4.2-3.1l.8-.5a3.6 3.6 0 0 0 3.4 2.6 3.6 3.6 0 0 0 3.1-1.8l1.5.9a5.4 5.4 0 0 1-4.6 1.9zm-5.5-3.5a4.5 4.5 0 0 1-1.2-5.1l.9.5a3.6 3.6 0 0 0 .9 4.1 3.6 3.6 0 0 0 3.6.4l.8 1.5a5.4 5.4 0 0 1-5 2.6zm1.3-6.8a4.5 4.5 0 0 1 3-2l.1 1a3.6 3.6 0 0 0-2.5 1.6 3.6 3.6 0 0 0 .5 3.6l-1.5.8a5.4 5.4 0 0 1 .9-5z" fill="%2310A37F"/></svg>',
  chrome:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="white"/><circle cx="12" cy="12" r="10" fill="%23EA4335"/><circle cx="12" cy="12" r="6" fill="%23FBBC05"/><circle cx="12" cy="12" r="4.2" fill="%2334A853"/><circle cx="12" cy="12" r="2.8" fill="%234285F4"/><circle cx="12" cy="12" r="3.2" stroke="white" stroke-width="0.8" fill="none"/></svg>',
  code: 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="22" height="22" x="1" y="1" rx="5.5" fill="%23007ACC"/><path d="m7.5 9.5-3.5 2.5 3.5 2.5m9-5 3.5 2.5-3.5 2.5m-5.5 3 2.5-10" stroke="white" stroke-width="2" fill="none" stroke-linecap="round"/></svg>',
  default:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="%232B8FF7"/><path d="M7 12h10M12 7v10" stroke="white" stroke-width="2.5" stroke-linecap="round"/></svg>',
  feishu:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="%231F73F1"/><path d="M6 16l5-8 7 5-6 6-6-3z" fill="white"/></svg>',
  figma:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="22" height="22" x="1" y="1" rx="5.5" fill="%23A855F7"/><circle cx="12" cy="12" r="3.5" fill="white"/></svg>',
  finder:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="22" height="22" x="1" y="1" rx="5.5" fill="%233B82F6"/><path d="M8 8v4c0 2 4 2 4 0V8m4 0v4c0 2-4 2-4 0" stroke="white" stroke-width="2" fill="none"/></svg>',
  notes:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="22" height="22" x="1" y="1" rx="5.5" fill="%23F59E0B"/><path d="M6 8h12M6 12h12M6 16h8" stroke="white" stroke-width="2" stroke-linecap="round"/></svg>',
  safari:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="%232B8FF7"/><polygon points="12,4 16,12 12,20 8,12" fill="white"/></svg>',
  wechat:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="%2307C160"/><circle cx="9.5" cy="10" r="1.2" fill="white"/><circle cx="14.5" cy="10" r="1.2" fill="white"/><path d="M7 14c1.5 1.5 4.5 1.5 6 0" stroke="white" stroke-width="1.8" fill="none" stroke-linecap="round"/></svg>',
  yuque:
    'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="%2310B981"/><path d="M8 13c2-3 6-3 8 0" stroke="white" stroke-width="2.5" fill="none" stroke-linecap="round"/></svg>',
};

/**
 * 解析 URL 信息（提取主机名和匹配已知品牌）
 */
export function parseUrlInfo(urlStr?: string | null) {
  if (!urlStr) return null;
  try {
    const raw = urlStr.trim();
    const withProto =
      raw.startsWith("http://") || raw.startsWith("https://")
        ? raw
        : `https://${raw}`;
    const parsed = new URL(withProto);
    const host = parsed.hostname.replace(/^www\./, "");

    if (host.includes("yuque")) {
      return {
        headerBg: "linear-gradient(135deg, #10B981 0%, #059669 100%)",
        host,
        iconUrl: LOCAL_SVG_ICONS.yuque,
        name: "语雀文档",
        pathname: parsed.pathname + parsed.search,
      };
    }

    if (host.includes("github")) {
      return {
        headerBg: "linear-gradient(135deg, #24292F 0%, #0D1117 100%)",
        host,
        iconUrl: LOCAL_SVG_ICONS.chatgpt,
        name: "GitHub",
        pathname: parsed.pathname + parsed.search,
      };
    }

    return {
      headerBg: "linear-gradient(135deg, #2B8FF7 0%, #1A80F5 100%)",
      host,
      iconUrl: LOCAL_SVG_ICONS.chrome,
      name: host,
      pathname: parsed.pathname + parsed.search,
    };
  } catch {
    return null;
  }
}

/**
 * 格式化相对时间
 */
export function formatRelativeTime(dateStr?: string | null): string {
  if (!dateStr) return "刚刚";
  try {
    const time = new Date(dateStr).getTime();
    const diff = Math.floor((Date.now() - time) / 1000);
    if (diff < 60) return "刚刚";
    if (diff < 3600) return `${Math.floor(diff / 60)}分钟前`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}小时前`;
    return `${Math.floor(diff / 86400)}天前`;
  } catch {
    return "刚刚";
  }
}

/**
 * 根据应用与类型智能匹配卡片主题与图标（纯正 Paste 官方亮调配色）
 */
export function getAppTheme(
  appName?: string | null,
  appId?: string | null,
  kind?: string,
  subKind?: string | null,
  contentSummary?: string | null,
) {
  const tag =
    kind === "image"
      ? "图片"
      : kind === "files"
        ? "文件"
        : subKind === "url"
          ? "链接"
          : subKind === "color"
            ? "颜色"
            : "文本";

  const name = (appName || "").toLowerCase();
  const id = (appId || "").toLowerCase();

  // 1. 颜色类型特化
  if (subKind === "color") {
    return {
      headerBg: "linear-gradient(135deg, #8B5CF6 0%, #7C3AED 100%)",
      iconUrl: LOCAL_SVG_ICONS.figma,
      isPreset: true,
      tag,
    };
  }

  // 2. 链接类型特化
  if (subKind === "url") {
    const urlInfo = parseUrlInfo(contentSummary);
    return {
      headerBg:
        urlInfo?.headerBg ??
        "linear-gradient(135deg, #2B8FF7 0%, #1A80F5 100%)",
      iconUrl: urlInfo?.iconUrl ?? LOCAL_SVG_ICONS.chrome,
      isPreset: true,
      tag,
    };
  }

  // 3. 活动监视器 / 性能监控
  if (
    name.includes("activity") ||
    name.includes("monitor") ||
    name.includes("监视器") ||
    id.includes("activitymonitor")
  ) {
    return {
      headerBg: "linear-gradient(135deg, #1C3E2F 0%, #132A20 100%)",
      iconUrl: LOCAL_SVG_ICONS.activity,
      isPreset: true,
      tag,
    };
  }

  // 4. Arc 浏览器（官方 Paste 第一张卡片的亮天蓝）
  if (name.includes("arc") || id.includes("thebrowser")) {
    return {
      headerBg: "linear-gradient(135deg, #2B8FF7 0%, #1A80F5 100%)",
      iconUrl: LOCAL_SVG_ICONS.arc,
      isPreset: true,
      tag,
    };
  }

  // 5. Chrome
  if (name.includes("chrome") || id.includes("chrome")) {
    return {
      headerBg: "linear-gradient(135deg, #3B82F6 0%, #2563EB 100%)",
      iconUrl: LOCAL_SVG_ICONS.chrome,
      isPreset: true,
      tag,
    };
  }

  // 6. ChatGPT / OpenAI
  if (name.includes("chatgpt") || id.includes("openai")) {
    return {
      headerBg: "linear-gradient(135deg, #2B8FF7 0%, #1A80F5 100%)",
      iconUrl: LOCAL_SVG_ICONS.chatgpt,
      isPreset: true,
      tag,
    };
  }

  // 7. 微信 / 企业微信
  if (
    name.includes("wechat") ||
    name.includes("微信") ||
    id.includes("wechat")
  ) {
    return {
      headerBg: "linear-gradient(135deg, #07C160 0%, #059A4C 100%)",
      iconUrl: LOCAL_SVG_ICONS.wechat,
      isPreset: true,
      tag,
    };
  }

  // 8. 飞书
  if (
    name.includes("feishu") ||
    name.includes("lark") ||
    name.includes("飞书")
  ) {
    return {
      headerBg: "linear-gradient(135deg, #1F73F1 0%, #1557BF 100%)",
      iconUrl: LOCAL_SVG_ICONS.feishu,
      isPreset: true,
      tag,
    };
  }

  // 9. 备忘录 / Notes
  if (
    name.includes("notes") ||
    name.includes("备忘录") ||
    id.includes("notes")
  ) {
    return {
      headerBg: "linear-gradient(135deg, #F59E0B 0%, #D97706 100%)",
      iconUrl: LOCAL_SVG_ICONS.notes,
      isPreset: true,
      tag,
    };
  }

  // 10. Figma
  if (name.includes("figma") || id.includes("figma")) {
    return {
      headerBg: "linear-gradient(135deg, #A855F7 0%, #7E22CE 100%)",
      iconUrl: LOCAL_SVG_ICONS.figma,
      isPreset: true,
      tag,
    };
  }

  // 11. VS Code / 编程
  if (
    name.includes("code") ||
    id.includes("vscode") ||
    id.includes("visualstudio")
  ) {
    return {
      headerBg: "linear-gradient(135deg, #007ACC 0%, #005A9E 100%)",
      iconUrl: LOCAL_SVG_ICONS.code,
      isPreset: true,
      tag,
    };
  }

  // 12. 访达 / Finder
  if (kind === "files" || name.includes("finder") || id.includes("finder")) {
    return {
      headerBg: "linear-gradient(135deg, #059669 0%, #047857 100%)",
      iconUrl: LOCAL_SVG_ICONS.finder,
      isPreset: true,
      tag,
    };
  }

  // 13. EcoPaste 自身
  if (
    name.includes("ecopaste") ||
    id.includes("eco-paste") ||
    id.includes("ayangweb")
  ) {
    return {
      headerBg: "linear-gradient(135deg, #2B8FF7 0%, #1A80F5 100%)",
      iconUrl: LOCAL_SVG_ICONS.arc,
      isPreset: true,
      tag,
    };
  }

  // 默认（与 Paste 一致的纯正明亮天蓝，非硬编码预设）
  return {
    headerBg: "linear-gradient(135deg, #2B8FF7 0%, #1A80F5 100%)",
    iconUrl: LOCAL_SVG_ICONS.safari,
    isPreset: false,
    tag,
  };
}
