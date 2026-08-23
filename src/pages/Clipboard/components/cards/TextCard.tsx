import type { CSSProperties, FC, MouseEvent } from "react";
import { useSnapshot } from "valtio";
import Highlight from "@/components/Highlight";
import { clipboardViewState } from "@/stores/clipboardView";
import type { ClipboardItem } from "@/types/clipboard";
import { cn } from "@/utils/cn";
import { isMobile } from "@/utils/is";

interface TextCardProps extends ClipboardItem {
  /**
   * MOD 键按下时，URL / Email 以可点击链接样式渲染。
   */
  isLinkActive?: boolean;
  /**
   * 点击 URL / Email 文本时由列表层打开系统浏览器或邮件客户端。
   */
  onOpenLink?: () => void;
}

/**
 * 文本类卡片：渲染 summary（列表视图 content 已置空），按设置限制最大显示行数。
 * 子类型（HTML/RTF/URL/Email/Color/Path）以小 Tag 提示。
 */
const TextCard: FC<TextCardProps> = (props) => {
  const { summary, subKind, colorPreview, isLinkActive, onOpenLink } = props;
  const { keyword } = useSnapshot(clipboardViewState);
  const isOpenableLink =
    isLinkActive && (subKind === "url" || subKind === "email");

  if (subKind === "color" && colorPreview) {
    const style: CSSProperties = {
      backgroundColor: colorPreview,
    };

    return (
      <div className="flex h-full flex-col justify-between py-1">
        <div
          className="h-20 w-full rounded-xl border border-black/10 shadow-xs"
          style={style}
        />
        <span className="font-bold font-mono text-neutral-800 text-xs">
          {colorPreview}
        </span>
      </div>
    );
  }

  const handleLinkMouseDown = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
  };

  const handleLinkClick = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();

    onOpenLink?.();
  };

  if (isOpenableLink) {
    return (
      <button
        className={cn(
          "block w-full min-w-0 cursor-pointer whitespace-pre-wrap break-all border-0 bg-transparent p-0 text-left text-[#007AFF] text-[15px] leading-[1.4] tracking-tight underline underline-offset-2",
          isMobile() ? "line-clamp-6" : "line-clamp-[20]",
        )}
        onClick={handleLinkClick}
        onMouseDown={handleLinkMouseDown}
        type="button"
      >
        <Highlight keyword={keyword} text={summary ?? ""} />
      </button>
    );
  }

  return (
    <div
      className={cn(
        "w-full min-w-0 whitespace-pre-wrap break-all font-sans text-[15px] text-neutral-900 leading-[1.4] tracking-tight dark:text-neutral-100",
        isMobile() ? "line-clamp-6" : "line-clamp-[20]",
      )}
    >
      <Highlight keyword={keyword} text={summary ?? ""} />
    </div>
  );
};

export default TextCard;
