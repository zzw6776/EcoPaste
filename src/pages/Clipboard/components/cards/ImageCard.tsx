import type { FC } from "react";
import AssetImage from "@/components/AssetImage";
import type { ClipboardItem } from "@/types/clipboard";

/**
 * 图片类卡片：使用 Rust 返回的缩略图路径，并在卡片内容区内等比完整展示。
 */
const ImageCard: FC<ClipboardItem> = (props) => {
  const { imageThumbnailPath } = props;

  return (
    <div className="flex h-full w-full items-center justify-center overflow-hidden">
      <AssetImage
        className="size-full rounded-md object-contain"
        src={imageThumbnailPath}
      />
    </div>
  );
};

export default ImageCard;
