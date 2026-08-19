import type { FC } from "react";
import AssetImage from "@/components/AssetImage";
import type { ClipboardItem } from "@/types/clipboard";

/**
 * 图片类卡片：按 `content`（文件名）向 Rust 取缩略图路径并 `convertFileSrc` 加载。
 * 高度按设置限制，宽高来自 DB（width/height），不存在时不展示尺寸文案。
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
