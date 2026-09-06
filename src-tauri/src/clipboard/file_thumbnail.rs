//! 文件卡片的静态图缓存。独立于原始资源，源路径与元数据变化自动使用新键。

use anyhow::{Context, Result};
use image::{ImageDecoder, ImageFormat, ImageReader};
use std::{
    fs,
    io::{BufReader, Cursor, Write},
    path::{Path, PathBuf},
};

const MAX_DIMENSION: u32 = 1200;
const CACHE_BYTES: u64 = 128 * 1024 * 1024;

pub(super) fn cache_path(root: &Path, source: &Path) -> Result<Option<PathBuf>> {
    let extension = source
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if !matches!(extension.as_str(), "png" | "jpg" | "jpeg" | "bmp") {
        return Ok(None);
    }
    let metadata = source.metadata()?;
    let key = format!(
        "v1:{MAX_DIMENSION}:{source:?}:{}:{:?}:{:?}",
        metadata.len(),
        metadata.modified()?,
        metadata.created().ok()
    );
    let digest = blake3::hash(key.as_bytes()).to_hex();
    Ok(Some(root.join(format!("{digest}.png"))))
}

/// 只缩放静态图片，保留透明度与 EXIF 方向；动画或不支持的实际格式继续使用原文件。
pub(super) fn generate(root: &Path, source: &Path, target: &Path) -> Result<PathBuf> {
    if target.exists() {
        return Ok(target.to_path_buf());
    }
    let reader = ImageReader::open(source)?.with_guessed_format()?;
    let format = reader.format();
    if !matches!(
        format,
        Some(ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::Bmp)
    ) {
        return keep_original(source, target);
    }
    if format == Some(ImageFormat::Png) {
        let decoder = image::codecs::png::PngDecoder::new(BufReader::new(fs::File::open(source)?))
            .context("failed to read PNG header")?;
        if decoder
            .is_apng()
            .context("failed to inspect PNG animation")?
            || decoder
                .gamma_value()
                .context("failed to inspect PNG gamma")?
                .is_some()
        {
            return keep_original(source, target);
        }
    }
    let mut decoder = reader
        .into_decoder()
        .context("failed to decode file thumbnail")?;
    if decoder
        .icc_profile()
        .context("failed to inspect image color profile")?
        .is_some()
        || (format == Some(ImageFormat::Png)
            && decoder
                .exif_metadata()
                .context("failed to inspect PNG metadata")?
                .is_some())
    {
        return keep_original(source, target);
    }
    let orientation = decoder
        .orientation()
        .context("failed to read image orientation")?;
    let mut image =
        image::DynamicImage::from_decoder(decoder).context("failed to decode file image")?;
    image.apply_orientation(orientation);
    let thumbnail = image.thumbnail(MAX_DIMENSION, MAX_DIMENSION);
    let mut bytes = Cursor::new(Vec::new());
    thumbnail
        .write_to(&mut bytes, ImageFormat::Png)
        .context("failed to encode file thumbnail")?;
    // 正在改写的源文件不发布到旧版本的缓存键。
    if cache_path(root, source)?.as_deref() != Some(target) {
        return Ok(source.to_path_buf());
    }
    atomic_write(target, bytes.get_ref())?;
    trim_cache(root, target);
    Ok(target.to_path_buf())
}

fn keep_original(source: &Path, target: &Path) -> Result<PathBuf> {
    let marker = target.with_extension("original");
    atomic_write(&marker, b"")?;
    if let Some(root) = target.parent() {
        trim_cache(root, &marker);
    }
    Ok(source.to_path_buf())
}

fn atomic_write(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target.parent().context("file thumbnail has no parent")?;
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(bytes)?;
    temp.persist(target)
        .map_err(|error| anyhow::anyhow!(error))?;
    Ok(())
}

/// 只淘汰本模块缓存，按生成时间回收；原图与用户资源不在此目录。
fn trim_cache(root: &Path, current: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let mut files = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if !matches!(path.extension()?.to_str()?, "png" | "original") {
                return None;
            }
            let metadata = entry.metadata().ok()?;
            Some((path, metadata.len(), metadata.modified().ok()))
        })
        .collect::<Vec<_>>();
    let mut bytes = files.iter().map(|entry| entry.1).sum::<u64>();
    let mut count = files.len();
    files.sort_by_key(|entry| entry.2);
    for (path, size, _) in files {
        if bytes <= CACHE_BYTES && count <= 1024 {
            break;
        }
        if path != current && fs::remove_file(path).is_ok() {
            bytes = bytes.saturating_sub(size);
            count -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_thumbnail_preserves_alpha_and_source_and_invalidates_on_change() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.png");
        let root = temp.path().join("cache");
        let pixels = image::RgbaImage::from_pixel(1600, 800, image::Rgba([10, 20, 30, 128]));
        pixels.save(&source).unwrap();
        let original = fs::read(&source).unwrap();
        let target = cache_path(&root, &source).unwrap().unwrap();
        assert_eq!(generate(&root, &source, &target).unwrap(), target);
        let thumbnail = image::open(&target).unwrap().to_rgba8();
        assert_eq!(thumbnail.dimensions(), (1200, 600));
        assert_eq!(thumbnail.get_pixel(0, 0).0, [10, 20, 30, 128]);
        assert_eq!(fs::read(&source).unwrap(), original);
        image::RgbaImage::new(100, 100).save(&source).unwrap();
        assert_ne!(cache_path(&root, &source).unwrap().unwrap(), target);
        assert!(cache_path(&root, &temp.path().join("animation.gif"))
            .unwrap()
            .is_none());
        assert!(cache_path(&root, &temp.path().join("animation.webp"))
            .unwrap()
            .is_none());
    }
}
