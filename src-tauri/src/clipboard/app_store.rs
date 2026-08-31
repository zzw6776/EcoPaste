//! 来源应用图标的规范化与落盘。
//!
//! 图标统一转换为 64 × 64 RGBA PNG，并按规范化字节的 BLAKE3 平铺存放在
//! `<app_local_data>/resources/app-icons/<hash>.png`。稳定字节既用于本机去重，
//! 也作为跨设备来源图标资源的明文身份。

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Context};
use image::codecs::png::PngEncoder;
use image::imageops::FilterType;
use image::{ColorType, ImageEncoder, ImageFormat, ImageReader, Limits, RgbaImage};
use tauri::AppHandle;

use crate::core::Result;

const APP_ICONS_DIR: &str = "app-icons";
pub const APP_ICON_SIZE: u32 = 64;
pub const MAX_APP_ICON_BYTES: usize = 256 * 1024;
const MAX_SOURCE_ICON_BYTES: usize = 4 * 1024 * 1024;
const MAX_SOURCE_ICON_DIMENSION: u32 = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAppIcon {
    pub file_name: String,
    pub icon_hash: String,
    pub original_size: u64,
    pub accent_start: String,
    pub accent_end: String,
}

#[derive(Clone)]
pub struct AppIconStore {
    root: Arc<RwLock<PathBuf>>,
}

impl AppIconStore {
    pub fn new(app: &AppHandle) -> Result<Self> {
        Ok(Self {
            root: Arc::new(RwLock::new(
                crate::core::paths::resources_dir(app)?.join(APP_ICONS_DIR),
            )),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(root: PathBuf) -> Self {
        Self {
            root: Arc::new(RwLock::new(root)),
        }
    }

    /// 重新绑定到当前真实数据根；数据目录热迁移后由存储命令调用。
    pub fn rebase(&self, app: &AppHandle) -> Result<()> {
        let next = crate::core::paths::resources_dir(app)?.join(APP_ICONS_DIR);
        *self
            .root
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
        Ok(())
    }

    /// 返回同步和渲染共同需要的哈希、尺寸与强调色。
    pub fn store_with_metadata(&self, png_bytes: &[u8]) -> Result<StoredAppIcon> {
        let (normalized, rgba) = normalize_png(png_bytes)?;
        self.store_normalized(&normalized, &rgba)
    }

    /// 校验同步下载的规范化 PNG 与明文哈希，成功后原子写入缓存。
    pub fn store_synced_png(&self, png_bytes: &[u8], expected_hash: &str) -> Result<StoredAppIcon> {
        if png_bytes.len() > MAX_APP_ICON_BYTES {
            return Err(anyhow!("source app icon is too large").into());
        }
        if blake3_hex(png_bytes) != expected_hash {
            return Err(anyhow!("source app icon hash mismatch").into());
        }
        if png_bytes.get(24) != Some(&8) || png_bytes.get(25) != Some(&6) {
            return Err(anyhow!("source app icon must be an 8-bit RGBA PNG").into());
        }
        let rgba = decode_png(png_bytes, APP_ICON_SIZE)?;
        if rgba.dimensions() != (APP_ICON_SIZE, APP_ICON_SIZE) {
            return Err(anyhow!("source app icon dimensions are invalid").into());
        }
        self.store_normalized(png_bytes, &rgba)
    }

    /// 读取已经按同步哈希命名的 PNG，并在不重新编码的前提下恢复元数据。
    pub fn synced_metadata_for_hash(&self, expected_hash: &str) -> Result<StoredAppIcon> {
        if !is_hex_hash(expected_hash) {
            return Err(anyhow!("source app icon hash is invalid").into());
        }
        let file_name = format!("{expected_hash}.png");
        let path = self.icon_path(&file_name);
        let bytes = std::fs::read(&path)
            .with_context(|| format!("failed to read synchronized app icon {path:?}"))?;
        self.store_synced_png(&bytes, expected_hash)
    }

    /// 重新规范化旧缓存，供升级后的首次同步补齐稳定元数据。
    pub fn refresh_metadata(&self, file_name: &str) -> Result<StoredAppIcon> {
        let path = self.icon_path(file_name);
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read app icon {path:?}"))?;
        self.store_with_metadata(&bytes)
    }

    pub fn icon_path(&self, file_name: &str) -> PathBuf {
        self.root().join(file_name)
    }

    pub fn icon_file_for_hash(&self, icon_hash: &str) -> Option<String> {
        if !is_hex_hash(icon_hash) {
            return None;
        }
        let file_name = format!("{icon_hash}.png");
        let path = self.icon_path(&file_name);
        let bytes = std::fs::read(path).ok()?;
        (blake3_hex(&bytes) == icon_hash).then_some(file_name)
    }

    fn store_normalized(&self, png_bytes: &[u8], rgba: &RgbaImage) -> Result<StoredAppIcon> {
        let icon_hash = blake3_hex(png_bytes);
        let file_name = format!("{icon_hash}.png");
        write_if_absent(&self.root().join(&file_name), png_bytes)?;
        let (accent_start, accent_end) = accent_colors(rgba, &icon_hash);
        Ok(StoredAppIcon {
            file_name,
            icon_hash,
            original_size: png_bytes.len() as u64,
            accent_start,
            accent_end,
        })
    }

    fn root(&self) -> PathBuf {
        self.root
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// 无图标时从同步空间内的匿名来源 key 确定性生成强调色。
pub fn fallback_accent_colors(source_key: &str) -> (String, String) {
    let digest = blake3::hash(source_key.as_bytes());
    let bytes = digest.as_bytes();
    let hue = u16::from_be_bytes([bytes[0], bytes[1]]) as f64 / u16::MAX as f64 * 360.0;
    harmonize_hsl(hue, 84.0)
}

fn normalize_png(bytes: &[u8]) -> Result<(Vec<u8>, RgbaImage)> {
    if bytes.is_empty() || bytes.len() > MAX_SOURCE_ICON_BYTES {
        return Err(anyhow!("source app icon input is too large").into());
    }
    let decoded = decode_png(bytes, MAX_SOURCE_ICON_DIMENSION)?;
    let normalized =
        image::imageops::resize(&decoded, APP_ICON_SIZE, APP_ICON_SIZE, FilterType::Lanczos3);
    let mut png = Vec::with_capacity((APP_ICON_SIZE * APP_ICON_SIZE * 4) as usize);
    PngEncoder::new(&mut png)
        .write_image(
            normalized.as_raw(),
            APP_ICON_SIZE,
            APP_ICON_SIZE,
            ColorType::Rgba8.into(),
        )
        .context("failed to encode normalized app icon")?;
    if png.len() > MAX_APP_ICON_BYTES {
        return Err(anyhow!("normalized source app icon is too large").into());
    }
    Ok((png, normalized))
}

fn decode_png(bytes: &[u8], maximum_dimension: u32) -> Result<RgbaImage> {
    if !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Err(anyhow!("source app icon is not PNG").into());
    }
    let mut reader = ImageReader::with_format(Cursor::new(bytes), ImageFormat::Png);
    let mut limits = Limits::default();
    limits.max_image_width = Some(maximum_dimension);
    limits.max_image_height = Some(maximum_dimension);
    limits.max_alloc = Some(32 * 1024 * 1024);
    reader.limits(limits);
    Ok(reader
        .decode()
        .context("failed to decode source app icon")?
        .into_rgba8())
}

fn accent_colors(image: &RgbaImage, fallback_seed: &str) -> (String, String) {
    let mut buckets = [(0_f64, 0_f64, 0_f64, 0_f64); 12];
    let mut colored = 0_u32;
    let mut dark_neutral = 0_u32;
    let mut sampled = 0_u32;
    for pixel in image.pixels() {
        let [red, green, blue, alpha] = pixel.0;
        if alpha < 100 {
            continue;
        }
        sampled += 1;
        let delta = red.max(green).max(blue) - red.min(green).min(blue);
        let brightness =
            (u32::from(red) * 299 + u32::from(green) * 587 + u32::from(blue) * 114) / 1000;
        if delta < 22 {
            dark_neutral += u32::from(brightness < 120);
            continue;
        }
        let (hue, saturation, _) = rgb_to_hsl(red, green, blue);
        if saturation < 20.0 {
            continue;
        }
        colored += 1;
        let index = ((hue / 30.0).floor() as usize).min(11);
        let weight = saturation / 50.0 + f64::from(delta) / 255.0;
        buckets[index].0 += f64::from(red) * weight;
        buckets[index].1 += f64::from(green) * weight;
        buckets[index].2 += f64::from(blue) * weight;
        buckets[index].3 += weight;
    }
    if colored < 32 && dark_neutral * 10 > sampled {
        return ("#1E293B".to_owned(), "#0F172A".to_owned());
    }
    let best = buckets
        .into_iter()
        .max_by(|left, right| left.3.total_cmp(&right.3));
    let Some((red, green, blue, weight)) = best.filter(|bucket| bucket.3 > 0.0) else {
        return fallback_accent_colors(fallback_seed);
    };
    let (hue, saturation, _) = rgb_to_hsl(
        (red / weight).round() as u8,
        (green / weight).round() as u8,
        (blue / weight).round() as u8,
    );
    harmonize_hsl(hue, saturation.clamp(78.0, 95.0))
}

fn rgb_to_hsl(red: u8, green: u8, blue: u8) -> (f64, f64, f64) {
    let (red, green, blue) = (
        f64::from(red) / 255.0,
        f64::from(green) / 255.0,
        f64::from(blue) / 255.0,
    );
    let maximum = red.max(green).max(blue);
    let minimum = red.min(green).min(blue);
    let lightness = (maximum + minimum) / 2.0;
    let delta = maximum - minimum;
    if delta == 0.0 {
        return (0.0, 0.0, lightness * 100.0);
    }
    let saturation = delta
        / if lightness > 0.5 {
            2.0 - maximum - minimum
        } else {
            maximum + minimum
        };
    let hue = if maximum == red {
        (green - blue) / delta + if green < blue { 6.0 } else { 0.0 }
    } else if maximum == green {
        (blue - red) / delta + 2.0
    } else {
        (red - green) / delta + 4.0
    } / 6.0;
    (hue * 360.0, saturation * 100.0, lightness * 100.0)
}

fn harmonize_hsl(mut hue: f64, saturation: f64) -> (String, String) {
    if (38.0..=65.0).contains(&hue) {
        hue = 36.0;
    }
    (
        hsl_to_hex(hue, saturation, 58.0),
        hsl_to_hex(hue, saturation, 51.0),
    )
}

fn hsl_to_hex(hue: f64, saturation: f64, lightness: f64) -> String {
    let lightness = lightness / 100.0;
    let a = saturation / 100.0 * lightness.min(1.0 - lightness);
    let channel = |offset: f64| {
        let k = (offset + hue / 30.0).rem_euclid(12.0);
        let value = lightness - a * (k - 3.0).min(9.0 - k).clamp(-1.0, 1.0);
        (255.0 * value).round().clamp(0.0, 255.0) as u8
    };
    format!(
        "#{:02X}{:02X}{:02X}",
        channel(0.0),
        channel(8.0),
        channel(4.0)
    )
}

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn is_hex_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn write_if_absent(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.is_file() && std::fs::read(path).is_ok_and(|current| current == bytes) {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create app-icons dir {parent:?}"))?;
    }
    let temporary = path.with_extension(format!("part-{}", uuid::Uuid::new_v4()));
    std::fs::write(&temporary, bytes)
        .with_context(|| format!("failed to write app icon {temporary:?}"))?;
    if path.exists() {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to replace invalid app icon {path:?}"))
                    .map_err(Into::into);
            }
        }
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        std::fs::remove_file(&temporary).ok();
        if !path.is_file() || !std::fs::read(path).is_ok_and(|current| current.as_slice() == bytes)
        {
            return Err(error)
                .with_context(|| format!("failed to commit app icon {path:?}"))
                .map_err(Into::into);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::codecs::png::{CompressionType, FilterType as PngFilterType};

    #[test]
    fn normalizes_and_stores_idempotently() {
        let directory = tempfile::tempdir().unwrap();
        let store = AppIconStore::for_test(directory.path().join("app-icons"));
        let image = RgbaImage::from_pixel(128, 96, image::Rgba([235, 68, 90, 255]));
        let mut bytes = Vec::new();
        PngEncoder::new(&mut bytes)
            .write_image(image.as_raw(), 128, 96, ColorType::Rgba8.into())
            .unwrap();

        let first = store.store_with_metadata(&bytes).unwrap();
        let second = store.store_with_metadata(&bytes).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.file_name, format!("{}.png", first.icon_hash));
        let stored = std::fs::read(store.icon_path(&first.file_name)).unwrap();
        assert_eq!(
            decode_png(&stored, APP_ICON_SIZE).unwrap().dimensions(),
            (64, 64)
        );
        assert!(store.store_synced_png(&stored, &first.icon_hash).is_ok());
        assert!(store.store_synced_png(&stored, &"0".repeat(64)).is_err());

        std::fs::write(store.icon_path(&first.file_name), b"corrupt").unwrap();
        assert_eq!(store.icon_file_for_hash(&first.icon_hash), None);
        store.store_synced_png(&stored, &first.icon_hash).unwrap();
        assert_eq!(
            store.icon_file_for_hash(&first.icon_hash),
            Some(first.file_name)
        );
    }

    #[test]
    fn accepts_canonical_png_from_a_different_encoder_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let store = AppIconStore::for_test(directory.path().join("app-icons"));
        let image = RgbaImage::from_pixel(64, 64, image::Rgba([20, 80, 180, 255]));
        let mut png = Vec::new();
        PngEncoder::new_with_quality(&mut png, CompressionType::Fast, PngFilterType::NoFilter)
            .write_image(image.as_raw(), 64, 64, ColorType::Rgba8.into())
            .unwrap();
        let hash = blake3_hex(&png);

        let stored = store.store_synced_png(&png, &hash).unwrap();

        assert_eq!(stored.icon_hash, hash);
        assert_eq!(
            std::fs::read(store.icon_path(&stored.file_name)).unwrap(),
            png
        );
        assert_eq!(
            store.synced_metadata_for_hash(&stored.icon_hash).unwrap(),
            stored
        );
    }
}
