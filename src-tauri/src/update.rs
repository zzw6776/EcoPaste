#[cfg(any(target_os = "android", test))]
use std::collections::HashMap;
#[cfg(target_os = "android")]
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use tauri::{AppHandle, Emitter, Manager};
#[cfg(not(target_os = "android"))]
use tauri_plugin_updater::{Update as TauriUpdate, UpdaterExt};
use url::Url;

use crate::core::{AppError, Result};
use crate::settings::{SettingsStore, Update as UpdateSettings, UpdateFrequency};

const UPDATE_PROGRESS_EVENT: &str = "update://progress";
#[cfg(target_os = "android")]
const UPDATE_STATUS_EVENT: &str = "update://status";
#[cfg(target_os = "android")]
const UPDATE_ERROR_EVENT: &str = "update://error";
const STABLE_ENDPOINT_ENV: &str = "ECOPASTE_UPDATE_ENDPOINT";
const DEFAULT_STABLE_ENDPOINT: &str =
    "https://github.com/zzw6776/EcoPaste/releases/latest/download/latest.json";
const AUTO_CHECK_INITIAL_DELAY_SECONDS: u64 = 8;
const AUTO_CHECK_SETTINGS_REFRESH_SECONDS: u64 = 60 * 60;
const AUTO_CHECK_FAILURE_RETRY_SECONDS: u64 = 60 * 60;
#[cfg(target_os = "android")]
const ANDROID_UPDATE_FILE_PREFIX: &str = "ecopaste-update-";

#[cfg(not(target_os = "android"))]
pub struct UpdateState {
    current: Mutex<Option<PendingUpdate>>,
}

#[cfg(not(target_os = "android"))]
struct PendingUpdate {
    update: TauriUpdate,
    metadata: UpdateMetadata,
    bytes: Option<Vec<u8>>,
}

#[cfg(target_os = "android")]
pub struct UpdateState {
    current: Mutex<Option<PendingUpdate>>,
}

#[cfg(target_os = "android")]
struct PendingUpdate {
    metadata: UpdateMetadata,
    apk_path: Option<PathBuf>,
    downloading: bool,
    sha256: String,
}

#[cfg(any(target_os = "android", test))]
#[derive(serde::Deserialize)]
struct AndroidUpdateManifest {
    version: String,
    notes: Option<String>,
    pub_date: Option<String>,
    platforms: HashMap<String, AndroidUpdatePlatform>,
}

#[cfg(any(target_os = "android", test))]
#[derive(serde::Deserialize)]
struct AndroidUpdatePlatform {
    #[serde(default)]
    sha256: Option<String>,
    url: String,
}

#[cfg(target_os = "android")]
impl UpdateState {
    fn new() -> Self {
        Self {
            current: Mutex::new(None),
        }
    }

    fn snapshot(&self, current_version: String) -> AppUpdateStatus {
        let update = self.with_current(|current| {
            current.as_ref().map(|pending| {
                let mut metadata = pending.metadata.clone();
                metadata.downloaded = pending.apk_path.as_ref().is_some_and(|path| path.is_file());
                metadata
            })
        });

        AppUpdateStatus {
            current_version,
            update,
        }
    }

    fn set_update(&self, update: Option<PendingUpdate>) {
        self.with_current(|current| {
            if let (Some(existing), Some(next)) = (current.as_ref(), update.as_ref()) {
                if existing.metadata.version == next.metadata.version
                    && existing.metadata.download_url == next.metadata.download_url
                {
                    return;
                }
            }

            *current = update;
        });
    }

    fn begin_download(&self, version: &str) -> Result<(String, String)> {
        self.with_current(|current| {
            let pending = current
                .as_mut()
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("no update is ready")))?;
            if pending.metadata.version != version {
                return Err(AppError::Other(anyhow::anyhow!(
                    "the selected update is no longer current"
                )));
            }
            if pending.downloading {
                return Err(AppError::Other(anyhow::anyhow!(
                    "the selected update is already downloading"
                )));
            }
            pending.downloading = true;
            Ok((
                pending.metadata.download_url.clone(),
                pending.sha256.clone(),
            ))
        })
    }

    fn mark_downloaded(&self, version: &str, apk_path: PathBuf) -> Result<UpdateMetadata> {
        self.with_current(|current| {
            let pending = current
                .as_mut()
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("no update is ready")))?;
            if pending.metadata.version != version {
                return Err(AppError::Other(anyhow::anyhow!(
                    "the selected update is no longer current"
                )));
            }
            pending.apk_path = Some(apk_path);
            pending.downloading = false;
            pending.metadata.downloaded = true;
            Ok(pending.metadata.clone())
        })
    }

    fn mark_download_failed(&self, version: &str) {
        self.with_current(|current| {
            if let Some(pending) = current.as_mut() {
                if pending.metadata.version == version {
                    pending.downloading = false;
                }
            }
        });
    }

    fn downloaded_path(&self, version: &str) -> Result<PathBuf> {
        self.with_current(|current| {
            let pending = current
                .as_ref()
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("no update is ready")))?;
            if pending.metadata.version != version {
                return Err(AppError::Other(anyhow::anyhow!(
                    "the selected update is no longer current"
                )));
            }
            pending
                .apk_path
                .clone()
                .filter(|path| path.is_file())
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("update is not downloaded")))
        })
    }

    fn with_current<R>(&self, f: impl FnOnce(&mut Option<PendingUpdate>) -> R) -> R {
        let mut guard = self.current.lock().unwrap_or_else(|poisoned| {
            log::error!("Android update state mutex poisoned, recovering");
            poisoned.into_inner()
        });
        f(&mut guard)
    }
}

#[cfg(target_os = "android")]
pub fn init(app: &AppHandle) {
    app.manage(UpdateState::new());
}

#[cfg(target_os = "android")]
pub async fn status(app: &AppHandle) -> AppUpdateStatus {
    app.state::<UpdateState>()
        .snapshot(app.package_info().version.to_string())
}

#[cfg(target_os = "android")]
pub async fn check(app: &AppHandle, mode: CheckMode) -> Result<AppUpdateStatus> {
    if matches!(mode, CheckMode::Auto) && !should_auto_check(app) {
        return Ok(status(app).await);
    }

    let settings = app.state::<SettingsStore>().snapshot();
    let endpoint = update_endpoints()?
        .into_iter()
        .next()
        .context("update endpoint is missing")?;
    let manifest = reqwest::Client::new()
        .get(endpoint)
        .header(reqwest::header::USER_AGENT, "EcoPaste Android updater")
        .send()
        .await
        .context("failed to check for updates")?
        .error_for_status()
        .context("update server rejected the request")?
        .json::<AndroidUpdateManifest>()
        .await
        .context("failed to decode update manifest")?;
    let current_version = semver::Version::parse(&app.package_info().version.to_string())
        .context("current app version is invalid")?;
    let available_version =
        semver::Version::parse(&manifest.version).context("available app version is invalid")?;
    let skipped = settings
        .update
        .skipped_version
        .as_deref()
        .is_some_and(|version| version == manifest.version);
    let state = app.state::<UpdateState>();

    if available_version > current_version && !skipped {
        let target = android_update_target();
        let platform = manifest
            .platforms
            .get(target)
            .with_context(|| format!("update manifest does not contain Android target {target}"))?;
        let sha256 = platform
            .sha256
            .clone()
            .with_context(|| format!("Android update target {target} has no SHA-256 digest"))?;
        let apk_path = app.path().app_cache_dir().ok().map(|cache_dir| {
            cache_dir.join(format!(
                "{ANDROID_UPDATE_FILE_PREFIX}{}.apk",
                manifest.version
            ))
        });
        let apk_path = apk_path.filter(|path| path.is_file());
        state.set_update(Some(PendingUpdate {
            metadata: UpdateMetadata {
                current_version: current_version.to_string(),
                version: manifest.version,
                date: manifest.pub_date,
                body: manifest.notes,
                target: target.to_owned(),
                download_url: platform.url.clone(),
                downloaded: apk_path.is_some(),
            },
            apk_path,
            downloading: false,
            sha256,
        }));
    } else {
        state.set_update(None);
    }

    persist_last_checked_at(app);
    let next = status(app).await;
    if let Some(update) = next.update.as_ref() {
        start_android_update(app, update.version.clone());
    }
    Ok(next)
}

#[cfg(target_os = "android")]
pub async fn download(app: &AppHandle, version: String) -> Result<UpdateMetadata> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    if app.state::<UpdateState>().downloaded_path(&version).is_ok() {
        return Ok(status(app).await.update.context("no update is ready")?);
    }

    let cache_dir = app
        .path()
        .app_cache_dir()
        .context("failed to resolve Android update cache directory")?;
    let (download_url, expected_sha256) = app.state::<UpdateState>().begin_download(&version)?;
    let apk_path = cache_dir.join(format!("{ANDROID_UPDATE_FILE_PREFIX}{version}.apk"));
    let temporary_path = apk_path.with_extension("apk.download");
    let result = async {
        use sha2::{Digest, Sha256};

        tokio::fs::create_dir_all(&cache_dir)
            .await
            .context("failed to create Android update cache directory")?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(8))
            .build()
            .context("failed to create Android update client")?;
        let mut response = client
            .get(&download_url)
            .header(reqwest::header::USER_AGENT, "EcoPaste Android updater")
            .header(reqwest::header::ACCEPT, "application/octet-stream")
            .send()
            .await
            .context("failed to download Android update")?
            .error_for_status()
            .context("Android update download was rejected")?;
        let total = response.content_length();
        let mut downloaded = 0_u64;
        let mut sha256 = Sha256::new();
        let mut file = tokio::fs::File::create(&temporary_path)
            .await
            .context("failed to create Android update file")?;
        while let Some(chunk) = response
            .chunk()
            .await
            .context("failed while reading Android update")?
        {
            file.write_all(&chunk)
                .await
                .context("failed to write Android update")?;
            sha256.update(&chunk);
            downloaded = downloaded.saturating_add(chunk.len() as u64);
            emit_progress(app, downloaded, total);
        }
        file.flush()
            .await
            .context("failed to flush Android update")?;
        drop(file);
        if downloaded == 0 || total.is_some_and(|expected| expected != downloaded) {
            anyhow::bail!("Android update download is incomplete");
        }
        let actual_sha256 = format!("{:x}", sha256.finalize());
        if !actual_sha256.eq_ignore_ascii_case(&expected_sha256) {
            anyhow::bail!("Android update checksum does not match the release manifest");
        }
        let mut header = [0_u8; 4];
        let mut downloaded_file = tokio::fs::File::open(&temporary_path)
            .await
            .context("failed to validate Android update")?;
        downloaded_file
            .read_exact(&mut header)
            .await
            .context("failed to read Android update header")?;
        if &header != b"PK\x03\x04" {
            anyhow::bail!("downloaded Android update is not an APK");
        }
        if tokio::fs::try_exists(&apk_path).await.unwrap_or(false) {
            tokio::fs::remove_file(&apk_path)
                .await
                .context("failed to replace previous Android update")?;
        }
        tokio::fs::rename(&temporary_path, &apk_path)
            .await
            .context("failed to finalize Android update")?;
        cleanup_old_android_updates(&cache_dir, &apk_path).await;
        Ok::<_, anyhow::Error>(
            app.state::<UpdateState>()
                .mark_downloaded(&version, apk_path)?,
        )
    }
    .await;

    match result {
        Ok(metadata) => {
            emit_status(app).await;
            Ok(metadata)
        }
        Err(error) => {
            app.state::<UpdateState>().mark_download_failed(&version);
            let _ = tokio::fs::remove_file(&temporary_path).await;
            Err(error.into())
        }
    }
}

#[cfg(target_os = "android")]
pub fn install(app: &AppHandle, version: String) -> Result<()> {
    let path = app.state::<UpdateState>().downloaded_path(&version)?;
    crate::commands::android::open_android_package_installer(&path)
}

#[cfg(target_os = "android")]
pub fn skip(app: &AppHandle, version: String) -> Result<AppUpdateStatus> {
    let patch = serde_json::json!({
        "update": {
            "skippedVersion": version,
        },
    });
    let next = app.state::<SettingsStore>().update(patch)?;
    crate::commands::emit_settings_updated(app, &next);
    app.state::<UpdateState>().set_update(None);

    Ok(app
        .state::<UpdateState>()
        .snapshot(app.package_info().version.to_string()))
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppUpdateStatus {
    pub current_version: String,
    pub update: Option<UpdateMetadata>,
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMetadata {
    pub current_version: String,
    pub version: String,
    pub date: Option<String>,
    pub body: Option<String>,
    pub target: String,
    pub download_url: String,
    pub downloaded: bool,
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDownloadProgress {
    pub downloaded: u64,
    pub total: Option<u64>,
    pub progress: Option<f64>,
}

#[derive(Clone, Copy)]
pub enum CheckMode {
    Manual,
    Auto,
}

#[cfg(not(target_os = "android"))]
impl UpdateState {
    pub fn new() -> Self {
        Self {
            current: Mutex::new(None),
        }
    }

    fn snapshot(&self, current_version: String) -> AppUpdateStatus {
        let update = self.with_current(|current| {
            current.as_ref().map(|pending| {
                let mut metadata = pending.metadata.clone();
                metadata.downloaded = pending.bytes.is_some();
                metadata
            })
        });

        AppUpdateStatus {
            current_version,
            update,
        }
    }

    /// 相同版本、下载地址和签名的候选包保留已下载内容。
    fn set_update(&self, update: Option<PendingUpdate>) {
        self.with_current(|current| {
            if let (Some(existing), Some(next)) = (current.as_ref(), update.as_ref()) {
                if existing.metadata.version == next.metadata.version
                    && existing.metadata.download_url == next.metadata.download_url
                    && existing.update.signature == next.update.signature
                {
                    return;
                }
            }

            *current = update;
        });
    }

    fn mark_downloaded(&self, version: &str, bytes: Vec<u8>) -> Result<UpdateMetadata> {
        self.with_current(|current| {
            let pending = current
                .as_mut()
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("no update is ready")))?;

            if pending.metadata.version != version {
                return Err(AppError::Other(anyhow::anyhow!(
                    "the selected update is no longer current"
                )));
            }

            pending.bytes = Some(bytes);
            pending.metadata.downloaded = true;

            Ok(pending.metadata.clone())
        })
    }

    fn install_downloaded(&self, version: &str) -> Result<()> {
        self.with_current(|current| {
            let pending = current
                .as_mut()
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("no update is ready")))?;

            if pending.metadata.version != version {
                return Err(AppError::Other(anyhow::anyhow!(
                    "the selected update is no longer current"
                )));
            }

            let bytes = pending
                .bytes
                .as_ref()
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("update is not downloaded")))?;

            pending
                .update
                .install(bytes)
                .context("failed to install update")?;

            Ok(())
        })
    }

    fn with_current<R>(&self, f: impl FnOnce(&mut Option<PendingUpdate>) -> R) -> R {
        let mut guard = self.current.lock().unwrap_or_else(|poisoned| {
            log::error!("update state mutex poisoned, recovering");
            poisoned.into_inner()
        });
        f(&mut guard)
    }
}

#[cfg(not(target_os = "android"))]
pub fn init(app: &AppHandle) {
    app.manage(UpdateState::new());
}

#[cfg(not(target_os = "android"))]
pub async fn status(app: &AppHandle) -> AppUpdateStatus {
    app.state::<UpdateState>()
        .snapshot(app.package_info().version.to_string())
}

#[cfg(not(target_os = "android"))]
pub async fn check(app: &AppHandle, mode: CheckMode) -> Result<AppUpdateStatus> {
    if matches!(mode, CheckMode::Auto) && !should_auto_check(app) {
        return Ok(status(app).await);
    }

    let settings = app.state::<SettingsStore>().snapshot();
    let endpoints = update_endpoints()?;
    let updater = app
        .updater_builder()
        .endpoints(endpoints)
        .context("failed to configure update endpoints")?
        .build()
        .context("failed to build updater")?;

    let found = updater
        .check()
        .await
        .context("failed to check for updates")?;
    let state = app.state::<UpdateState>();

    if let Some(update) = found {
        let metadata = metadata_from_update(&update);
        let skipped = settings
            .update
            .skipped_version
            .as_deref()
            .is_some_and(|version| version == metadata.version);

        if skipped {
            state.set_update(None);
        } else {
            state.set_update(Some(PendingUpdate {
                update,
                metadata,
                bytes: None,
            }));
        }
    } else {
        state.set_update(None);
    }

    persist_last_checked_at(app);

    Ok(status(app).await)
}

#[cfg(not(target_os = "android"))]
pub async fn download(app: &AppHandle, version: String) -> Result<UpdateMetadata> {
    let update = {
        let state = app.state::<UpdateState>();
        state.with_current(|current| {
            let pending = current
                .as_ref()
                .ok_or_else(|| AppError::Other(anyhow::anyhow!("no update is ready")))?;

            if pending.metadata.version != version {
                return Err(AppError::Other(anyhow::anyhow!(
                    "the selected update is no longer current"
                )));
            }

            Ok(pending.update.clone())
        })?
    };

    let mut downloaded = 0_u64;
    let app_handle = app.clone();
    let bytes = update
        .download(
            move |chunk_len, total| {
                downloaded = downloaded.saturating_add(chunk_len as u64);
                emit_progress(&app_handle, downloaded, total);
            },
            || {},
        )
        .await
        .context("failed to download update")?;

    app.state::<UpdateState>().mark_downloaded(&version, bytes)
}

#[cfg(not(target_os = "android"))]
pub fn install(app: &AppHandle, version: String) -> Result<()> {
    if cfg!(debug_assertions) {
        let lang = crate::i18n::current_language(app);
        return Err(AppError::Other(anyhow::anyhow!(
            crate::i18n::commands::label(
                lang,
                crate::i18n::commands::Key::UpdateInstallRequiresRelease,
            )
        )));
    }

    log::info!("installing downloaded update {version}");
    app.state::<UpdateState>().install_downloaded(&version)?;
    log::info!("update {version} installed, requesting app restart");
    app.request_restart();

    Ok(())
}

#[cfg(not(target_os = "android"))]
pub fn skip(app: &AppHandle, version: String) -> Result<AppUpdateStatus> {
    let patch = serde_json::json!({
        "update": {
            "skippedVersion": version,
        },
    });
    let next = app.state::<SettingsStore>().update(patch)?;
    crate::commands::emit_settings_updated(app, &next);
    app.state::<UpdateState>().set_update(None);

    Ok(app
        .state::<UpdateState>()
        .snapshot(app.package_info().version.to_string()))
}

pub fn schedule_auto_check(app: &AppHandle) {
    if cfg!(debug_assertions) {
        return;
    }

    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(Duration::from_secs(AUTO_CHECK_INITIAL_DELAY_SECONDS)).await;

        loop {
            let delay = next_auto_check_delay(&handle);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
                continue;
            }

            run_auto_check(&handle).await;
            tokio::time::sleep(Duration::from_secs(AUTO_CHECK_FAILURE_RETRY_SECONDS)).await;
        }
    });
}

async fn run_auto_check(app: &AppHandle) {
    match check(app, CheckMode::Auto).await {
        Ok(status) if status.update.is_some() => {
            #[cfg(not(target_os = "android"))]
            {
                if let Err(err) =
                    crate::window::show_window(app, crate::window::UPDATE_WINDOW_LABEL)
                {
                    log::warn!("show update window after automatic check failed: {err}");
                }
            }
        }
        Ok(_) => {}
        Err(err) => {
            log::warn!("automatic update check failed: {err}");
        }
    }
}

#[cfg(not(target_os = "android"))]
fn metadata_from_update(update: &TauriUpdate) -> UpdateMetadata {
    UpdateMetadata {
        body: update.body.clone(),
        current_version: update.current_version.clone(),
        date: update.date.map(|date| {
            date.format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| date.to_string())
        }),
        download_url: update.download_url.to_string(),
        downloaded: false,
        target: update.target.clone(),
        version: update.version.clone(),
    }
}

fn update_endpoints() -> Result<Vec<Url>> {
    let stable_endpoint =
        std::env::var(STABLE_ENDPOINT_ENV).unwrap_or_else(|_| DEFAULT_STABLE_ENDPOINT.to_owned());
    Ok(vec![parse_endpoint(&stable_endpoint)?])
}

fn parse_endpoint(endpoint: &str) -> Result<Url> {
    endpoint
        .parse::<Url>()
        .map_err(|err| AppError::Other(anyhow::anyhow!("update endpoint is invalid: {err}")))
}

fn should_auto_check(app: &AppHandle) -> bool {
    next_auto_check_delay(app).is_zero()
}

fn next_auto_check_delay(app: &AppHandle) -> Duration {
    let settings = app.state::<SettingsStore>().snapshot();

    next_auto_check_delay_for_settings(&settings.update, Utc::now())
}

fn next_auto_check_delay_for_settings(settings: &UpdateSettings, now: DateTime<Utc>) -> Duration {
    let settings_refresh = Duration::from_secs(AUTO_CHECK_SETTINGS_REFRESH_SECONDS);

    if !settings.auto_check {
        return settings_refresh;
    }

    let Some(last_checked_at) = settings.last_checked_at.as_deref() else {
        return Duration::ZERO;
    };
    let Ok(last_checked_at) = chrono::DateTime::parse_from_rfc3339(last_checked_at) else {
        return Duration::ZERO;
    };

    let elapsed = now
        .signed_duration_since(last_checked_at.with_timezone(&Utc))
        .num_seconds();
    let remaining = frequency_seconds(settings.frequency).saturating_sub(elapsed);
    if remaining <= 0 {
        return Duration::ZERO;
    }

    Duration::from_secs(remaining as u64).min(settings_refresh)
}

fn frequency_seconds(frequency: UpdateFrequency) -> i64 {
    match frequency {
        UpdateFrequency::Daily => 24 * 60 * 60,
        UpdateFrequency::Weekly => 7 * 24 * 60 * 60,
        UpdateFrequency::Monthly => 30 * 24 * 60 * 60,
    }
}

fn persist_last_checked_at(app: &AppHandle) {
    let patch = serde_json::json!({
        "update": {
            "lastCheckedAt": Utc::now().to_rfc3339(),
        },
    });

    match app.state::<SettingsStore>().update(patch) {
        Ok(next) => crate::commands::emit_settings_updated(app, &next),
        Err(err) => log::warn!("persist update check timestamp failed: {err}"),
    }
}

fn emit_progress(app: &AppHandle, downloaded: u64, total: Option<u64>) {
    let progress = total.filter(|value| *value > 0).map(|value| {
        let ratio = downloaded as f64 / value as f64;
        ratio.clamp(0.0, 1.0)
    });

    if let Err(err) = app.emit(
        UPDATE_PROGRESS_EVENT,
        UpdateDownloadProgress {
            downloaded,
            total,
            progress,
        },
    ) {
        log::warn!("emit update progress failed: {err}");
    }
}

#[cfg(target_os = "android")]
fn android_update_target() -> &'static str {
    #[cfg(target_arch = "aarch64")]
    return "android-aarch64";
    #[cfg(target_arch = "arm")]
    return "android-armv7";
    #[cfg(target_arch = "x86")]
    return "android-i686";
    #[cfg(target_arch = "x86_64")]
    return "android-x86_64";
    #[allow(unreachable_code)]
    "android-unsupported"
}

#[cfg(target_os = "android")]
fn start_android_update(app: &AppHandle, version: String) {
    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        match download(&handle, version.clone()).await {
            Ok(_) => {
                if let Err(error) = install(&handle, version.clone()) {
                    log::warn!("open Android package installer for {version} failed: {error}");
                    if let Err(emit_error) = handle.emit(UPDATE_ERROR_EVENT, error.to_string()) {
                        log::warn!("emit Android update error failed: {emit_error}");
                    }
                }
            }
            Err(error) => {
                log::warn!("automatic Android update download {version} failed: {error}");
                if let Err(emit_error) = handle.emit(UPDATE_ERROR_EVENT, error.to_string()) {
                    log::warn!("emit Android update error failed: {emit_error}");
                }
            }
        }
    });
}

#[cfg(target_os = "android")]
async fn cleanup_old_android_updates(cache_dir: &std::path::Path, keep: &std::path::Path) {
    let Ok(mut entries) = tokio::fs::read_dir(cache_dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path == keep {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(ANDROID_UPDATE_FILE_PREFIX) && name.ends_with(".apk") {
            if let Err(error) = tokio::fs::remove_file(&path).await {
                log::warn!(
                    "remove stale Android update {} failed: {error}",
                    path.display()
                );
            }
        }
    }
}

#[cfg(target_os = "android")]
async fn emit_status(app: &AppHandle) {
    if let Err(error) = app.emit(UPDATE_STATUS_EVENT, status(app).await) {
        log::warn!("emit Android update status failed: {error}");
    }
}

#[cfg(all(test, not(target_os = "android")))]
mod tests {
    use super::*;

    fn update_settings(
        auto_check: bool,
        frequency: UpdateFrequency,
        last_checked_at: Option<String>,
    ) -> UpdateSettings {
        UpdateSettings {
            auto_check,
            frequency,
            include_beta: false,
            include_nightly: false,
            last_checked_at,
            skipped_version: None,
        }
    }

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-30T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn auto_check_delay_uses_settings_refresh_when_disabled() {
        let settings = update_settings(false, UpdateFrequency::Daily, None);

        assert_eq!(
            next_auto_check_delay_for_settings(&settings, fixed_now()),
            Duration::from_secs(AUTO_CHECK_SETTINGS_REFRESH_SECONDS)
        );
    }

    #[test]
    fn auto_check_delay_is_due_without_previous_check() {
        let settings = update_settings(true, UpdateFrequency::Daily, None);

        assert_eq!(
            next_auto_check_delay_for_settings(&settings, fixed_now()),
            Duration::ZERO
        );
    }

    #[test]
    fn auto_check_delay_uses_configured_frequency() {
        let now = fixed_now();
        let last_checked_at = (now - chrono::Duration::hours(24)).to_rfc3339();
        let daily = update_settings(true, UpdateFrequency::Daily, Some(last_checked_at.clone()));
        let weekly = update_settings(true, UpdateFrequency::Weekly, Some(last_checked_at));

        assert_eq!(
            next_auto_check_delay_for_settings(&daily, now),
            Duration::ZERO
        );
        assert_eq!(
            next_auto_check_delay_for_settings(&weekly, now),
            Duration::from_secs(AUTO_CHECK_SETTINGS_REFRESH_SECONDS)
        );
    }

    #[test]
    fn auto_check_delay_sleeps_until_due_when_within_refresh_window() {
        let now = fixed_now();
        let last_checked_at = (now - chrono::Duration::minutes(23 * 60 + 30)).to_rfc3339();
        let settings = update_settings(true, UpdateFrequency::Daily, Some(last_checked_at));

        assert_eq!(
            next_auto_check_delay_for_settings(&settings, now),
            Duration::from_secs(30 * 60)
        );
    }

    #[test]
    fn stable_endpoint_matches_bundled_config() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();

        assert_eq!(
            config["plugins"]["updater"]["endpoints"],
            serde_json::json!([DEFAULT_STABLE_ENDPOINT])
        );
        assert_eq!(
            parse_endpoint(DEFAULT_STABLE_ENDPOINT).unwrap().scheme(),
            "https"
        );
    }

    #[test]
    fn android_manifest_accepts_desktop_platforms_without_sha256() {
        let manifest: AndroidUpdateManifest = serde_json::from_value(serde_json::json!({
            "version": "1.2.3",
            "notes": "test",
            "pub_date": "2026-10-25T00:00:00Z",
            "platforms": {
                "windows-x86_64": {
                    "signature": "desktop-signature",
                    "url": "https://example.com/EcoPaste.exe"
                },
                "android-aarch64": {
                    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "url": "https://example.com/EcoPaste.apk"
                }
            }
        }))
        .unwrap();

        assert_eq!(manifest.version, "1.2.3");
        assert_eq!(manifest.notes.as_deref(), Some("test"));
        assert_eq!(manifest.pub_date.as_deref(), Some("2026-10-25T00:00:00Z"));
        assert_eq!(manifest.platforms["windows-x86_64"].sha256, None);
        assert_eq!(
            manifest.platforms["android-aarch64"].sha256.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(
            manifest.platforms["android-aarch64"].url,
            "https://example.com/EcoPaste.apk"
        );
    }
}
