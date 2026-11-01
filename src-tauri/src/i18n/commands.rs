pub use crate::i18n::keys::CommandKey as Key;

use crate::settings::Language;

/// 返回 Tauri 命令错误中会展示给用户的根因文案。
pub fn label(lang: Language, key: Key) -> &'static str {
    match lang {
        Language::ZhCN => crate::i18n::zh_cn::commands::label(key),
        Language::EnUS => crate::i18n::en_us::commands::label(key),
    }
}

/// Formats share feedback in the user's configured app language, including partial failures.
#[cfg(any(target_os = "android", test))]
pub fn share_result(lang: Language, saved: usize, skipped: usize, failed: usize) -> String {
    if saved + skipped + failed == 0 {
        return label(lang, Key::AndroidShareEmpty).to_owned();
    }
    label(lang, Key::AndroidShareResult)
        .replace("{saved}", &saved.to_string())
        .replace("{skipped}", &skipped.to_string())
        .replace("{failed}", &failed.to_string())
}
