pub use crate::i18n::keys::SyncKey as Key;

use crate::settings::Language;

/// 返回同步状态中会直接展示给用户的错误根因文案。
pub fn label(lang: Language, key: Key) -> &'static str {
    match lang {
        Language::ZhCN => crate::i18n::zh_cn::sync::label(key),
        Language::EnUS => crate::i18n::en_us::sync::label(key),
    }
}
