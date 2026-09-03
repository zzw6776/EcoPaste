/// 初始化 prevent-default 插件，禁用 webview 内置的浏览器默认行为
/// （右键菜单、F5 刷新、Ctrl+P 打印、Ctrl+F 查找等），让应用更像原生窗口。
///
/// debug 构建保留 devtools / reload / 右键菜单等开发所需行为；Android release
/// 保留长按文本操作菜单，其余 release 平台禁用全部默认快捷操作。
pub fn init() -> tauri::plugin::TauriPlugin<tauri::Wry> {
    #[cfg(debug_assertions)]
    {
        tauri_plugin_prevent_default::debug()
    }

    #[cfg(all(not(debug_assertions), target_os = "android"))]
    {
        let flags = tauri_plugin_prevent_default::Flags::all()
            .difference(tauri_plugin_prevent_default::Flags::CONTEXT_MENU);

        tauri_plugin_prevent_default::with_flags(flags)
    }

    #[cfg(all(not(debug_assertions), not(target_os = "android")))]
    tauri_plugin_prevent_default::init()
}
