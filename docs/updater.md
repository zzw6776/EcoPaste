# 桌面应用内更新

macOS 和 Windows 复用 Tauri updater。正式版按偏好设置中的周期检查更新，发现新版后提示；用户点击“下载”和“安装”，应用完成安装并重新启动，无需拖拽 DMG 或手动打开安装包。自动检查默认关闭，Android 暂不支持应用内更新。

更新窗口支持从顶部空白区域拖动；仅在 EcoPaste 唤出或处于前台时显示在其上方，切换到其他应用后取消置顶。窗口按需创建后保留单个实例；隐藏和重新唤起会保留下载进度及已下载包，需要刷新版本信息时点击“重新检查”。

## 发布与签名

- 更新来源固定为本仓库 GitHub Releases 的 `releases/latest/download/latest.json`，只接收正式发布版。Beta、nightly 开关暂不展示，旧配置中的字段保留，不再影响正式版查询。
- Release CI 生成并签署 macOS `.app.tar.gz` 和 Windows NSIS `.exe` 更新包，由 Tauri Action 上传并合并 `latest.json`。三个桌面构建依次执行，避免同时覆盖清单；发布前必须确认清单包含 macOS ARM64、macOS Intel、Windows x64。
- 更新签名与 macOS 系统代码签名相互独立。CI 使用 GitHub Secret `TAURI_SIGNING_PRIVATE_KEY`；公钥保存在 `src-tauri/tauri.conf.json`。当前更新私钥没有密码，CI 将 `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` 设为空字符串。
- 本机私钥位于 `~/.tauri/ecopaste/updater.key`，权限为 `0600`，目录权限为 `0700`。请妥善备份，后续发布复用同一密钥；私钥不得提交到仓库或写入日志。
- 本地手动打包时，设置 `TAURI_SIGNING_PRIVATE_KEY` 为上述私钥文件路径，并将 `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` 设为空字符串。本地打包不自动发布 GitHub Release。
- `ECOPASTE_UPDATE_ENDPOINT` 可覆盖查询地址，用于定向测试。下载包仍必须通过客户端公钥验签。

## 首次升级与验收

原有版本使用旧更新地址和公钥，首次迁移需要从本仓库 GitHub 手动下载并安装包含新配置的版本。之后发布更高版本，即可从应用内更新。

`pnpm tauri dev` 等 debug 构建不会自动检查更新；允许手动检查和下载，但 Rust 安装入口拒绝安装。该入口不更新电脑上另一份正式应用。完整验收应使用正常安装的较旧版本，更新到新发布版本，并确认重启后的版本号、历史记录和设置。
