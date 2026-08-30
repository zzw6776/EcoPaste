# AGENTS.md

> 本文件是本项目 AI 编码工具的**单一真相源**。其它工具入口若存在，只应引用本文件，不要重复维护规则。

EcoPaste 是跨平台剪贴板管理器，采用 Rust-First 的 Tauri 架构。

## 快速原则

- **Rust-First**：业务、系统能力、数据库与持久化优先放 Rust；前端只做展示与交互。
- **仅支持 macOS + Windows + Android**：不要新增 Linux 或 iOS 代码、依赖、构建产物或文档承诺。
- **已发布版本按发布数据处理**：数据结构、配置格式、默认值和 migration 变更必须有明确迁移策略，不再直接覆盖已发布数据契约。
- **每次改动必须升级版本**：每个进入 `master` 的提交都必须提升 `package.json` 版本，并同步更新 `src-tauri/Cargo.toml` 与 `src-tauri/Cargo.lock`；Tauri 桌面包和 Android 包统一使用该版本。
- **AI 默认只编译不打包运行**：完成代码修改后只执行必要的编译检查、静态检查和测试，不构建桌面安装包、Android APK 或 Docker 镜像，不安装、不启动应用或服务；交付时明确列出需要用户重启或重新构建的组件。只有用户明确要求时才执行打包、安装、启动或重启。
- **用户可见结果先核对链路**：修复页面显示或跨端结果前，按实际问题范围核对“数据来源 → 跨端传递 → 状态更新 → 组件渲染”，确认失效层后再修改；某一层取值正确只能证明该层，不能据此判断最终显示已修复。
- **固定 Rust 工具链**：所有本地和 AI 会话都使用仓库根目录 `rust-toolchain.toml` 管理的 rustup 工具链，不使用 Homebrew Rust 或绕过版本文件的 `cargo`；测试 profile 采用低磁盘配置。
- **共享工作区的 WSL 构建隔离**：Windows 与 WSL 可以共同编辑 Windows 磁盘中的源码，但 WSL 的 `node_modules`、`src-tauri/target`、Android 项目 `.gradle`、各模块 `build` 与 `jniLibs` 必须 bind mount 到 `/root/.cache/ecopaste/` 下的 ext4 目录，Windows 使用这些挂载点下方的原生目录；挂载项由 WSL `/etc/fstab` 维护，缺失时先执行 `mount -a`，禁止在未挂载状态下构建或向 Windows 目录写入 WSL 符号链接。只有 `artifacts/` 中的最终产物跨系统共享，Windows 与 WSL 不得同时构建 Android。
- **Rust 构建低磁盘配置**：客户端只生成 macOS/Windows 使用的 `rlib` 与 Android 使用的 `cdylib`，不要恢复仅供 iOS 的 `staticlib`；桌面客户端、Android debug 和同步服务保留自身 `debug = 1` 与增量编译，release 使用 Cargo 默认策略，第三方依赖使用 `debug = 0`，测试使用 `debug = 0` 且关闭增量编译。不要为了省空间无条件删除完整客户端或服务端 `target`、Cargo registry，否则下次会重新编译或下载。
- **本地构建缓存按阈值清理**：`pnpm tauri dev` / `pnpm tauri build` 在启动构建前检查桌面 host 的 dev + release 缓存，总量超过 6 GiB 才清理两个 profile，并保留已有桌面 bundle；Android arm64 打包前检查对应 Cargo target 与项目 Gradle 目录，总量超过 3 GiB 才清理 Android dev/release profile 和 Gradle 产物。未超阈值时保留缓存，构建结束、应用启动、重启和热更新均不触发清理；当前最新版 APK 与 bundle、Cargo registry/git、Gradle 共享依赖缓存始终保留。不得把阈值清理扩大为 `pnpm clean:native`。
- **Android 日常只构建 arm64 release**：本地开发和真机验收默认使用 `pnpm android:build:release`；需要 ADB、WebView 或 JNI 调试时才使用 `pnpm android:build:debug`。两种 APK 成功后均保存在 `artifacts/android/`，新 APK 成功写入后必须删除该目录中的全部旧版本 APK，只保留最新版本；构建前按上述阈值决定是否清理；只有正式多 ABI 发布流程才构建其它 Android targets。
- **Docker 镜像由 CI 构建**：同步服务使用 `sync-server/Cargo.toml` 中的独立版本号；服务端镜像构建输入发生变化时必须同步提升该版本及 `sync-server/Cargo.lock`，推送到 `master` 后由独立 GitHub Docker Image CI 构建并发布 `linux/amd64` 与 `linux/arm64` 镜像。`sync-server/docker-up.sh` 只拉取远端镜像、替换本地容器并清理该服务的旧镜像，不在本机创建 Buildx builder 或 Rust 编译缓存。不要改回本地 `docker compose up --build`，也不要手动执行无过滤条件的 `docker image prune` 或 `docker builder prune`。
- **本地同步服务数据持久化**：本机 Docker 服务的 `/data` 固定使用外部 Docker volume `ecopaste-sync-data`，`sync-server/docker-up.sh` 只替换容器和镜像，不得删除该数据卷；直接运行统一使用 `sync-server/run-local.sh` 的平台固定数据目录。停止或重建服务必须保留 Hub 数据、Iroh 服务端身份和密文文件，清理数据需要用户明确授权。
- **共享依赖缓存保守清理**：默认保留 Cargo registry/git、pnpm store、当前 Gradle wrapper 和 Gradle modules；它们体积较小或跨项目共享，删除会导致重新下载。旧 Gradle 版本可能属于其它项目，未经用户明确授权不得删除。
- **配对方向明确**：二维码生成方是要加入的同步空间基准；扫码设备已属于其它同步空间时，必须让用户显式选择“加入二维码设备”或“保留本机并展示本机二维码”。切换空间时清理旧同步队列和路由但保留本地剪贴板历史，禁止静默覆盖或自动合并两套密钥与事件序列。
- **Android 配对扫码固定使用 Worker**：当前高版本密集配对二维码无法被 Android WebView 原生 `BarcodeDetector` 稳定识别，`qr-scanner` 必须禁用原生检测器并使用其 Worker；调整该策略前必须用真实完整配对二维码完成真机识别验证。
- **局域网自动发现与加入**：复用 Iroh mDNS `UserData` 广播协议版本、设备名称和由组密钥单向派生的匿名空间标识，禁止广播 Token、内容密钥或配对码；申请走独立 `ecopaste/pair/2` ALPN，双方核对六位短码并由已有设备显式批准，60 秒超时且必须限流；批准信息必须经过申请端确认、批准端原子落库和最终完成确认后才结束 Iroh 连接。Android 仅在连接页面或手动刷新时短时持有 `MulticastLock`，后台不可达时回退二维码；局域网同步关闭时不得发现、广播或审批。被删除设备重新批准必须传播恢复记录，不能只删除本机墓碑。
- **同步地址与重连**：Iroh Endpoint ID 是稳定设备身份，直连 IP、端口和 Relay URL 只作动态路由；局域网只允许由局域网发现/配对获得并由 mDNS 刷新的私网直连地址，禁止拨号或接受 Relay/公网路径。云端可独立选择关闭 Relay（默认）、Iroh 免费公共 Relay 或自定义 Relay，自定义认证 Token 必须存入受限身份文件，不能进入公开设置、日志或配对码。启动、窗口回到前台和地址发现变化立即连接，离线按 `2/5/15/30/60/300` 秒退避，禁止恢复高频轮询或把连接超时当作成功周期；全部设备和单设备都必须保留手动重连入口。
- **主动演进当前项目**：实现新能力时以当前代码、产品需求和平台约束为准，把当前仓库作为唯一实现基线。
- **提交工作空间中的有用改动**：不要回滚或覆盖已有改动；需要动到已修改文件时先读清楚。提交时应纳入工作空间内所有有用改动，只排除确认无用的临时文件、生成噪声或用户明确要求保留的改动。

## 技术栈

| 维度 | 选型                                            |
| ---- | ----------------------------------------------- |
| 桌面 | Tauri v2                                        |
| 前端 | React 19 + Ant Design v6 + UnoCSS `presetWind4` |
| 状态 | Valtio（仅 UI 状态与设置镜像）                  |
| 后端 | Rust + sqlx + SQLite                            |
| 构建 | Vite + pnpm                                     |
| 质量 | Biome、rustfmt、clippy、cargo test              |

## 架构边界

**必须在 Rust 实现**

- 剪贴板监听、写回剪贴板、模拟粘贴，以及监听回环抑制。
- 所有数据库读写、SQLite FTS5 搜索、历史记录清理。
- 内容类型识别：URL、email、color、path。
- 窗口定位计算、OS 级键盘钩子、全局快捷键、托盘、自启。
- 图片落盘、缩略图、文件元信息读取、设置项持久化。
- Rust 侧直接展示给用户的短文案（托盘、原生右键菜单、命令返回 toast）走 `i18n/` 模块；日志与内部错误上下文不走这里。

**保留在前端**

- 组件渲染、虚拟滚动、瀑布流、动画、列表选中态。
- 主题视觉应用、CSS 变量注入、前端 i18n 文案渲染（Rust 侧文案见上）。
- HTML sanitize 与预览、RTF 渲染、Markdown 渲染。
- 普通键盘交互；Windows 主窗口收不到键时走 Rust `keyboard/` 事件。

**跨端契约**

- 前端通过 `#[tauri::command]` 调 Rust，Rust 用 `emit` 通知刷新。
- 事件名用 `domain://action`，如 `clipboard://updated`、`settings://updated`、`window://visibility`、`keyboard://nav`。
- 命令名、事件名、channel/storage key 等跨端或多处复用字面量必须集中维护：Rust 模块常量 + `src/constants/` 同步更新。

## 目录约定

```text
src-tauri/
  src/
    commands/   # tauri command 入口，只做校验与转发
    db/         # sqlx 仓储、连接池、模型
    clipboard/  # 剪贴板读写、监听、内容识别
    window/     # 窗口管理、定位、平台特化
    keystroke/  # 模拟粘贴按键注入
    keyboard/   # OS 级键盘钩子（仅 windows）
    mouse/      # 全局鼠标钩子，主窗口失焦隐藏（仅 windows）
    shortcut/   # 全局快捷键
    tray/       # 托盘菜单
    menu/       # 列表项右键菜单（macOS muda / Windows webview 窗）
    drag_out/   # OS 级拖出（文件/图片/文本拖到外部应用）
    backup/     # .ecopastebak 历史备份导出与接收
    i18n/       # Rust 侧用户可见文案（托盘、菜单、命令 toast）
    autostart/  # 开机自启
    settings/   # 设置模型与持久化
    core/       # 错误类型、路径、prevent_default（setup 在 lib.rs）
  migrations/
src/            # 前端 components/pages/stores/hooks/locales/utils
```

## 常用命令

```bash
pnpm install
pnpm tauri dev
pnpm tauri build
pnpm android:build:release
pnpm android:build:debug
pnpm lint
pnpm format
pnpm clean:native
pnpm clean:app-cache

source "$HOME/.cargo/env"
cd src-tauri
cargo fmt
cargo clippy -- -D warnings
cargo test

cd ../sync-server
cargo test --workspace
./docker-up.sh
```

`pnpm clean:app-cache` 只清理 EcoPaste 主包产物并保留第三方依赖，适合手动回收旧 hash；`pnpm clean:native` 会同时清空客户端与同步服务构建产物，只用于明确需要建立干净构建基线时。手动运行任一清理命令前先确认没有 Cargo、Tauri 或 Gradle 构建进程。服务端协议 crate 属于 `sync-server` workspace，不单独生成或清理 `sync-server/protocol/target`。

## Rust 约定

- 命令与仓储函数使用 `async`，返回 `Result<T, AppError>`；`AppError` 序列化为 `{ kind, message }`。
- `message` 写用户可读根因，不加 `"xxx failed: {err}"` 动作前缀；动作上下文由前端 toast label 拼接，技术上下文写日志。
- 错误处理用 `thiserror` 定义错误类型、`anyhow` 做内部传播、`tauri-plugin-log` 记录上下文。
- 数据库使用 Tauri `State<SqlitePool>`；不要每次新建连接。
- Cargo 依赖版本不要写 patch 级完整版本；所有依赖优先写主版本号，如 `"2"`，确需收窄时最多写到 minor，如 `"0.9"`，除非有明确锁定原因。
- SQL 用 `sqlx::query` / `query_as`，不用 `query!` 宏，避免维护离线缓存。
- 已发布版本的 schema 变更必须新增 migration；已发布 migration 不回改。
- 改 schema 时同步检查所有 `SELECT`、`INSERT`、`UPDATE`、`bind`、测试结构体字面量；`query_as` 字段不匹配可能表现为 UI 空结果。
- 表必须有 `created_at` / `updated_at`，类型 `TEXT NOT NULL`；剪贴板 `updated_at` 表示内容重新使用时间，收藏、置顶、备注等元数据更新不要刷新它。
- `commands/` 保持薄层：参数校验 + 调用下层模块，不写业务逻辑。
- 平台代码用 `#[cfg(target_os = "macos")]` / `#[cfg(target_os = "windows")]` 隔离；新增能力两端同步实现，或显式标注 TODO。

## 前端约定

**React 与组件**

- 组件用 `FC<Props>`；函数体内解构 `props`，不要在参数处解构。
- 解构时需要透传剩余字段就用 `...rest` 收尾。
- React 19 优先用 Actions、`use`、`useOptimistic`、ref as prop；不要新增 `forwardRef`。
- JSX 事件回调提取为命名函数；单一动作用动词名，通用事件用 `handleXxx`。
- 箭头函数一律使用 `{}` 和显式 `return`；不要单表达式隐式返回。
- `useEffect` 只写同步副作用；异步初始化用 `useMount` + `useUnmount`，清理句柄用 `useRef`。

**状态、数据与平台 API**

- Valtio 只存 UI 状态和设置镜像；业务数据从 Rust command 拉取，不在前端建数据库副本。
- 异步统一 `async` / `await` + `try` / `catch`；不要 `.then()` / `.catch()` / `.finally()` 链式写法。
- 表达未定义用 `void 0`，不要写 `undefined`。
- 日志统一走 `@/utils/log`，禁止裸 `console.*`。
- 平台与环境判断统一从 `@/utils/is` 引入。
- 当前窗口统一用 `getCurrentWebviewWindow()`，不要用 `getCurrentWindow()`。

**样式与 UI**

- 优先使用 Ant Design v6 组件；prop 命名用 `open` / `checked` / `disabled` / `onClick`。
- 自定义 antd 内部结构优先用组件 `classNames` / `styles` 语义槽位；谨慎使用 `.ant-*` 全局覆盖。
- 样式使用 UnoCSS；条件 className 统一用 `cn from "@/utils/cn"` + 对象语法，不拼模板字符串或 `+`。
- 颜色只能用 antd token 映射类，如 `text-ant-secondary`、`bg-ant-container`、`border-ant-border`；需要新颜色先扩 `src/unocss/presetAntdColors.ts`。
- 普通文本继承全局 `text-ant-text`；次级信息优先 `text-ant-secondary`，更浅层级需有明确设计理由。
- 字号用标准语义字号：`text-xs`、`text-sm`、`text-base`、`text-lg`；不要用 `text-3` / `text-3.5`。
- 尺寸走 wind4 数字制（1 = 4px），如 `p-1.5`、`gap-2`、`rounded-2.5`、`w-36`；不要写任意 px 类或 inline px。
- 主题通过根部 `ConfigProvider` 的 `theme.algorithm` 切换，同时把 `light` / `dark` 类同步到 `<html>`。

**内容与列表**

- i18n 文案必须同步补齐 `zh-CN`（默认）和 `en-US`。
- 列表使用 `react-virtuoso` 虚拟滚动。
- HTML 内容必须经 DOMPurify sanitize 再渲染。

## 通用代码规范

- 非显然函数 / 方法上方写文档注释：TS/JS 用多行 JSDoc，Rust 用连续 `///`；getter/setter、显然一行包装、纯字面量常量可省。
- 优先早返回，避免把主流程包进嵌套 `if`。
- hooks、变量声明、副作用、不同语义阶段和 `return` 前用空行分组。
- 函数体内少写注释；只解释隐藏约束、反直觉行为或规避原因。
- 不写历史残留注释，不引用 TODO 阶段号或外部行号。
- 不做超出当前需求的抽象、兼容垫片或提前优化；React hook / 工具函数遇到真实复杂度再抽象。
- 提交信息用单行 Conventional Commits，如 `feat:`、`fix:`、`refactor:`、`docs:`。

## 外部文档

- Ant Design v6：<https://ant.design/components/overview-cn> · <https://ant.design/docs/react/customize-theme-cn>
- UnoCSS：<https://unocss.dev/> · <https://unocss.dev/presets/wind4>
- Tauri v2：<https://tauri.app/llms-full.txt>
