# macOS 剪贴板窗口长时间隐藏后显示延迟

> 状态：根因链路于 1.1.128 完成运行时取证，1.1.131 移除已确认的应用内干扰。本文记录测量结果、WebKit 边界、修复约束与验收方法；在正式包完成长时间隐藏后的重复实测前，不宣称系统冷路径已经完全消失。

## 问题描述

macOS 上的剪贴板主窗口在正常使用或连续唤出时较快，但长时间未使用后第一次通过全局快捷键显示可能明显延迟。再次隐藏并立即唤出通常恢复正常。

这类问题容易被误判为 App Nap、快捷键回调、Spaces、窗口动画或前端列表渲染。诊断必须把一次显示拆成“请求进入 → 原生窗口调用 → AppKit 可见 → 事件送达 → 前端同步处理 → WebKit 连续帧”几个阶段，不能只比较用户主观感受。

## 1.1.128 实测

测试环境为 macOS 26.6.2、Apple M3、4K 主显示器。正式版主进程和主 WebContent 在慢请求期间均未重建。

| 阶段 | 长时间隐藏后的 request 14 | 紧接着的 request 15 |
| --- | ---: | ---: |
| Rust 与 AppKit 显示调用完成 | 19 ms | 1 ms |
| AppKit 报告窗口可见 | 80 ms | 27 ms |
| `window://visibility` 送达前端 | 3 ms | 2 ms |
| 前端同步处理 | 8 ms | 2 ms |
| 第一个 `requestAnimationFrame` | 167 ms | 45 ms |
| 连续两个 `requestAnimationFrame` | 229 ms | 54 ms |

后续连续请求总耗时为 92 ms 和 89 ms。慢请求发生前机器已经完成系统唤醒且显示器点亮约 7 分钟，因此它不是一次硬件唤醒耗时。

关键结论是：慢请求中 Rust 原生链路只占 19 ms，主要时间位于 `orderFrontRegardless` 返回后、WebKit 恢复可见并提交首帧之前。

## 已确认的根因链路

### 真实隐藏会让 WebKit 退出可见绘制生命周期

`src-tauri/src/window/macos.rs` 的隐藏路径最终调用 `NSPanel.orderOut:`，显示路径调用 `orderFrontRegardless` 和 `makeKeyWindow`。

WebKit 的 macOS 实现会同时检查以下条件：

- `WKWebView` 必须仍有窗口；
- WebView 及祖先不能隐藏；
- `NSWindow.isVisible` 必须为真；
- 开启遮挡检测时，窗口必须带 `NSWindowOcclusionStateVisible`。

窗口上下屏及遮挡状态变化会触发 `ActivityState::IsVisible` 更新，再进入 `viewIsBecomingVisible` 或 `viewIsBecomingInvisible`。图层提交需要重新调度 `RemoteLayerTreeDrawingAreaProxyMac` 的 DisplayLink 回调。

相关上游实现：

- [PageClientImplMac::isViewVisible](https://github.com/WebKit/WebKit/blob/main/Source/WebKit/UIProcess/mac/PageClientImplMac.mm#L206-L228)
- [WebViewImpl 的窗口上下屏通知](https://github.com/WebKit/WebKit/blob/main/Source/WebKit/UIProcess/mac/WebViewImpl.mm#L2400-L2410)
- [WebPageProxy 的可见状态分派](https://github.com/WebKit/WebKit/blob/main/Source/WebKit/UIProcess/WebPageProxy.cpp#L3686-L3726)
- [RemoteLayerTree 的 DisplayLink 注册](https://github.com/WebKit/WebKit/blob/main/Source/WebKit/UIProcess/RemoteLayerTree/mac/RemoteLayerTreeDrawingAreaProxyMac.mm#L516-L548)

因此，连续唤出较快不是业务代码忽快忽慢，而是 WindowServer、WebKit 图层树和 DisplayLink 仍处于热状态。真实 `orderOut` 后，公开 API 不提供“窗口不可见但始终保留可见绘制状态”的独立开关。

### 全局窗口事件错误唤醒了隐藏预览 WebView

1.1.130 及之前存在两项叠加问题：

1. 主窗口每次显示都会调用 `preview::resume_after_clipboard_show`，它不仅解除预览压制，还会后台执行 `ensure_preview_window`。这与文件顶部“只在预览请求到达时按需建窗”的生命周期约定矛盾。
2. `window://visibility` 与 `window://lifecycle` 携带窗口 label，但 Rust 使用 `AppHandle.emit` 广播。前端只能在收到事件、唤醒 JS 后再根据 label 丢弃无关事件。

Tauri 2.11.5 的 `AppHandle.emit` 会对所有已登记 WebView 执行事件 JS；`emit_to` 才会按目标过滤：

- [Tauri Emitter API](https://github.com/tauri-apps/tauri/blob/tauri-v2.11.5/crates/tauri/src/lib.rs#L934-L989)
- [Tauri 向全部 WebView 注入事件的实现](https://github.com/tauri-apps/tauri/blob/tauri-v2.11.5/crates/tauri/src/manager/mod.rs#L536-L549)

慢请求的系统日志显示，主窗口首帧尚未完成时，隐藏预览 WebContent 被 `runJavaScriptInFrameInScriptWorld` 恢复，随后依次发生：

```text
ProcessDidResume
cancelMarkAllLayersVolatile
unfreezeAllLayerTrees
两次 runJavaScriptInFrameInScriptWorld
PrepareToSuspend
freezeAllLayerTrees
markLayersVolatile 按 20/40/80/160/320/640/1280 ms 退避重试
```

两次 JS 执行在时间与数量上都和主窗口显示路径中的 visibility、lifecycle 两次全局广播吻合；再结合 Tauri 的广播实现，可以确认这些事件会注入隐藏预览 WebView。这不是系统必需工作，而是 EcoPaste 制造的额外 WebKit、GPU 与 WindowServer 竞争。

用户关闭轻量模式时，`DestroyWhenIdle` 计时器不会销毁隐藏预览窗口。于是一次无意的预热会让该 WebContent 长期存在，并在后续每次窗口广播时反复被唤醒。

### 显示事件还触发了非首帧必需 IPC

`SyncStatusIcons` 的源码确认它会在每次主窗口显示时立即调用 `getSyncStatus`。慢请求中，显示事件后观察到两个 Tauri 自定义协议任务，分别耗时约 87 ms 和 102 ms；连续唤出时同一时段的两个任务约为 4 ms 和 17 ms。系统日志不能把两个任务逐一映射到具体 command，因此这些数值只证明首帧阶段确有 IPC 活动，不把它们全部归因于 `getSyncStatus`。

这些异步请求不是 AppKit 变为可见之前延迟的原因，但会与 WebKit 首次图层提交重叠。同步状态不是窗口首帧的前置条件，应在主窗口连续两帧恢复后再读取。列表隐藏期间确有待处理更新时仍可按原逻辑刷新，不能为了性能展示已确认过期的数据。

## 1.1.131 修复

### 窗口事件定向投递

以下带目标 label 的事件统一使用 `emit_to(label, ...)`：

- `window://visibility`
- `window://lifecycle`
- `window://before-destroy`

前端保留 label 校验作为防御，但不再依靠接收方过滤来实现路由。新增窗口专属事件时必须遵守同一规则；只有确实需要所有窗口消费的领域事件才允许使用 `AppHandle.emit`。

### 预览严格按需创建

`resume_after_clipboard_show` 只解除 `PREVIEW_SUPPRESSED`，不创建窗口。第一次真实的空格或悬停预览请求通过 `ensure_preview_window` 建窗；建成后继续沿用现有复用与生命周期策略。

这会把首次预览建窗成本归还给预览功能本身，避免为了一个可能不会发生的操作拖慢每次主窗口显示。

### 非关键状态查询移出首帧

`SyncStatusIcons` 收到主窗口显示事件后等待两个 `requestAnimationFrame`，再通过下一任务执行状态查询。窗口在此期间重新隐藏会取消任务；真实 `sync://updated` 到达时取消延迟并立即刷新，避免展示旧状态。

## 已排除项与不可替代边界

### 不是根因

- 全局快捷键与 Rust 入口：慢请求的主线程排队仅几十微秒。
- 窗口位置恢复：慢请求不足 1 ms。
- React 同步显示处理：慢请求为 8 ms。
- 主 WebContent 重建或当次进程恢复：进程从应用启动起持续存在，慢请求时没有 `ProcessDidResume`。
- Spaces：它影响窗口出现在哪个空间，不改变 WebKit 对 `NSWindow.isVisible` 的判断。
- 窗口动画：关闭动画不能绕过图层树与 DisplayLink 的恢复。

### `backgroundThrottling` 与 `LatencyCritical` 的边界

`backgroundThrottling: disabled` 对应 WebKit 的 inactive scheduling policy，作用是让不在窗口中的 WebView 继续正常调度任务。它不把 `orderOut` 的窗口声明为可见，也不保证其 RemoteLayerTree 和 DisplayLink 常驻。

`NSProcessInfo` 的 `UserInitiatedAllowingIdleSystemSleep | LatencyCritical` 作用于 EcoPaste 进程调度，并允许系统按用户设置睡眠。它不能替 WebKit WebContent 或 WindowServer 保留窗口绘制状态。二者都不能代替本次事件路由和热路径修复。

### `NSGlassEffectView` 不是可见生命周期开关

WebKit 的可见判断读取 WKWebView、NSWindow 和 occlusion，不读取承载它的 `NSGlassEffectView` 或 `NSVisualEffectView` 类型。Liquid Glass 可能增加合成成本，但现有证据不能把它确定为本次冷启动根因。

在移除已确认干扰后，如果正式包仍有不可接受的剩余耗时，可以对两种材质做单变量 A/B；在此之前不得用换材质替代根因修复。

## 不采用的方案

### 永久有序、透明隐藏主窗口

保持窗口 ordered，并通过 `alphaValue=0`、忽略鼠标等方式实现逻辑隐藏，可能避免 WebKit 进入不可见状态。但窗口仍参与 WindowServer 合成与空间管理，可能增加闲置 GPU/内存占用和睡眠前耗电，也容易引入点击、焦点、全屏空间及窗口阴影边界问题。

只有产品明确接受这些副作用并要求进一步压低系统冷路径时，才允许重新评估。不得把它作为无副作用的普通优化。

### 私有 WebKit SPI

关闭窗口遮挡检测的私有 SPI 既不受支持，也不能绕过 `NSWindow.isVisible == false`。它还有系统升级和发布审核风险，不采用。

### 原生占位截图

先显示旧截图、再切换真实 WebView 只能改善主观反馈，会短暂展示过期内容且不能立即交互。它属于感知层规避，不是根因修复。

## 回归约束

- 主窗口显示热路径只保留定位、`orderFront`、key/focus 与目标窗口可见事件。
- 不得在主窗口显示时创建、重配或唤醒其它 WebView。
- 携带窗口 label 且只由该窗口消费的事件必须定向投递。
- 非首帧所需的数据库、同步或更新查询应在首帧后执行；必要数据应优先在隐藏期间准备完成。
- 不得用固定延时掩盖未定位的耗时；诊断继续使用分段时间戳和连续帧指标。
- 不得仅凭连续唤出结果判断长时间隐藏问题已解决。

## 验收方法

正式包至少覆盖以下三组，每组记录原生总耗时、AppKit 可见耗时、首帧和连续两帧耗时：

1. 连续显示/隐藏 5 次，建立热路径基线。
2. 主窗口隐藏 5 分钟后显示 3 次。
3. Mac 完成睡眠唤醒、桌面稳定 1 分钟后显示 3 次。

同时检查统一日志：

- 主窗口显示期间不应再因为 `window://visibility` 或 `window://lifecycle` 出现隐藏预览进程的 `ProcessDidResume`。
- 未使用预览前不应存在由主窗口显示创建的预览 WebContent。
- 同步状态 IPC 应发生在主窗口连续帧指标之后。
- 正常隐藏仍应触发主窗口 `viewIsBecomingInvisible`；这是保留 `orderOut` 语义后的预期系统行为。

完成标准是消除应用自身的跨 WebView 唤醒和首帧竞争，并显著收敛长时间隐藏后的尾延迟。只要继续使用真正的 `orderOut`，就不能承诺完全消除 macOS/WebKit 自身的冷显示下限；若产品要求绝对无冷路径，只能重新评估永久有序窗口或原生 AppKit UI，并单独接受其成本。
