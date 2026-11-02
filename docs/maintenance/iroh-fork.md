# Iroh fork 维护

客户端与同步服务端使用 https://github.com/zzw6776/iroh 的 `ecopaste` 分支，
Cargo manifest 和 lockfile 固定到同一个完整提交 SHA，不直接跟随移动分支。
Iroh、iroh-base、iroh-dns、iroh-relay 必须一起更新，避免混用不同来源的公共类型。

fork 基于 Iroh 1.4.1，回补上游 PR #4645。noq-udp 在发送 GSO 包遇到 EIO/EINVAL
时会降低 socket 的分段上限；补丁让已有发送器实时读取这个上限，随后由 QUIC
普通重传恢复，不依靠应用识别 VPN 或强制断线重连。Android 不再统一关闭 GSO。

fork 的 `Sync Upstream Stable` 每天检查上游正式版并创建更新 PR，保留补丁与测试。
必需的 `Fork validation` 检查通过后自动合并；补丁冲突、测试失败或跨大版本时保留
原有版本。自动更新维护的是 fork，本项目仍使用已验证的固定提交，更新依赖时需
同步修改两个 workspace 的 Cargo manifest 与 lockfile，并完成客户端、Android 与
同步服务端的必要回归。无需切回官方仓库。

自动化实现、上游版本记录、补丁归属及验证命令见 fork 的 `ecopaste/README.md`。
