# EcoPaste Sync Server

EcoPaste 的可独立部署密文同步 Hub，传输层使用 Iroh 1.0/QUIC。服务端只保存：

- 端到端加密后的剪贴板事件；
- 端到端加密后的文件块；
- 设备 Iroh 地址（用于设备优先直连）；
- 设备组鉴权 token 的 BLAKE3 摘要。

服务端不会收到剪贴板明文或设备组内容密钥。局域网同步由客户端之间直接完成；Hub 不在线时，已经配对且能互相发现的设备仍可在局域网工作。

Hub 是有界的加密同步缓冲，不是永久备份。默认每个同步空间仅限制 10 GiB 密文数据，时间和原始事件数量限制均为 `0`（不限制）；后台按小批次裁剪未受保护的旧事件，并在最后一个引用消失后回收对应密文文件。收藏、置顶记录及其附件豁免自动淘汰，但仍计入空间用量。新设备首次同步会在 Hub 当前保留范围内继续应用本机的历史保留时长与最大条数设置，不会先下载完整云端历史再本地删除。

## 本机运行

需要 Rust 1.98：

```bash
cd sync-server
./run-local.sh
```

本机运行使用固定数据目录，保存 SQLite、密文文件和 Iroh 服务端身份；停止并重新启动后 Endpoint ID 保持不变。macOS 默认使用 `~/Library/Application Support/EcoPaste Sync Server`，Windows 默认使用 `%LOCALAPPDATA%/EcoPaste Sync Server`，也可以通过 `ECOPASTE_LOCAL_DATA_DIR` 指定其它目录。

启动日志会输出：

```text
ECOPASTE_SERVER_ENDPOINT_ID=<固定服务端身份>
ECOPASTE_SERVER_DIRECT_ADDRESS=<探测到的地址>
ECOPASTE_SERVER_RELAY_URL=<Iroh relay 地址>
```

部署在公网机器后，客户端应使用输出的 Endpoint ID，并把公网 `IP:端口` 作为 direct address。安全组和主机防火墙必须放行相同的 UDP 端口。本机 Docker 默认把容器的 UDP 4443 映射到宿主机 UDP 4443。

客户端把 Endpoint ID 和公网 `IP:端口` 填入云端 Hub 配置。云端 Relay 默认关闭：需要穿透或直连不可达时可开启“免费公共 Relay”（无需 Token），自建 Relay 则选择“自定义 Relay”并按服务要求填写可选 Bearer Token。Relay 只用于云端 Hub，局域网同步始终只接受私网直连路径，并通过 mDNS 刷新动态地址。配对码会把 Hub 地址和 Relay 模式连同同步空间交给新设备，但不会携带自定义 Relay Token；服务地址为空时只启用局域网同步。

## 同步唤醒与耗电

- 本地出现新剪贴板事件时立即唤醒同步，不扫描整张历史表。
- 局域网入站事件由 Iroh 连接直接推送，设备离线只记为跳过，不把卡片标成失败。
- 云端使用 `Watch` 长轮询等待游标变化，服务端有新密文事件时立即唤醒客户端；连接每 60 秒续订一次。
- 连续失败按 2 秒到 5 分钟指数退避，正常状态仅保留 10 分钟一次的安全对账。

因此常态不再执行原来的 3 秒全量同步轮询；云端不可用时局域网链路、历史记录和手动同步状态仍独立工作。

## Docker

```bash
cd sync-server
./docker-up.sh
docker compose logs -f ecopaste-sync
```

`docker-up.sh` 不在本机编译服务，而是从 GitHub Container Registry 拉取公开镜像 `ghcr.io/zzw6776/ecopaste-sync-server:latest`，因此本地不再保留 Rust Buildx 编译缓存。同步服务使用 `sync-server/Cargo.toml` 中的独立版本号；升级该版本并推送到 `master` 时，GitHub Docker Image CI 自动构建并发布 `linux/amd64` 与 `linux/arm64` 镜像。稳定版同时更新版本标签和 `latest`，预发布版只更新版本标签。需要固定版本时可在启动前设置 `ECOPASTE_SYNC_IMAGE_TAG`，例如 `ECOPASTE_SYNC_IMAGE_TAG=0.1.1 ./docker-up.sh`。

新镜像成功启动后，脚本会删除被替换的旧镜像，并清理该服务遗留的悬空镜像。本机容器的 `/data` 固定挂载外部 Docker volume `ecopaste-sync-data`，停止、重建容器或清理旧镜像都不会删除 Hub 数据和服务端身份；首次运行时脚本会自动创建该卷。如果拉取或启动失败，旧镜像会保留，避免失去回退基础。

Compose 使用 `4443:4443/udp` 发布 Hub 端口。Linux 上客户端可通过宿主机地址访问；macOS 虚拟化 Docker 运行时还需要正确转发 UDP/QUIC，不能只根据容器启动成功判断 `127.0.0.1:4443` 或 Mac 局域网地址已经可达。部署后应分别完成 Mac 和 Android 的 Iroh 握手验证；运行时无法转发 UDP 时启用云端公共 Relay，或把服务原生运行在宿主机。

GHCR 发布使用工作流自带的 `GITHUB_TOKEN` 和仓库 `packages: write` 权限，不需要配置长期仓库密钥。公开镜像可匿名拉取；若将来改为私有镜像，服务器应使用只有 `read:packages` 的拉取令牌。GitHub Actions 使用独立远端缓存，本机无需创建 Buildx builder；也不要为了清理该服务而执行无过滤条件的全局 `docker image prune` 或 `docker builder prune`。

如需完全不依赖公共 Iroh relay，可增加环境变量 `ECOPASTE_NO_RELAY=true`；这时只保留 UDP 直连，公网部署必须保证客户端能够访问服务端 UDP 端口。

## 验证

0.1.20 新增可延后上传的附件引用协议：客户端 1.1.162 起会将来源图标标记为可选附件，先同步剪贴板内容，再异步补齐图标；图片和文件附件仍须先上传。图标引用继续跟随事件保留和回收，已有数据库无需迁移。新客户端连接 0.1.17～0.1.19 服务端时会先上传图标以兼容旧校验规则；旧客户端仍应升级，单独更新服务端无法识别其未标记的可选图标。

协议单元测试与真实 Iroh 端到端测试（包含建组、鉴权、事件推拉、设备路由和文件上传下载）：

```bash
cargo test --manifest-path protocol/Cargo.toml
cargo test
```

端到端测试使用本机临时 UDP 端口，不依赖云服务或公共 relay。

## 配置

| CLI | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--data-dir` | `ECOPASTE_DATA_DIR` | `./data` | SQLite、Iroh 身份和密文文件目录 |
| `--bind` | `ECOPASTE_BIND` | `0.0.0.0:44820` | Iroh QUIC UDP 监听地址 |
| `--max-blob-bytes` | `ECOPASTE_MAX_BLOB_BYTES` | `2147483648` | 单个密文文件上限 |
| `--max-group-bytes` | `ECOPASTE_MAX_GROUP_BYTES` | `10737418240` | 每个同步空间的密文事件与文件总配额；`0` 表示不限 |
| `--event-retention-days` | `ECOPASTE_EVENT_RETENTION_DAYS` | `0` | 云端事件保留天数；`0` 表示不按时间裁剪 |
| `--max-events-per-group` | `ECOPASTE_MAX_EVENTS_PER_GROUP` | `0` | 每个同步空间保留的原始事件数；`0` 表示不限 |
| `--no-relay` | `ECOPASTE_NO_RELAY` | `false` | 禁用公共 Iroh relay，只允许直连 |

客户端 1.1.169 起要求服务端 0.1.22（协议 14），以保证收藏和置顶保护。服务端只接收由空间密钥派生的匿名记录标识及各设备的保护快照，无法据此读取正文或解密附件；同一设备的旧快照不能覆盖新快照。任一未移除设备仍报告保护时继续保留，取消收藏、置顶后随各设备同步一致解除保护，移除设备会删除它的保护快照。

已有数据库通过新增迁移保留原有密文和身份；尚未补齐匿名标识的旧事件先保守保留。新版客户端分批读取这些事件的密文、验证并合并收藏和置顶状态后补齐匿名标识与附件引用，不下载附件；该空间全部旧事件补齐前暂停自动清理，避免较后的收藏状态尚未读到就淘汰较早的内容。客户端使用连续的有限分页优先完成补齐，再开始上传，避免空间已满时补齐任务被上传失败退避拖慢。旧客户端仍可连接新服务端，但其未登记的事件会继续保留，建议空间内设备均升级。保护内容或尚未登记的旧数据占满配额时，不删除这些内容，新上传按现有退避策略等待；受保护内容不会让空间上限自动扩大。已有环境变量显式配置的非零时间和数量限制继续生效，对应限制仅清理未受保护且标识已登记的事件。

云端历史查询默认只展示内容；客户端核对旧同步结果时可显式包含收藏、置顶、备注等状态事件，并按消息大小分页，不改变正常同步游标。

Hub 定时执行保留策略清理；上传因空间配额不足被拒绝时，还会按本次上传所需容量触发异步低水位回收。客户端保持本地操作可用，并按现有云端退避策略自动重试。

## 生产数据与备份

显式指定持久化 `data-dir` 的生产部署，运行时数据包括：

```text
data/
  iroh-secret.key
  hub.sqlite3
  blobs/
```

备份时应同时保存这三项。SQLite 与 blob 目录需要来自同一个时间点；恢复后继续使用原 `iroh-secret.key`。
