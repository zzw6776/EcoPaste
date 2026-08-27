# EcoPaste Sync Server

EcoPaste 的可独立部署密文同步 Hub，传输层使用 Iroh 1.0/QUIC。服务端只保存：

- 端到端加密后的剪贴板事件；
- 端到端加密后的文件块；
- 设备 Iroh 地址（用于设备优先直连）；
- 设备组鉴权 token 的 BLAKE3 摘要。

服务端不会收到剪贴板明文或设备组内容密钥。局域网同步由客户端之间直接完成；Hub 不在线时，已经配对且能互相发现的设备仍可在局域网工作。

## 本机运行

需要 Rust 1.98：

```bash
cd sync-server
./run-local.sh
```

本机运行使用临时数据目录；停止后会自动删除 SQLite、密文文件和 Iroh 服务端身份。下次启动的 Endpoint ID 会改变，客户端需要重新填写。需要持久化的生产部署应显式传入独立的 `--data-dir`，不要复用本机临时启动配置。

启动日志会输出：

```text
ECOPASTE_SERVER_ENDPOINT_ID=<固定服务端身份>
ECOPASTE_SERVER_DIRECT_ADDRESS=<探测到的地址>
ECOPASTE_SERVER_RELAY_URL=<Iroh relay 地址>
```

部署在公网机器后，客户端应使用输出的 Endpoint ID，并把公网 `IP:端口` 作为 direct address。安全组和主机防火墙必须放行相同的 UDP 端口。本机 Docker 默认使用 UDP 4443。

客户端把这些值分别填入“同步服务 Endpoint ID / 直连地址 / Relay 地址”。配对码会把服务地址连同同步空间一起交给新设备；服务地址为空时只启用局域网同步，云端状态会明确显示为未启用。

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

`docker-up.sh` 使用 Rust 1.98 slim 构建镜像和项目专属的 `ecopaste-sync-slim` Buildx builder。Rust 基础镜像、Cargo registry 和编译结果会在后续构建中复用，不需要每次重新下载和全量编译。脚本在每次退出时把该 builder 的缓存控制在 3 GiB 内并停止 builder；当前热缓存约 2.3 GiB，正常重复构建不会被清除。超过上限时会优先淘汰旧缓存层，之后若重新用到这些层，才需要重新下载或编译。

新镜像成功启动后，脚本会删除被替换的旧 `ecopaste-sync-server:local` 镜像，并清理该服务遗留的悬空镜像。本机容器的 `/data` 使用 tmpfs，容器停止或重建后自动清空，不创建持久化数据卷。如果构建或启动失败，旧镜像会保留，避免失去回退基础。

builder 默认处于停止状态。查看缓存时先运行 `docker buildx inspect ecopaste-sync-slim --bootstrap`，再运行 `docker buildx du --builder ecopaste-sync-slim`，查看后可用 `docker buildx stop ecopaste-sync-slim` 重新停止。确认不再需要加速后，才运行 `docker buildx rm ecopaste-sync-slim` 彻底回收；这会导致下次构建重新下载基础镜像并重新编译依赖。不要使用全局 `docker image prune` 或 `docker builder prune` 代替项目脚本。

如需完全不依赖公共 Iroh relay，可增加环境变量 `ECOPASTE_NO_RELAY=true`；这时只保留 UDP 直连，公网部署必须保证客户端能够访问服务端 UDP 端口。

## 验证

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
| `--no-relay` | `ECOPASTE_NO_RELAY` | `false` | 禁用公共 Iroh relay，只允许直连 |

## 生产数据与备份

显式指定持久化 `data-dir` 的生产部署，运行时数据包括：

```text
data/
  iroh-secret.key
  hub.sqlite3
  blobs/
```

备份时应同时保存这三项。SQLite 与 blob 目录需要来自同一个时间点；恢复后继续使用原 `iroh-secret.key`。
