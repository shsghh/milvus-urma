# hnsw_urma 接入 URMA 版 Milvus：对接工作说明

> 代码基线：`D:\retrival\boostdb-ci`，HEAD `4aff52e`。
> 读者：需要维护/扩展这条链路的开发人员。
> 范围：**只讲“对接做了什么”**。hnsw_urma 的算法本身（图遍历、candidate list、ef/topk）不在这里展开。
>
> 前提很重要：目标不是“裸 Milvus”，而是**已经由 `UrmaManager` 等适配好 URMA 基础通信的 Milvus**。这次工作没有重造 URMA 通信层，而是把 hnsw_urma 接到这套已有能力上，并补齐它缺的“语义/生命周期/数据面装配”。

---

## 0. 一句话

把 hnsw_urma 从“独立 MPI 程序”改造成“跑在 QueryNode 里的库”：复用它已经有的算法和 URMA 数据面，接入 URMA 版 Milvus 已有的 `UrmaManager` 资源目录与 native handle，补齐 **shard 语义、句柄注入、加载/查询/结果映射、距离下推** 这几处缺口。算法核心不改，Milvus 的存储/WAL/协议不改。

---

## 1. 对接的起点和终点

| 维度 | 裸机 hnsw_urma | URMA 版 Milvus | 对接后 |
|---|---|---|---|
| 进程形态 | 独立 binary（`umdk_runner` / benchmark） | QueryNode 进程 | 算法以库的形式跑在 QN 进程里 |
| 分片单位 | MPI rank | shard（图分区）+ segment（Milvus 存储单位） | 一个 QN 持有一个 shard；segment 的 index 文件承载 triple |
| 索引来源 | 本地 triple 目录 | MinIO 对象 / 本地 `seedDir` | `UBHNSWIndex` 加载 triple，交给单例 |
| 拓扑发现 | MPI `AllGather` 交换描述符 | 无 MPI；`UrmaManager` 的 etcd 资源目录 + LinkCache | 用 UrmaManager 发布/订阅 shard 资源 |
| 通信资源 | 算法自己建 context/队列/jetty | `UrmaManager` 建 context 和 link | 算法 **adopt** 控制面 context，自建 lane 队列/注册段 |
| query 驱动 | `main()` 里的固定 query 列表 | delegator 收到 proxy 的 `SearchRequest` | `SearchEntry` + P2P forward + `force_main` |
| 结果出口 | benchmark 打印 | `SearchResultData` / `SearchResult` | 全局 node id → seg_offset / PK |
| 距离计算 | 算法自带 RPC 下推 | 需要 mailbox/角色/线程配合 | role 2/3/4 + `RpcDistanceOracle`/`RpcServerRuntime` |

### 1.1 URMA 版 Milvus 已经提供了什么

这些是**对接的前提，不是本次工作**：

- `UrmaManager::Init`：进程唯一 `urma_context`、本地 JFC/JFR/JFS/jetty；
- `RegisterLocalResource`：`urma_register_seg` + `urma_get_seg_ctx` + `urma_get_rjetty`；
- `ActivateRemoteResource`：`urma_import_jetty` + `urma_import_seg`；
- etcd 资源目录：`ResourceRecord`、session lease、version/generation；
- `LinkCache`：按 `(owner, segment, role)` 单飞激活；
- `storage_c.cpp` 的 C ABI、QN 的 `initUrma`/`startUrmaDiscovery`；
- QueryNode 的会话、segment 加载、delegator 搜索入口。

### 1.2 它没有提供、需要对接补的

- 全局图的 **shard 语义**（哪个 shard、entry point、layer-0/vector 内存在哪）；
- 发布端要暴露的 **算法内存区域**（graph/vectors/RPC mailbox）；
- 控制面 native handle → 算法 backend 的 **注入通道**；
- Milvus 的 **segment 加载 / query 入口 / 结果语义** 与算法的映射；
- 距离下推所需的 **role 2/3/4、mailbox、线程** 接线。

**对接工作 = 把上面 5 类缺口补齐，同时尽量不改算法核心、不改 Milvus 存储/协议。**

---

## 2. 工作总览

```
┌──────────────────────────────────────┐
│           hnsw_urma 算法本体           │
│  HNSW 状态机 / scheduler / readers    │
└───────────────▲──────────────────────┘
                │ C ABI / 直接调用
┌───────────────┴──────────────────────┐
│           对接层（本次工作）            │
│  UBHNSWIndex / MilvusUbRuntime        │
│  milvus_distributed_runtime / ub_c_api│
└───────▲───────────────────▲──────────┘
        │                   │
┌───────┴────────┐   ┌──────┴─────────────────┐
│  Milvus 原生    │   │ UrmaManager 控制面      │
│  load / search │   │ register/import/etcd   │
└────────────────┘   └────────────────────────┘
```

| # | 工作项 | 解决的差距 | 主要产出 |
|---|---|---|---|
| 1 | 进程与入口 | 独立 binary → QN 进程 | C ABI + CGO + 生命周期挂载 |
| 2 | 索引加载 | triple → Milvus index 管线 | `UBHNSWIndex::LoadV2Index` |
| 3 | 多 segment 聚合 | N 个 segment → 一张全局图 | `MilvusUbRuntime` 单例 |
| 4 | 拓扑发现 | MPI → etcd 资源目录 | 发布/激活/bootstrap |
| 5 | handle 桥 | native 指针 → 算法 backend | `ExportPeerHandles` → `UbImportPeerHandles` → `ImportExternal` |
| 6 | 数据面装配 | 控制面 context/link → 算法的 lane/mailbox | 6 个注册段、duplex jetty、RPC mailbox |
| 7 | query 驱动 | main loop → Milvus 请求 | `SearchEntry` / forward / `force_main` |
| 8 | 结果语义 | 全局 id → seg_offset/PK | `use_offset_as_id_` |
| 9 | 距离下推 | 算法 RPC ↔ Milvus mailbox | role 2/3/4 + RPC 运行时 |
| 10 | 配置/部署 | 裸机参数 → Milvus 配置 | `UbSetConfig`、`seedDir/shardIds`、部署脚本 |

---

## 3. 逐项工作

### 3.1 进程与入口：从 `main()` 到 QN 进程

**差距**：hnsw_urma 是独立程序，自己 `main`、自己初始化；Milvus 里它必须作为库跑在 QueryNode 进程内，由 QN 的生命周期驱动。

**做法**：

1. 把 `urma_runtime/` 作为源码 vendor 进 `internal/core/src/index/ub_hnsw/urma_runtime/`，编进 `milvus_index`；
2. 去掉 `main` 依赖，暴露 C ABI（`hnsw_urma/ub_c_api.h`）：
   - `UbLoadShard`、`UbSeedFromDir(WithShards)`、`UbSearch`、`UbSearchEntry`、`UbSearchEntryAsMain`；
   - `UbImportPeerHandles`、`UbGetShardRegions`、`UbGetEntryPoint/EntryShard`、`UbValidateShardLayout`；
   - `UbEnsureRpcMailbox`、`UbGetRpcMailboxRegions`；
   - `UbEnableDistributedSearch`、`UbSetConfig`、`UbSetUrmaDevice`。
3. Go 侧 CGO 包装：`internal/core/src/index/ub_hnsw/ubcgo/ub_cgo.go`；
4. `initcore` 提供 Go 绑定，QN 在 `Init/Register/Shutdown` 调用；
5. MPI 控制代码保留但不用；`DetectPlacement()` 在 Milvus 构建里是 stub，CPU 来自 `allowed_cpus`。

**结果**：算法不再有“进程入口”，只暴露“加载 / 配置 / 搜索 / 句柄注入 / enable”几个动作。

---

### 3.2 索引加载：triple → Milvus 的 index 管线

**差距**：算法只认本地目录（`std::ifstream`）；Milvus 的索引加载给的是 `index_files`（对象存储 key），或者根本不走 segment index（`seedDir`）。

**做法**：新增 `UBHNSWIndex : VectorIndex`（`index/ub_hnsw/ub_hnsw_index.{h,cpp}`），并在 `IndexFactory::CreateVectorIndex` 里用一个 `if (index_type == "UB_HNSW")` 分派，故意用 knowhere 不认识的名字绕开 `UseDiskLoad/FeatureCheck`。

两条加载路径：

| 路径 | 触发 | 做法 |
|---|---|---|
| A. `seedDir` | QN `Init` → `InitUbRuntime` → `UbSeedFromDir/WithShards` | 直接读本地 triple，`LoadIndexForShard` 物化，`LoadShard` 进单例 |
| B. segment-index | QN `LoadSegment` → `UBHNSWIndex::Load(config)` | `StageFileToDisk` 把 MinIO 对象落到 `/tmp/ub_urma_<shard>_<pid>/`，再 `LoadIndexForShard` |

两条路径都做 C4 校验（`sum(local_count)==ntotal`、本 shard 唯一命中），然后把 `LoadedIndex` 交给单例。

**`ub_inject` 的边界**：它只写 etcd 的 `field-index`/`segment-index`（`IndexFileKeys`、`NumRows`、`Finished`），**不上传 MinIO、不落本地盘**。所以它只服务路径 B 的“元数据”，路径 B 的文件搬运仍由 `StageFileToDisk` 做。

**结果**：Milvus 能像加载普通索引一样加载 `UB_HNSW`；算法拿到 `LocalShard`。

---

### 3.3 多 segment 聚合：N 个 segment → 一张全局图

**差距**：Milvus 按 segment 加载，一个 collection 有 8 个 segment；算法要的是**一张全局图**（meta + shard map + upper + 本地 shard）。每个 segment 都会触发一次 `Load`。

**做法**：进程级单例 `MilvusUbRuntime`（`urma_runtime/src/milvus_ub_runtime.cpp`）：

- 第一个 `LoadShard` 播种 `meta_` / `shard_map_` / `upper_`；
- 后续 shard 校验 `meta` 一致、`shard_map` 的 ntotal/ranges 一致，只追加 `local_shard`；
- 同一 shard 重复加载幂等；
- 新 shard 会让已构建的 reader/oracle 失效，下次 Search 重建；
- 暴露 `UbGetShardRegions` / `UbGetEntryPoint` / `UbGetEntryShard` / `UbValidateShardLayout` 给发布/校验用。

**结果**：一个 QN 上多个 segment 最终只对应“一个 shard 的一份 LocalShard”，全局状态只有一份。

---

### 3.4 拓扑发现：MPI → UrmaManager 的 etcd 资源目录

**差距**：裸机靠 MPI `AllGather` 交换 segment/jetty 描述符；Milvus 的 QN 之间没有 MPI 关系，只有 etcd/session。

**做法**：把“谁发布了哪个 shard 的哪块内存”放进 etcd，复用控制面的 `ResourceRecord` + `LinkCache`。

**发布侧**（`QueryNode.publishUbShardResources`）：

```
UbShardRegions() → 每个本地 shard 的 graph/vector 注册内存
UbGetEntryPoint()/UbGetEntryShard() → entry vertex / entry shard
RegisterAndPublishUrmaGraphResource：
    topology (role 0)、vectors (role 1)
    1-shard worker 额外发布 distance-tasks/results/notify (role 2/3/4)
```

**激活侧**（`prepareUbSearch`）：

```
Snapshot 资源目录
  → FilterUbGraphRecords（GlobalGraphID=="ub-hnsw" && epoch==1）
  → 先 UbValidateShardLayout（用本地 global.meta 校验 shard/entry 同源）
  → 跳过自己
  → LinkCache.Activate(record)
       → UrmaManager::ActivateRemoteResource（import jetty + seg）
       → ExportPeerHandles
       → UbImportPeerHandles
```

**启动 bootstrap**（`QueryNode.Register`）：

- 起一个 goroutine，每秒 Snapshot，直到看到 7 个 peer 各自都有 5 个 role（最多 120s）；
- 然后 `prepareUbSearch` + `UbEnableDistributedSearch`；
- **search 路径上不再做 etcd/prepare**。

**结果**：MPI 被 etcd 目录 + lease/version + LinkCache 替代；不需要改算法的搜索逻辑。

---

### 3.5 handle 桥：UrmaManager 的 native 指针 → 算法 backend

**差距**：控制面 import 出来的是 `urma_target_jetty_t*` / `urma_target_seg_t*`，是**进程内地址**，不能序列化；算法 backend 需要的是自己的 `UrmaResourceHandle`。

**做法**（三段）：

1. **控制面借出**：`UrmaManager::ExportPeerHandles(owner, segment, role)` 返回两个 `void*`；
   - RM 下 `*target_jetty` 是 `UrmaPeerEndpointInfo*`（完整 `rjetty_descriptor` + `jetty_token`）；
   - `*target_segment` 是 `urma_target_seg_t*`；
   - 明确 **caller 不拥有**，link 释放后失效。
2. **对接层暂存**：`UbImportPeerHandles(shard, role, jetty, seg)` → `MilvusUbRuntime::peer_handles_[(shard,role)]`。
3. **算法 backend 记账**：
   - segment：`ImportExternal(kImportedSegment, ptr)`，只登记已有 native 指针，不重复 import；
   - endpoint：用 `rjetty_descriptor` + token 走 `CreatePeerResource(kImportedEndpoint)`，由 backend 自己 `urma_import_jetty`（因为要绑到自己的 lane jetty）。

**角色映射**：

| role | 用途 | 算法侧 |
|---|---|---|
| 0 | topology | `kLayer0Graph` READ |
| 1 | vectors | `kVectors`（pull 回退/本地 distance） |
| 2 | distance-tasks | `kRequests` + `kRequestWrite` |
| 3 | distance-results | `kResponses` + `kResponseWrite` |
| 4 | distance-notify | `kNotify`（只有段，无 endpoint） |

**结果**：控制面负责“把远端内存变成可用 handle”，算法负责“拿 handle 发 WR”；所有权始终在控制面。

---

### 3.6 数据面装配：控制面 context/link → 算法的 lane/队列/mailbox

**差距**：算法裸机自己建 context、队列、jetty；Milvus 里这些要么由控制面提供（context），要么必须和 Milvus 的 link 对齐（jetty/segment），不能各建一套。

**做法**（`milvus_distributed_runtime.cpp`）：

1. **共享 context**：`backend_options.external_context = UrmaManager::GetNativeContext()`；backend 跳过 `urma_init`，直接 adopt。否则控制面 import 的 segment 和算法的 JFS 不在同一 context，WR 必失败。
2. **传输模式**：`bonding_dev_0` + `kCtp` + `configure_bonding=true`（duplex jetty）。
3. **6 个本地注册段**：
   - `layer0_graph`、`vectors`、`rpc_requests`、`rpc_responses`、`rpc_notify`、`read_scratch`；
   - `UrmaResourceBundle::CreateLocal` 统一 `urma_register_seg`，bundle 负责生命周期。
4. **每 lane 的发送资源**：duplex 下建 `kSendJetty`（wrap target JFR），而不是裸 `kSendJfs`。
5. **对端导入**：每个 peer 导入 5 个 segment（graph/vectors/tasks/results/notify）+ 3 个 endpoint（read/requestWrite/responseWrite），按 lane 绑定到 engine。
6. **RPC mailbox**：复用 `EnsureRpcMailbox` 创建并已发布的那一份（不能在这里新建第二份，否则 peer 写进没人 poll 的内存）。
7. **线程组**：control / search / distance / read_progress / rpc_progress。

**一个具体修复**：mailbox 段在 `UrmaManager::RegisterLocalResource` 里的 access 从 `WRITE` 放宽到 `READ|WRITE`，否则 `urma_register_seg` 返回 nullptr。

**结果**：算法在 Milvus 里拥有完整的数据面，但根 context 和远端资源来自控制面。

---

### 3.7 query 驱动与主 rank：main loop → Milvus 请求

**差距**：算法裸机由 `main` 准备 query 列表；Milvus 由 delegator 收请求，而且要决定“哪个 QN 执行”。

**做法**：

1. **查询入口**：`ub.RunSearch` / `RunSearchAsWorker` 解析 topk/ef/query，逐 query 调 `UbSearchEntry` / `UbSearchEntryAsMain`（nq=1）。
2. **主 rank**：
   - `SearchEntry(req_id)` 仍保留 `req_id % shard_count` 路由（兼容旧拓扑）；
   - P2P forward 路径用 `force_main=true`，跳过 ownership 和 dedupe：SN 选了我，所有 query 都由我执行。
3. **P2P forward**（`querynodev2/ub/ub_forward.go`）：
   - SN 内嵌 QN（seed 8 shard）用 round-robin 选 worker；
   - `worker.SearchSegments(gRPC)`；
   - worker 的 delegator 检测到 `TrySearch`，走 `RunSearchAsWorker`。
4. **启动 bootstrap**：见 §3.4；worker 即使不收 search，也必须在启动时把 RPC server 拉起。
5. **Go 侧并发**：`UbGoConcurrentQueries`（默认 1）控制每请求的 query 并发；每个 query 一次 C ABI。

**结果**：query 的“谁执行”从算法的 `req_id % S` 变成 Milvus 的 forward 决策；算法只负责“被选中后跑完整全局图”。

---

### 3.8 结果语义：全局 node id → Milvus 的 seg_offset / PK

**差距**：算法返回的是**全局 node id**；Milvus 原生假设 `seg_offset` 必须小于 segment 行数，并且要去 PK 列查主键。对接后的 segment 只有 placeholder 行，没有真实 PK。

**做法**：

1. `UBHNSWIndex::Query` 把结果写进 `seg_offsets_`，并置 `use_offset_as_id_ = true`；
2. `ReduceHelper::FilterInvalidSearchResult` 跳过 `offset < segment_row_count` 校验，只校验非负；
3. `FillPrimaryKeys` 直接把 `seg_offsets_` 拷成 `primary_keys_`（要求 INT64）；
4. `FillTargetEntry` 直接返回，不查 placeholder 行；
5. 走 delegator 快路径时，直接在 Go 侧组 `SearchResultData`，`Topks` 压缩（非 main 的 query 不贡献行）。

**结果**：算法返回的全局 id 可以直接作为 Milvus 的主键参与后续流程；不需要为 UB 段准备真实数据/PK 列。

---

### 3.9 距离下推：算法 RPC ↔ Milvus mailbox

**差距**：算法裸机有 distance 下推（mailbox + task/result 队列），但 Milvus 侧没有发布这些内存、没有线程、没有和 UrmaManager 的角色模型对齐。

**做法**：

1. **角色扩展**：`UrmaManager`/`ResourceRecord` 增加 `distance-notify`（role 4），`distance-tasks`/`distance-results` 真正使用；access 改为 `READ|WRITE`。
2. **mailbox 创建**：`MilvusUbRuntime::EnsureRpcMailbox` 在发布前创建 3 段（requests/responses/notify），`UbGetRpcMailboxRegions` 暴露注册内存。
3. **发布**：1-shard worker 在 `publishUbShardResources` 里发布 role 2/3/4；SN 不发布（避免和真正 owner 撞 key）。
4. **导入**：主 QN 在 enable 时为每个 peer 导入 role 2/3/4 的 segment 和 kRequestWrite/kResponseWrite endpoint。
5. **客户端**：`RpcDistanceOracle` + `RpcClientOwner` + `RpcTransportOwner`：
   - 批量任务 → `PostWriteNotifies`（payload → 对端 kRequests，notify → 对端 kNotify）；
   - 轮询完成、读响应、唤醒 search task。
6. **服务端**：`RpcServerRuntime`：
   - `ProgressServiceIngress` 观察 notify；
   - `PopService` 读任务；
   - `ComputeRemoteDistanceTask` 读本地 vectors 算距离；
   - `PublishResult` 写响应 + notify 回请求方。
7. **策略/回退**：默认 `pushdown`；`UB_DISTANCE_POLICY` 可覆盖；role 缺失或 `UB_DISABLE_RPC_DISTANCE=1` 时用 `PullVectorDistanceOracle`。

**结果**：向量不再逐条 READ，距离算在数据所在 QN；图邻居仍然 READ。

---

### 3.10 配置、部署与运维

**做法**：

- `UbSetConfig` 增加 `max_active_queries`、`read_jfs_depth`；Go 侧 `InitUbRuntime` 从 paramtable 读；
- 新增 `queryNode.ub.goConcurrentQueries`、`maxActiveQueries`、`readJfsDepth`；
- 部署用 `queryNode.ub.seedDir` + `shardIds` 指定每个 QN 的 shard；
- 启动等待 7 peer × 5 role；
- 调试开关：`UB_DISTANCE_POLICY`、`UB_DISABLE_RPC_DISTANCE`、`UB_SELFRREAD_SN`、`UB_LANE_*` 等（部分只是诊断）。

**结果**：同一套 binary 可以在伪分布式（全量 seed）和真分布式（1 shard/QN）之间切换。

---

## 4. 与 UrmaManager 的接口清单

| UrmaManager 能力 | 对接层怎么用 | 是否改动 |
|---|---|---|
| `Init` / `GetNativeContext` | 算法 backend adopt context | 复用 |
| `RegisterLocalResource` | 发布 graph/vectors/mailbox；role 0..4 | 扩展到 5 个 role；mailbox access 放宽 |
| `ActivateRemoteResource` | 每个 `(peer, role)` 建 link、import jetty/seg | 复用；role 4 |
| `ExportPeerHandles` | 借出 `void*`；RM 下返回完整 rjetty + token | payload 从 24B jfr 改成完整 rjetty |
| `DeactivateRemoteResource` | link 失效/卸载时释放 | 复用 |
| `ResourceRecord` / etcd 目录 | 发布/订阅 shard 资源 | 增加 role 4；distance-* 真正使用 |
| `LinkCache` | 单飞激活、按 version/generation 更新 | 复用 |

**生命周期约定**：

```
Register: publish（控制面 register seg + etcd）
   ↓
bootstrap: activate（控制面 import）+ UbImportPeerHandles（暂存）
   ↓
enable: ImportExternal / import_jetty（算法 backend 记账）
   ↓
search: 只使用 handle，不再 import
   ↓
unload/shutdown: Deactivate → RemovePeerHandles
```

**关键边界**：handle 所有权始终在 UrmaManager；对接层只借、只暂存、只使用。

---

## 5. 对 hnsw_urma 的改动边界

| 文件/模块 | 改动 | 是否算法核心 |
|---|---|---|
| `urma_runtime/src/milvus_ub_runtime.cpp` | 新增：单例、加载聚合、SearchEntry、mailbox、句柄注入 | 否（Milvus 驱动） |
| `urma_runtime/src/milvus_distributed_runtime.cpp` | 新增：backend 装配、导入、RPC、线程组 | 否（Milvus 驱动） |
| `ub_c_api.{h,cpp}` | 新增：C ABI | 否（接口层） |
| `transport/urma/urma_transport.cpp` | 增加 `external_context`、`ImportExternal`、`kSendJetty`、`PostWriteNotifies` | 边缘（传输适配） |
| `transport/urma_resources.*` | 增加 `duplex_jetty`、`kSendJetty` | 边缘 |
| `transport/read_dispatcher.*` | 用外部 handle 的 READ 分发 | 边缘 |
| `search/remote_io.*` | `PeerSegmentTable` + prepared wait | 边缘 |
| HNSW 状态机 / scheduler / candidate list | **未改** | 是 |

**原则**：算法核心（`search.cpp` 的推进逻辑、`scheduler`、`thread_group`）不动；只加“Milvus 驱动文件”和传输层的 hook。

---

## 6. 对 Milvus 的改动边界

| 文件/模块 | 改动 |
|---|---|
| `index/IndexFactory.cpp` | 增加 `UB_HNSW` 分派 |
| `index/ub_hnsw/ub_hnsw_index.*` | 新增 `VectorIndex` 子类（Load/Query/Count） |
| `common/QueryResult.h` | 增加 `use_offset_as_id_` |
| `segcore/reduce/Reduce.cpp` | 跳过 placeholder 行数校验 |
| `segcore/SegmentInterface.cpp` | `FillPrimaryKeys`/`FillTargetEntry` 适配 |
| `querynodev2/delegator/delegator.go` | 插入 UB fast path + forward |
| `querynodev2/server.go` | 发布/激活/bootstrap/forward wire |
| `querynodev2/ub/*`、`ub_forward*.go` | 新增 UB 搜索、转发、worker 路径 |
| `util/initcore/*` | CGO 绑定、配置、mailbox |
| `pkg/util/paramtable` | 新增 `queryNode.ub.*` |
| `configs/milvus.yaml`、部署脚本 | seedDir/shardIds/线程/调试开关 |

**没有改**：storage/WAL、proxy、querycoord、segment 数据格式、Milvus 协议。

---

## 7. 一次搜索的跨层路径

```
proxy
 └─ vchannel leader（SN 内嵌 QN）
      └─ delegator.Search
           ├─ ub.TrySearch
           ├─ ub.TryForward（SN）
           │    ├─ RoundRobinSelector 选 worker
           │    └─ worker.SearchSegments(gRPC)
           │         └─ worker delegator.Search
           │              └─ ub.RunSearchAsWorker
           │                   └─ UbSearchEntryAsMain(force_main)
           │                        └─ MilvusUbRuntime::SearchEntry
           │                             ├─ FIFO/准入/slot
           │                             ├─ HnswSearchTask::Step
           │                             │    ├─ 本地 node → LocalShard
           │                             │    ├─ 远端邻居 → READ
           │                             │    └─ 距离 → HybridDistanceOracle
           │                             │         ├─ pushdown → RpcDistanceOracle
           │                             │         │    └─ WRITE_NOTIFY → peer mailbox
           │                             │         └─ pull → READ 向量本地算
           │                             └─ 返回全局 id + 距离
           └─ 结果原路返回 proxy
```

---

## 8. 落地清单

**新增（对接层）**

- `index/ub_hnsw/ub_hnsw_index.{h,cpp}`
- `index/ub_hnsw/ubcgo/ub_cgo.go`
- `urma_runtime/include/hnsw_urma/{milvus_ub_runtime.hpp, milvus_ub_runtime_impl.hpp, ub_c_api.h}`
- `urma_runtime/src/{milvus_ub_runtime.cpp, milvus_distributed_runtime.cpp, ub_c_api.cpp}`
- `urma_runtime/src/transport/read_dispatcher.{hpp,cpp}`
- `querynodev2/ub/{ub_search.go, ub_prepare.go, ub_forward.go, ub_forward_wire.go}`
- `querynodev2/ub_forward_adapter.go`
- `cmd/tools/ub_inject/main.go`、`cmd/tools/ub_probe/main.go`

**修改（Milvus）**

- `index/IndexFactory.cpp`、`index/CMakeLists.txt`
- `common/QueryResult.h`、`segcore/reduce/Reduce.cpp`、`segcore/SegmentInterface.cpp`
- `querynodev2/delegator/delegator.go`、`server.go`、`services.go`、`cluster/worker.go`、`local_worker.go`
- `util/initcore/{init_core.go, ub_config.go}`
- `pkg/util/paramtable/component_param.go`
- `configs/milvus.yaml`、部署脚本

**修改（UrmaManager/控制面）**

- `storage/UrmaManager.{h,cpp}`：role 4、完整 rjetty、mailbox access
- `storage/storage_c.{h,cpp}`：对应 C ABI

**未改**

- hnsw_urma 的 HNSW 状态机、scheduler、candidate list
- Milvus 的 storage/WAL、proxy、querycoord、协议

---

## 9. 已知限制与后续工作

| 项 | 现状 | 建议 |
|---|---|---|
| `bonding_dev_0` / CTP / duplex | 硬编码 | 提成 paramtable |
| mailbox 布局 | `EnsureRpcMailbox` 硬编码 8/4096/4/4/8 | 与 RuntimeConfig 统一，或写进 ResourceRecord 校验 |
| distance-* 发布 | 只允许 1-shard QN | 拓扑变化（一 QN 多 shard）需重新设计 key |
| SN 与 worker 重复发布 topology/vectors | 两条记录，后激活覆盖 | 固定优先级或按 owner 版本校验 |
| forward | 纯 round-robin，不看负载/健康 | load-aware selector |
| `UbGoConcurrentQueries` | 默认 1 | 按 maxActive 调整 |
| 每 query 新建 `QueryScratch` | 1M 图约 1MB/query 分配+清零 | 按 `(ef, topk)` 池化 |
| 每 query 诊断日志 | `[UB SE]`、`[UB Timing]`、step 日志 | 默认关闭/采样 |
| 图 READ 批量 | `post_batch=8` | 用 `read_jfs_depth`，允许混合 peer |
| 资源身份 | graphID 恒 `"ub-hnsw"`，无 collectionID | 带 collectionID/epoch |
| `UbShutdown` | 未从 Go 调用 | 明确 context 删除责任，先 Shutdown 再 Clean |
| `UrmaPeerEndpointInfo` 注释 | 仍残留 JFR 说法 | 同步更新 |
