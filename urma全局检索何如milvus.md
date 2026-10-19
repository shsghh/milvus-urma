这个工程主要是为了将hnsw_urma这个全局分布式检索算法合入milvus中，首先介绍一下全局分布式算法。
hnsw_urma：D:\retrival\hnsw_urma
milvus-3.0.0：D:\retrival\boostdb-ci
# 1.全局分布式检索算法
这个算法本身就是利用鲲鹏950的UB互联环境，借助高带宽、低时延的UB链路进行全局分布式检索算法。全局算法与milvus、faiss等分片算法最大的不同，就是图索引是连续的，虽然每个rank上都是一部分id，以gist 1M 960dim数据集为例，假设有8rank，那么每个rank就是125k，即
```
rank0 0-125k
rank1 125k-250k
...
rank7 875k-1000k
```
## 全局分布式算法与分片算法的不同
### 邻居范围不同。
这个id分配本身和分片没有什么不同，不同之处在于任意的id的范围。在分片的算法中，任一id的邻居列表的邻居id范围都是本地分片的，不会存在跨分片的边，但是全局分布式算法不同，它的邻居id可能在于其他分片上，也就是存在大量跨节点的边。
### 结果返回不同。
分片算法就是各rank收到同样的query，假设1000个，就是1000个query同时分给8个rank，每个rank1个分片，独立检索自己的分片索引，然后将结果reduce，即需要1000 * topk * 8个结果，然后reduce到1000 * topk个。而全局分布式检索，则是将1000个query均分到8个rank，每个只负责125个query，并行search，那么对于一个query来说，它被分配到哪个rank，哪个就是主rank，需要进行candidate list的推进，所以最终只需要125* topk * 8个，并且不需要reduce。
### 检索过程不同。
分片检索就是每个分片调用标准的算法，比如hnsw，而全局分布式算法则不同，做了一些适配。对于一个query，它被分配到某个rank，这个rank就会从入口点开始，每个rank都有全局图的上层数据，即入口是本地，检索到L0层之后，算法发生了变化。首先，邻居列表可能不在本地，因为分布式算法就是全局的，所以可能candidate list中待展开的id就是不在该rank的范围，需要去利用UB能力获取邻居列表；其次，邻居列表中id不全在该rank上，分为2种情况，在本地rank和不在本地rank。对于不在本地rank的，就把距离计算任务推送到对应的id所在的rank，推送完后，再处理本地rank，本地rank都计算完了，回头获取推送的远端距离计算结果。
### 线程分工不同。
上面说了，全局分布式检索需要借助UB能力，并且存在距离计算下推，就存在多个线程。首先search线程不变，就是维护candidate list推进，然后继续进行。此外，全局检索需要2类URMA线程，负责URMA的通信，这个是UB能力引入的。算法层面，最重要的就是距离计算线程，负责给其它rank提供距离计算服务。可能有一个主线程，负责给search线程分片query，结果返回等工作。

全局分布式检索算法主要是hnsw_urma，我们的任务就是将这个接入milvus，然后跑起来，这个demo在裸机950上是可以直接跑的，并且性能和功能都完整。

# 2.当前对接设计
裸机算法是MPI的，直接使用rank，而milvus search是使用QN，所以，QN就是要对标rank。每个QN负责一个分片，search的时候进行candidate list推进。所以需要8个QN，每个QN load一个索引分片，共同组成全局索引。

裸机算法是借助MPI进行URMA的handle注册和登记，但在milvus中，QN独立无MPI关联，就需要借助etcd机制，进行URMA的注册。这个部分主要是借助QN上线的机制，配合独一无二的数据信息，collection-channel-replication-segment，就借助这些，注册独一无二的名称，然后同步到etcd，就能在milvus中将同一个索引即collection的QN建立链接。然后检索的时候，再进行相互识别一次，激活链接，就能借助URMA进行search。

# 3.当前的问题
当前QN和SN一直难以分离，所有的检索信息都在SN的日志里，你能搞清楚为什么吗?
另外，URMA的read巨慢，需要400ms左右，hnsw_urma的检索，完整的query包括数千个通信，P99时延也就只有33ms左右。

# 4. 新的评判（代码走读结论）

> 结论先行：**上面两个"问题"不是两个 bug，而是同一个架构事实的两种表现。**
> Milvus 3.0 把 search 的入口（channel leader / shard delegator）绑定在 streamingnode 内嵌的
> QueryNode 上；普通 `milvus run querynode` 永远拿不到 channel，也就永远收不到 search。
> 于是当前部署实际是"1 个计算节点 + N 个远端内存节点"，而 400ms 不是 URMA 设备慢，
> 是这条 pull 数据面把"每 query 几百次小消息"放大成"每 query 1.3 万次单向量 READ"
> 之后排队的结果。继续调 URMA 驱动不会回到 33ms；要改的是数据面本身。

## 4.1 问题一：为什么 QN 和 SN 分不开（已确认，不是部署写错）

三条代码事实，按证据链排列：

1. **`run streamingnode` 必然同时拉起一个 QueryNode。**
   `cmd/milvus/util.go:138-141`：`case typeutil.StreamingNodeRole:` 里先
   `sessionutil.EnableEmbededQueryNodeLabel()`，再同时置 `EnableStreamingNode = true` 和
   `EnableQueryNode = true`。即同一个进程、同一个 nodeID 上跑着 SN + 内嵌 QN，
   label 为 `STREAMING-EMBEDDED`（`internal/util/sessionutil/session_util.go:77-80`）。
   所以"QN 与 SN 难以分离"在 3.0 是**设计**，不是你们部署漏了参数。

2. **channel（也就是 search 入口）只分配给内嵌 QN。**
   `internal/querycoordv2/assign/assign_policy_components.go:109-121`：channel 分配的
   `FilterNodes` 第一句就是 `nodes = filterSQNIfStreamingServiceEnabled(nodes)`；
   而 `filterSQNIfStreamingServiceEnabled`
   （`internal/querycoordv2/assign/streaming_query_node_channel_helper.go:60-79`）只保留
   `snmanager.GetStreamingQueryNodeIDs()` 里的节点——就是各 SN 的内嵌 QN。
   其余节点会被 `mlog.Warn("unexpected streaming querynode found when enable streaming service")`
   打出来，然后被丢弃。`cmd/main.go:38` 无条件开了 streaming，所以这条过滤永远生效。
   如果集群里一个 SN 都没有，候选集为空 → channel 永远分配不出去 → collection 一直 load 不完成。

3. **内嵌 QN 反过来不被允许承载 segment。**
   `internal/querycoordv2/meta/resource_manager.go:624-628`
   （`if nodeInfo == nil || nodeInfo.IsEmbeddedQueryNodeInStreamingNode() { return }`）
   与 `internal/querycoordv2/ops_services.go:361-364`（内嵌 QN 不能 Resume）。
   也就是说：**channel/检索在 SN 进程，segment/数据在普通 QN**，二者是硬性分工。

由此解释你们看到的现象，并且是必然的：

- 部署脚本里起 1 个 SN + 1 个"main QN" + 8 个 worker QN 时，**只有 SN 的内嵌 QN 是
  SQN，所有 vchannel 的 leadership 都落在它上面**，`delegator.Search` →
  `ub.TrySearch` → `ub.RunSearch`（`internal/querynodev2/delegator/delegator.go:561-562`）
  只可能在 SN 进程里执行，`UB fast path search done` 这类日志只会出现在
  `/tmp/inst2-streamingnode.log`。8 个 worker QN 收到的是 0 个 search。
- `internal/util/initcore/ub_config.go:91-94` 的注释其实已经写明了这一点：
  "lets the delegator QN (e.g. the embedded QN inside streamingnode, which never loads
  segments itself) serve the UB fast path"。
- `deploy_inst2_true_dist.sh:27-34` 里那句 "SN embeds the main QN that runs UB fast path
  dispatch" 只对了一半：SN 确实内嵌 QN，但它的作用不是"dispatch"，而是**唯一的 search
  执行者**；dispatch 发生在 proxy → vchannel leader 那一跳。

**所以"8 个 QN = 8 个 rank"在当前部署里根本没成立**：实际是 1 个 rank 承担 100% 的
candidate-list 推进，另外 7 份算力只贡献"内存被远端读"。这也解释了为什么 `reqID % 8`
的主 rank 轮转看起来"生效"了——因为那个 SN QN 在伪分布式下 seed 了全部 8 个 shard，
`MilvusUbRuntime::SearchEntry` 的 `owns` 检查
（`urma_runtime/src/milvus_ub_runtime.cpp:300-318`）对任何 target 都返回 true，
于是每个 query 都由它一个人跑完整全局搜索。

## 4.2 问题二：400ms 不是"URMA read 慢"，是排队

先把量级摆出来（数据取自 `hnsw_urma/docs/v3-current-performance-analysis.md`
的 8-rank / ef 剖面）：

| 指标 | 裸机 hnsw_urma | 现在这条 Milvus 数据面 |
|---|---|---|
| 每 query 远端通信次数 | 约 264 次 RPC operation（distance 下推） | 约 14,744 次 **URMA READ**（每个候选向量一次） |
| 每 query 远端 payload | response 约 160 B/task，几十 KB 量级 | 960 dim × 4 B × 14,744 ≈ **56 MB** |
| 距离计算位置 | 数据所在 rank（distance worker） | 请求方本地（pull 回来自己算） |
| 单次物理 read | 9.65 µs | 同一张卡的 READ，本身也是 10 µs 量级 |

1.3 万次 READ 不是"通信"，是"逐条取数"。`1.3万 × 30µs ≈ 390ms`，与 400ms 同量级——
这是目前**唯一能自然解释这个数量级**的项（设备 RTT 只有 ~10µs，再怎么抖动也凑不出 400ms）：
**单次 READ 并没有变慢，是被放大成 1.3 万次、并且串在一条 lane 的队列里。**

不过要分清两个都能凑出几百毫秒的候选，别把它们混成一个结论：

| 候选 | 量级 | 判别方法 |
|---|---|---|
| pull 数据面排队（本节 1–5 条） | ~390ms | 看 `read ops/query` 是否 ~1.3 万、`submit→completion` 分位数是否远大于 `post→completion`；`post→completion` 若仍在 10µs 量级即证实是排队 |
| 每 query 17 次 etcd 往返（第 6 条） | 17ms（本机 etcd）/ 100ms+（跨机 + 抖动） | 在 `prepareUbSearch` 前后打点，或临时把 Snapshot 换成缓存再测一次 |

两者都要打，但**先打掉哪个都不影响结论**：即便 etcd 那 17 次全砍掉，pull 数据面的 1.3 万次
READ 仍在，P99 回不到 33ms。

放大链路（全部可核对）：

1. `milvus_distributed_runtime.cpp:706-714` 用的是 `PullVectorDistanceOracle`，
   而不是裸机的 `RpcDistanceOracle`；`:1007-1012` 与 `:1071-1076` 里 distance / rpc_progress
   两个回调**只是 `sleep_for(10ms)` 的空转桩**，也就是说 **距离下推服务在 Milvus 里根本没启动**。
2. `remote_io.cpp:1180-1209` `PrepareForWait` 对 `ids` 里**每一个 id 各发一个 READ**
   （`operation->reads.resize(ids.size())`，循环 `dispatcher->PrepareForWait(...)`）；
   `:1270-1340` 的 `Submit` 同样是一 id 一 READ。
3. `read_dispatcher.cpp:1293-1354` 的 `Progress()` 每轮：从 slot 0 开始扫描，**只取一个
   `(peer_rank, remote_segment)` 批次**，且 `post_operations_.size() < post_batch_` 就停。
   `post_batch_` 在 `milvus_distributed_runtime.cpp:676-681` 被传成 `requested.wr_list_max`，
   而 `wr_list_max` 默认 **8**（`runtime/config.hpp:90`）。
   即：**每条 read lane 每轮最多发 8 个 READ**，8 个 peer 的请求在同一个 lane 上还要互相排队
   （head-of-line blocking），而 JFS depth 默认给到 1024 —— 硬件并发被软件压到 1/128。
4. `profile_.latency_nanoseconds`（`read_dispatcher.cpp:1387`）统计的是
   **submit → completion**，包含在 submit 队列里等待的时间。所以仪表上看到的
   "read latency 400ms" 正是"排队深度 × 吞吐上限"的体现，而不是设备 RTT。
5. 额外叠加：`internal/querynodev2/ub/ub_search.go:208-235` 是 **for 循环逐 query 调一次
   `UbSearchEntry`（nq=1）并同步等待**。nq=100 的请求就是 100 次串行全局搜索，
   C++ 侧 `max_active_queries` 还是硬编码 16（`config.hpp:55`，无 paramtable 项），
   66 个 search worker 根本用不上。
6. 再叠加：`internal/querynodev2/server.go:1126-1149` 的 `prepareUbSearch` **每次 search**
   都做一次 etcd `Snapshot`（`resource_directory.go:510-530`：1 次 prefix Get）
   + 每条记录一次 `TimeToLive`（`:618-630`）。8 shard × 2 role = 16 条记录
   → **每 query 17 次 etcd 往返**，全在检索关键路径上。

还有一个自伤：伪分布式下 SN QN 已经 seed 了全部 8 个 shard，但
`milvus_distributed_runtime.cpp:734-751` 只要 peer handles 齐全就优先选 URMA 读——
**自己本地就有的数据反而绕远路**，把 7/8 的本地内存访问变成远端 READ。

## 4.3 对原设计的三处纠正

| 原判断 | 纠正 |
|---|---|
| "QN 对标 rank，要 8 个 QN，每个 load 一个分片" | rank 的载体是 **SN 内嵌 QN**（channel leader），不是 `run querynode`。要 8 个 rank 就要 **8 个 streamingnode**；`run querynode` 的数量只决定数据/segment 放在哪。 |
| "全局检索不需要 reduce，只需 125×topk×8 个结果" | proxy 仍然会把一次 search 发给**所有** vchannel leader 并 reduce（`ub_search.go` 的 `IsTopkReduce=false` 只是跳过二次 topk）。现在的 `reqID%S` 轮转 + `topks=0 / id=-1 / dist=+inf` 占位（`ub_search.go:221-235`）是在既有 reduce 框架里"打洞"，代价是 **8 倍的 proxy→QN 派发 + 一次空结果合并**。要真正省掉 reduce，应在 proxy 侧做 query 级分片（一个 query 只发给一个 leader），而不是让 QN 广播后丢弃 7/8。 |
| "用 collection-channel-replication-segment 在 etcd 注册，检索时再识别一次" | 注册思路对，**粒度错了**。不该在每次 search 上做 Snapshot+TTL 校验，应当用已有的 `Watch`（`resource_directory.go:532`）维护内存快照，把激活/校验放在 load/unload 与 link 失效事件上。 |

另外两个正确性风险，按严重程度记在这里：

- `reqID % shard_count` 的主 rank 轮转（`milvus_ub_runtime.cpp:306`）假设
  **"每个 shard 的 leader 都会收到每个请求"**。一旦 pchannel 与 vchannel 不是一一对应
  （`rootCoord.dmlChannelNum` 默认 16，collection 8 个 shard），就会有两个 vchannel 落到同一个
  SN、另一个 SN 空转；空转那些 shard 的 query 会**静默返回空结果**（`is_main=false` 直接
  `Status::Ok()`，`ub_search.go` 再补 `-1/+inf` 占位）。
- `EnableDistributedSearch` 用 `world_size = shard_map_->ranges().size()`、
  `local_rank = min(loaded_shard_ids_)`（`milvus_distributed_runtime.cpp:113-124`），
  这只在"shard 数 == 参与节点数 == 编号连续"时成立；换布局（例如 4+4 两机、或某 QN seed 2 片）
  会静默错位，不会报错。

## 4.4 解决方案（先摘果子、再动手术）

### P0：让部署形状先正确（1 天内可验证，无需改代码）

把"8 个 rank"落到 **8 个 streamingnode**，每个内嵌 QN 各 seed 1 个 shard；
`run querynode` 只保留为 segment/数据宿主（也各 seed 1 片，用于发布 graph+vector 资源）。

```bash
# 8 个 SN 进程，每个 1 个 pchannel、1 个 UB shard
for s in 0 1 2 3 4 5 6 7; do
  METRICS_PORT=$((9100+s)) \
  MILVUS_CONF_querynodeUbSeedDir=/home/s00800245/index/gist_8_urma \
  MILVUS_CONF_querynodeUbShardIds=$s \
  MILVUS_CONF_querynodeport=$((21300+s)) \
  nohup $MILVUS_HOME/bin/milvus run streamingnode --run-with-subprocess \
    > /tmp/inst2-sn_$s.log 2>&1 &
done
```

配套：`rootCoord.dmlChannelNum` 设成 8（= shard 数 = SN 数），collection 建 8 shard、1 replica，
然后**用 metrics / leaderview 核对**每个 SN 各占 1 个 vchannel：
`GetChannelJSON` 里 `channel_node_infos` 的 leader 应均匀分布到 8 个 SN nodeID，而不是全在 9101。
验收：8 个 `inst2-sn_*.log` 里各自出现属于自己的 `UB SearchEntry: main`，且各进程
`nq` 之和等于客户端 nq。

同时把每进程线程数写死，别用 auto（96 核 auto 会给 66 search + 16 distance + 8 reserve，
×9 个进程 = 700+ 线程挤同一份 cpuset）：

```yaml
queryNode:
  ub:
    threads: { search: 8, distance: 0, readProgress: 8, rpcProgress: 0 }
    progressMode: busy
    allowedCpusSource: cgroup
```

### P1：摘掉两处"每 query 一次"的开销（Go 侧小改，可单测）

1. **etcd 快照缓存**：`prepareUbSearch` 改成读本地快照（`Watch` 维护，带 revision 与
   lease 过期时间），激活只在快照变化 / link 失效时执行；`TimeToLive` 从"每条记录每次 search"
   改成"后台节流校验"。预计每 query 省 17 次 etcd 往返。
2. **nq 批量提交**：`ub.RunSearch` 把 nq 行一次性投进 C++ FIFO、一次等全部 completion，
   而不是 `for i := range queries { UbSearchEntry(..., nq=1) }`；
   `max_active_queries` 提到 paramtable（`queryNode.ub.maxActiveQueries`）。nq=100 时这是 100× → 1×。

### P2：把 400ms 真正打下来（二选一，都需要服务器实测）

- **方案 A（改动最小，伪分布式立刻受益）**：`full_local_coverage` 时优先用本地 readers。
  即在 `milvus_distributed_runtime.cpp:734-751` 处加判定：本地持有全部 shard → 用
  `graph_reader_` / `distance_oracle_`（本地），URMA 只作远端 fallback。
  当前部署（SN QN seed 全部 8 片）会立刻从"7/8 走远端"变成"0 远端"。
- **方案 B（要回到 33ms 必须做这个）**：恢复 distance 下推。裸机的 `RpcDistanceOracle` +
  mailbox + distance worker 代码都在（`runtime/rpc/*.cpp`、`runtime/rpc_runtime.cpp`），
  Milvus 侧只是没接线（两个 sleep 桩）。接上以后每 query 远端操作从 ~1.3 万次降到 ~260 次、
  payload 从 56 MB 降到几十 KB，这才是裸机 P99=33ms 的形态。
  若短期不想接 RPC：退一步做**向量本地化**（1M×960×4B ≈ 3.7 GB/节点，UB 内存语义下可接受），
  每个 SN QN 把自己的 1/8 query 完全本地化，检索期零远端读。
- 无论哪条，`read_dispatcher.cpp:1293-1354` 都建议改两处：`post_batch` 用
  `read_jfs_depth`（而不是 `wr_list_max`=8）；允许一轮里混合多个 `(peer, segment)` 批次，
  避免同 lane 上的 8 个 peer 互相堵。

### 验收口径（按本仓库 G1/G2 要求）

- 数据不对：核对 `remote_id % shard_count` 的归属、每个 shard 的 `main` 计数之和 == nq，
  空结果占比必须为 0（当前 `topks=0` 的比例可以直接暴露主 rank 轮转错位）。
- 失败模式：kill 一个 SN 后，对应 shard 的 query 必须**报错**而不是静默返回空结果
  （`is_main=false` 现在被当成"别人会填"，在 leader 数 < shard 数时就是数据丢失）。
- 性能：分别给出 `read ops/query`、`read bytes/query`、`submit→completion p50/p99`、
  `etcd RTT/query`、`nq 串行度`、端到端 P99。没有前四项，只报 P99 无法判断是哪一层的问题。


