# 为什么要用 8 个 streamingnode（而不是 8 个 querynode）

> 背景：URMA 分布式检索（hnsw_urma 接入 Milvus）要求"每进程一个 rank"。
> 本文说明为什么 rank 必须落在 `streamingnode` 上，以及历史上"8 个 plain querynode"为什么跑不出结果。
> 结论对 2/4/8 都适用：不是"必须 8"，而是 `rank 数 = streamingnode 数 = dmlChannelNum = shard 数`。

## 1. 结论

UB 的 rank 必须**同时**满足三个身份，缺一不可：

| 身份 | 含义 | 谁满足 |
|---|---|---|
| ① 持有 vchannel 领导权 | 只有 leader 才能收到 search 请求 | 仅 streaming-enabled 的 QueryNode（= `milvus run streamingnode` 内嵌的那个 QN） |
| ② 本地加载了 shard（UB seed） | 手上有这份图和向量 | 通过 `queryNode.ub.seedDir` / `queryNode.ub.shardIds` 显式 seed 的进程 |
| ③ 是 search 的执行者（delegator） | UB 快速路径挂在 delegator 里（`delegator.Search → ub.TrySearch`） | vchannel leader 的 QN |

**普通 `milvus run querynode` 只可能满足 ②，永远不满足 ①**，因此也永远不满足 ③。
所以"8 个 rank"只能是"8 个 streamingnode"。

## 2. 机制：search 为什么到不了普通 QN

### 2.1 请求是发给 vchannel leader 的

proxy 按 collection 的 vchannel 把 search 分发给各 shard 的 leader（delegator）；delegator 要么走本地 segment 搜索，要么走 UB 快速路径：

```
proxy → shard leader (vchannel 所在 QN) → delegator.Search
                                          ├─ ub.TrySearch        (UB 快速路径)
                                          └─ 原生 segment 搜索
```

也就是说：**谁收到请求由 vchannel 分配决定，与"谁加载了数据"无关**。

### 2.2 vchannel 只分配给启用了 streaming 服务的 QN

- `cmd/milvus/util.go`：`milvus run streamingnode` 被展开为 `EnableStreamingNode && EnableQueryNode`（同一进程里 SN + 内嵌 QN，label `STREAMING-EMBEDDED`）。
- `internal/querycoordv2/assign/assign_policy_components.go`：候选节点经过 `filterSQNIfStreamingServiceEnabled` 过滤。
- `internal/querycoordv2/assign/streaming_query_node_channel_helper.go`：只从 streaming-enabled 节点里选 vchannel 的 owner。

普通 `milvus run querynode` 注册时 `EnableStreamingNode=false`，**在候选集合里就被过滤掉**，
所以它不会拿到任何 vchannel，也就不会收到任何 search 请求。

### 2.3 内嵌 QN 又不能承载 segment

- `internal/querycoordv2/resource_manager.go`：内嵌 QN 被排除在资源组之外，不会被当作 segment 的宿主。
- `internal/querycoordv2/ops_services.go`：内嵌 QN 不能被 resume。

所以最终形态是两层：

```
8 × streamingnode   → 8 个 vchannel（收请求）+ 各自 seed 1 片 UB shard + 跑 UB 搜索
2 × plain querynode → segment host（原生路径），不参与 UB rank
```

## 3. rank 的身份来自"本地 shard"

UB 数据面里：

- `local_rank = min(loaded_shard_ids)`（一个进程只 seed 一片时即 rank = shard）；
- 路由是 `target_shard = req_id % shard_count`；
- 只有 `loaded_shard_ids` 包含 `target_shard` 的进程才是 `main`，其他进程 `is_main=false` 并返回空行。

因此在 8 QN 方案里，**持有 shard i 的进程收不到 query**，`target_shard = i` 的请求就永远没有"本地 main"：
要么全量远程 pull，要么回退 native。这就是 8 QN 跑不出结果的本质原因——**数据与 channel 被拆到了不同进程**。

## 4. 组合对照（实测/推导）

| 组合 | 谁收 query | 谁有数据 | 结果 |
|---|---|---|---|
| 8 QN 各 seed 1 片 + 1 SN | 只有 SN | SN 没 seed（或只 1 片） | QN 全部空转；SN `data_plane_ready=false` → 回退 native/本地。实测 875/1000 空结果、recall 0.0081 |
| 1 SN 全量 seed（8 片） | SN | SN 全有 | 能跑、recall 0.936，但只有 **1 个 rank**：所有 query 串行在一个进程（`preferLocal` 5.07s / URMA pull 127.5s），没有 8× 聚合 |
| 8 QN 各 seed 1 片，无 SN | 无人（无 vchannel leader） | 8 个 QN | collection 不可搜 |
| **8 SN 各 seed 1 片（当前）** | 8 个 SN 各收 1/8 vchannel | 每 SN 1 片 | rank == shard，`req_id % 8` 路由，`main` 落在同时"持有数据 + 收到请求"的进程上 ✓ |

## 5. 为什么是 8（而不是 2 或 4）

成立的不是"必须 8"，而是这条等式：

```
rank 数 = streamingnode 进程数 = rootCoord.dmlChannelNum = UB shard 数
```

- **2 个 SN**：可以跑，但只有 2× 并发；每进程要持 4 片，而当前代码假设"一片一 rank"（`local_rank = min(loaded_shard_ids)`、pull oracle 里 `local_shard = *local_shards_.front()`），多片需要额外改造；
- **8 个 SN**：单机 8 进程，正好 1 进程 1 片，`shard == rank` 假设成立，最容易压满带宽/CPU，也最能体现 8× 聚合。

选 8 是为了验证"分布式线性扩展"，不是架构硬性要求。

## 6. 三个配套约束（8 SN 也失败过，就是这些）

1. **`rootCoord.dmlChannelNum` 必须等于 SN 数。**
   `internal/streamingcoord/server/channel/manager.go` 的 `AllocVirtualChannels` 会按 pchannel 上已分配的 vchannel 数量排序、取 N 个**不同** pchannel；
   `dmlChannelNum=8` 才能让 8 个 vchannel 落在 8 个不同 pchannel 上，从而分给 8 个不同的 leader。
   若 `dmlChannelNum=1`，8 个 vchannel 全在同一 pchannel → 全部落到同一个 SN。

2. **`streamingNode.port` 必须每进程不同。**
   `configs/milvus.yaml` 里 `streamingNode.port` 默认 **22222** 是单值，8 个进程抢同一端口 → 只有 1 个注册成功 → balancer 只看到 1 个节点 → 8 个 pchannel 全给它。
   启动脚本必须给每个 SN 传 `MILVUS_CONF_streamingnodeport=$((22222 + i))`。

3. **首次分配必须等所有 SN 注册完。**
   `streaming.walBalancer.expectedInitialStreamingNodeNum` 默认 `0`（不等待）：
   balancer 在第 1 个 SN 注册时就把 8 个 pchannel 全分给它，而 `vchannelFair` 策略**保留已有 assignment**（只对"新来的 channel"重新分配），
   rebalance 又被 `minRebalanceIntervalThreshold`（默认 5 分钟/WAL）节流 → 后 7 个 SN 会一直空着。
   必须设 `MILVUS_CONF_streamingwalBalancerexpectedInitialStreamingNodeNum=8`（并清一次元数据，否则旧的"全给一个"会粘住）。

## 7. 代价与收益

**代价**

- 8 个进程：每个都加载 1/8 的图 + 向量（总量不变），但**每进程**都有完整 thread group（search/distance/read/rpc 线程）、14 个 peer handle、6 段注册内存 → 进程级开销 ×8；
- 8 个进程共享一张 URMA 卡：read/写带宽需要均摊，`ub.threads.*` 必须按进程配额收小（例如 search=8 / distance=1 / read=2 / rpc=1）。

**收益**

- 8× 并发（query 被 8 个 vchannel 分走）；
- 每个 query 有 7/8 的概率 `main` 落在"别的 SN"上 —— 这正是 pull 模式下 ~8× 冗余读的来源，也是 distance pushdown 要解决的问题；
- 每进程只持 1/8 数据 → 单进程内存和 cache 压力小。

## 8. 布局自检清单

跑起来后按顺序确认：

```bash
# 1) 8 个 SN 都注册了
docker exec milvus2-etcd-1 /usr/local/bin/etcdctl --endpoints http://127.0.0.1:2379 \
  get --prefix --keys-only by-dev/meta/session | grep -c streamingnode        # 期望 8

# 2) 8 个 streamingNode 端口都在听
ss -tlnp | grep -cE ':2222[0-9]'                                             # 期望 8

# 3) balancer 等到了 8 个节点才首次分配
grep 'expected initial streaming node number reached' /tmp/inst2-mixcoord.log

# 4) 每个 SN 各持 1 片、rank 正确
grep 'UB seed from dir done' /tmp/inst2-sn_*.log                              # loadedShards=1 expectedShards=8
grep 'UB EnableDistributed: world_size=8 local_rank=' /tmp/inst2-sn_*.log     # local_rank 分别 0..7

# 5) 8 个 SN 都当过 main（每个 query 只在一个 SN 上 main）
for s in $(seq 0 7); do printf 'sn_%s main=%s\n' $s "$(grep -c 'UB SearchEntry: main' /tmp/inst2-sn_$s.log)"; done
```

全部满足才说明"8 个 rank 的布局"成立；任何一条不满足，先修布局再看性能/召回。
