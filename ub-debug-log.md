# UB 并行 search / pushdown 调试记录

> 约定：从这里开始，每轮的分析结论都追加到本文档（最后一节按时间倒序/正序追加），
> 聊天里只留 3-5 行摘要。patch 文件仍在仓库根目录（`ub-parallel-search-vN.patch`），
> 设计/计划在 [ub-parallel-search-plan.md](ub-parallel-search-plan.md)。

---

## 1. 目标与当前状态

- 目标：一次 client search（nq 可达 1000）→ 8 个 vchannel 分片 → 每 QN 内多 query 并发
  → distance 走 **pushdown（RPC mailbox）**，而不是 pull（URMA READ）。
- 当前：**row sharding + 单次 forward + batch entry 已验证可用**；distance 在 pull 模式跑通
  （nq=1/10 通过），pushdown 在 nq=1/10 通过、**nq=100 卡在 lane 3**。
- 最新待验证版本：**v10**。

---

## 2. 已修复的问题（按发现顺序）

| # | 症状 | 根因 | 修法 | 版本 |
|---|---|---|---|---|
| 1 | 一个 client 请求 → 8 个 QN 各跑全量（8× 冗余） | proxy 按 vchannel fanout + delegator 每 channel 都 forward | 先做"每 collection 只 forward 一次"，后改为行分片 | v1→v3 |
| 2 | `expand_batch=32` 启动失败（status 4） | `maximum_batch = M0*eb` 撑大 RPC/队列计划，超预算；read 侧在 20/32 时都是 1024（与 32 无关） | 暴露预算 knob + 启动按 `max(config, SearchParams)` sizing | v2/v6 |
| 3 | row sharding 从不生效 | 错用 `rootCoord.dmlChannelNum`(16) 和 `dml_<n>` 序号；实测 vchannel 是 `dml_0,1,10..15`（稀疏） | 改为 `mod = 本进程 channel 数`、`rank = 字典序稠密下标` | v3 |
| 4 | QN 收到 forward 后 cgo 不返回 | 任一回调出错 → `RuntimeThreadGroup::RecordFailure` 停掉**全部**线程；且 completion 是裸指针，提交方不能提前返回 | 线程出错只记录不退出；completion 改 shared_ptr + `wait_until(timeout+5s)` | v3/v5 |
| 5 | 完成路径 use-after-free | reaper 先 `storage.reset()` 再读 `task->status()/result()` | 先拷贝再析构 | v2 |
| 6 | 提交方永远等不到 completion | slot 耗尽时丢弃已 pop 的 query / admission 失败不通知 / 排队行无超时 | 入回队首 / 显式失败 / 排队超时丢弃 | v2/v3 |
| 7 | 超时不能打断停住的 query | `RequestFailure` 只写 intent，query 在 `BeginWait` 无人重调度 | deadline + 2s 宽限后 detach 提交方为 `kTimeout` | v3 |
| 8 | **pushdown 每条 RPC 都失败**：`request fragment batch output is too small` | `EnsureRpcMailbox` 把 mailbox cell 硬编码 4096 B，transport 却按 `rpc_request_batch_max_bytes`(≥64 KiB) 打包；一个 task 就含 3840 B query vector | mailbox 改为与 transport 共用 `RequestedRuntimeConfig()` + `ResolveRuntimeConfig` 的同一份计划 | v8 |
| 9 | **nq=100 pushdown 卡死**（lane 3 单 peer，`post_peer_visits` 1M+） | 每 peer 请求信用窗口 = `request_ring_depth = 8`，小于同 peer 并发请求数；窗口关闭后 Abort→重试，对端 credit 不推进则永久空转 | 窗口 8 → 32；空转打四门计数；服务端队列视角；退避 | **v10** |

### 2.1 为什么 nq=1/10 过、nq=100 挂（问题 9 的推理）

- 距离请求是"每 query 每 peer 一个 in-flight"。
- nq=10 → 每 QN 1~2 行 → 单 peer 未完成请求 < 8（窗口内），不触发；
- nq=100 → 每 QN ~12-13 行 → 单 peer > 8 → 触发 `batch.sequence - credit_received > ring_depth`；
- owner map 是 `2/2/2/1`（7 peer / 4 lane）：lane 0-2 各 2 个 peer，负载对半摊仍 < 8；
  lane 3 只有 1 个 peer，12-13 个请求全压上去 → **第一个撞墙**；
- 撞墙后 `AbortPreparedRequest` + `continue`，`batch.sequence` 不前进但 credit 也不前进，
  于是无限空转（日志里 1M 次 `post_peer_visits`、`req_post=0`）。

---

## 3. v10 的改动（待验证）

1. `config.hpp`：`request_ring_depth` 8 → **32**。
   代价：mailbox 请求段 `8*32*64KiB = 16 MiB`，staging `rpc_threads*(32*7)*64KiB ≈ 57 MiB`(4 线程)。
2. `transport.cpp` `PostRequests`：`posted == 0 && active_peers 非空` 时每 1024 次打一行
   `[UB PostReq] idle ...`（staging_unavailable / ring_credit_waits / tx_unavailable /
   peer_visits / left_pending / ring_depth），并每 64 次退避 50µs。
3. `milvus_distributed_runtime.cpp` rpc_progress 心跳新增服务端视角：
   `[UB svc owner=N svc_pub/ svc_cons/ res_pub/ res_cons/ reserve_rejects/ svc_hwm/ res_hwm]`。

### 3.1 应用

```bash
git apply -R ub-parallel-search-v9.patch   # 或对应的已应用版本
git apply ub-parallel-search-v10.patch
```

### 3.2 判读表

| 现象 | 结论 | 下一步 |
|---|---|---|
| nq=100 跑通 | 窗口太小就是根因 | 固定 `queryNode.ub.threads.rpcProgress: 2`（3/4 分布，无单 peer lane），再做 1000 |
| 仍卡且 `ring_credit_waits` 大 | 对端不还 credit | 看对端 `[UB svc]`：`svc_cons` 不涨 = distance 线程没消费；`reserve_rejects>0` = 服务端 slot 不足（要改容量计算） |
| `staging_unavailable` 非 0 | 响应/请求 staging 环空 | 扩 staging 或修回收 |
| `tx_unavailable` 非 0 | tx 槽泄漏（提交失败路径未回收） | 修 `SubmitPreparedWrites` 失败/零提交路径 |
| `res_pub` 涨但 requester `req_comp` 不涨 | 响应没到（响应 credit / offset） | 查响应 credit 初始化与 offset 布局 |

---

## 4. 已知未解决 / 风险

1. pushdown 在 nq=100 的窗口问题需 v10 验证；若对端 credit 仍不推进，要改服务端释放/通告路径。
2. `rpc_progress_threads` 是 auto（`min(4, cpus/12)`），各 QN cgroup 不同会导致 mailbox 布局不一致
   → 生产必须固定。
3. pull 路径保留为逃生口（`UB_DISTANCE_POLICY=pull`），但它是 ~1000× 读放大的临时方案。
4. 所有 patch 均**未在本机编译/运行**（Windows 无 Go/milvus_core/URMA），验证都在用户的 Linux 集群。

---

## 5. 每轮记录

### 2026-10-22 · nq=100 pushdown 卡死诊断（v8 运行结果）

**输入证据**：8 QN 收到 batch，3 个完成（21304/21305/21307），5 个卡住
（21301/21302/21303/21306/21308）；卡住的 QN 的 rpc_progress 全在 lane 3，
`pending_peers=[3]`/`[4]`，`post_peer_visits` 1,061,000+（健康 lane 2k-5k）；
v8 init 正确（`request_cap=65536`、`req_stride=req_batch_max_bytes=65536`、`fatal=0`）；
nq=1、nq=10 正常，nq=100 卡。

**结论**：不是 mailbox 容量问题（v8 已修），是**每 peer 请求信用窗口 8 太小** +
**owner map 2/2/2/1 让 lane 3 单 peer 承受全部并发**，窗口关闭后无限 Abort→重试。

**产出**：v10（窗口 32 + 四门计数 + 服务端视角 + 退避）。

**下一步**：应用 v10 跑 nq=100，按 §3.2 判读；同时准备
`queryNode.ub.threads.rpcProgress=2` 消除单 peer lane。
