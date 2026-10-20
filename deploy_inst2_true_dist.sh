#!/bin/bash
# instance2 TRUE-distributed 8-rank startup (single machine).
#
# ---- WHY THIS SHAPE -------------------------------------------------------
# In Milvus 3.0 a *search* only ever runs on the QueryNode embedded in a
# streamingnode: `milvus run streamingnode` starts SN + QN in one process
# (cmd/milvus/util.go: StreamingNodeRole => EnableStreamingNode && EnableQueryNode,
# label STREAMING-EMBEDDED), and QueryCoord assigns vchannel leadership only to
# those nodes (internal/querycoordv2/assign/assign_policy_components.go ->
# filterSQNIfStreamingServiceEnabled). A plain `milvus run querynode` therefore
# NEVER receives a channel and never serves a search: whatever that process
# seeds is memory nobody searches.
#
# So "8 ranks" means 8 streamingnodes, not 8 querynodes:
#   * 8 SN processes: each embeds the QN that owns one vchannel and seeds
#     exactly ONE UB shard (rank i, 0-based). Each is the main rank for the
#     queries whose req_id % 8 == i, and reads the other 7 shards over URMA.
#   * plain querynodes: segment hosts only (the embedded QN is excluded from
#     resource groups, so segments must live elsewhere). They must NOT seed a
#     UB shard: one publisher per shard keeps the URMA resource directory
#     unambiguous (a duplicate record makes peer-handle import order-dependent
#     and doubles the registered memory).
#
# `rootCoord.dmlChannelNum` must equal the shard count so each of the 8
# vchannels maps 1:1 onto a pchannel and each SN's embedded QN becomes leader
# of exactly one of them. With the default 16 pchannels two vchannels can land
# on the same SN and the req_id % 8 rotation then leaves some shards unserved
# (silent empty results, not an error). Changing this value needs clean meta:
# run recreate_middleware.sh with WIPE_META=1 first, or drop by-dev/ in etcd
# and the Milvus buckets in minio.
#
# ---- VERIFY AFTERWARDS ----------------------------------------------------
#   grep -h 'UB seed from dir done' /tmp/inst2-sn_*.log         # loadedShards=1 expectedShards=8
#   grep -h 'UB SearchEntry: main' /tmp/inst2-sn_*.log | wc -l  # ~= total nq
#   grep -h 'serving search from local readers' /tmp/inst2-sn_*.log  # must be empty
#   GetChannelJSON: channel_node_infos leaders spread over the 8 SN node IDs
#
# Ports: queryNode.port is bound by the embedded QN of each SN process, so each
# SN needs a distinct one (21301..21308); segment hosts use 21401/21402.
MILVUS_HOME=/home/s00800245/milvus-3.0.0
cd $MILVUS_HOME
export LD_LIBRARY_PATH=$MILVUS_HOME/internal/core/output/lib/:$LD_LIBRARY_PATH
export LD_PRELOAD=$MILVUS_HOME/internal/core/output/lib/libjemalloc.so
export MALLOC_CONF=background_thread:true
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY ftp_proxy
export no_proxy='127.0.0.1,localhost,192.168.124.1,90.91.177.161,controller,'

SHARDS=8                                   # = UB shard count = number of ranks
UB_SEED_DIR=/home/s00800245/index/gist_8_urma
# Per-process UB thread budget. The C++ auto formula is
# search = cpus - distance - read - rpc - reserve, which on a 96-CPU cgroup
# starts ~90 threads per process; with 10 processes on one box they would all
# pin onto the same cpuset. Keep it small: maxActiveQueries bounds the
# per-rank query concurrency anyway.
UB_SEARCH=8
UB_DISTANCE=1
# 2 read lanes per SN: 8 SNs x 2 = 16 busy-poll progress threads on the shared
# cpuset. Each lane still owns ~4 remote shards and can keep read_jfs_depth
# (1024) READs in flight, so the lane count is not the throughput limit.
# If UmdkLanePostEngine::Create fails with a device resource error, drop to 1.
UB_READ=2
UB_RPC=1
UB_MAX_ACTIVE=8

echo '=== 0. verify containers ==='
docker ps --filter name=milvus2- --format '{{.Names}} {{.Status}}'

echo '=== 1. kill all instance2 milvus ==='
for p in $(ps -eo pid,args | grep -E 'milvus run' | grep -v grep | grep '/home/s00800245\|\./bin/milvus' | awk '{print $1}'); do
  kill -9 $p 2>/dev/null; pkill -9 -P $p 2>/dev/null
done
sleep 5

echo '=== 2. clear etcd consume-checkpoints (one per pchannel) ==='
for i in $(seq 0 $((SHARDS - 1))); do
  docker exec milvus2-etcd-1 /usr/local/bin/etcdctl --user=etcdadmin:etcdadmin \
    --endpoints http://127.0.0.1:2379 del \
    "by-dev/meta/streamingnode-meta/wal/by-dev-rootcoord-dml_${i}/consume-checkpoint" 2>&1 | head -1
done

echo '=== 3. mixcoord FIRST (dmlChannelNum == shard count) ==='
MILVUS_CONF_rootcoordDmlChannelNum=$SHARDS \
METRICS_PORT=9104 nohup $MILVUS_HOME/bin/milvus run mixcoord --run-with-subprocess > /tmp/inst2-mixcoord.log 2>&1 &
sleep 20
ss -tlnp 2>/dev/null | grep ':22127 ' >/dev/null && echo 'mixcoord up' || echo 'MIXCOORD NOT UP'

echo '=== 4. datanode ==='
METRICS_PORT=9102 nohup $MILVUS_HOME/bin/milvus run datanode --run-with-subprocess > /tmp/inst2-datanode.log 2>&1 &
sleep 8

echo '=== 5. 8 streamingnodes = 8 ranks (each seeds ONE shard) ==='
# SN i: embedded QN on port 2130(1+i), seeds shard i, metrics 911(1+i).
for s in $(seq 0 $((SHARDS - 1))); do
  # streamingNode.port is a single fixed port (default 22222). Without a
  # per-process value only one SN can bind and register; the balancer then
  # sees one node and assigns every pchannel to it (all assignments land on
  # one ServerID). queryNode.port alone is not enough.
  MILVUS_CONF_querynodeport=$((21301 + s)) \
  MILVUS_CONF_streamingnodeport=$((22222 + s)) \
  MILVUS_CONF_querynodeUbSeedDir=$UB_SEED_DIR \
  MILVUS_CONF_querynodeUbShardIds=$s \
  MILVUS_CONF_querynodeUbMaxActiveQueries=$UB_MAX_ACTIVE \
  MILVUS_CONF_querynodeUbThreadsSearch=$UB_SEARCH \
  MILVUS_CONF_querynodeUbThreadsDistance=$UB_DISTANCE \
  MILVUS_CONF_querynodeUbThreadsReadProgress=$UB_READ \
  MILVUS_CONF_querynodeUbThreadsRpcProgress=$UB_RPC \
  METRICS_PORT=$((9111 + s)) \
  nohup $MILVUS_HOME/bin/milvus run streamingnode --run-with-subprocess \
    > /tmp/inst2-sn_$s.log 2>&1 &
  disown
  sleep 3
done
sleep 15

echo '=== 6. segment hosts (plain QN, NO UB seed) ==='
# These never serve search. seedDir MUST be cleared explicitly: milvus.yaml
# sets a default path and a second seeder per shard would publish duplicate
# resource records.
for n in 1 2; do
  numactl --cpunodebind=$((n - 1)) --membind=$((n - 1)) \
    env MILVUS_CONF_querynodeport=$((21400 + n)) \
    MILVUS_CONF_querynodeUbSeedDir= \
    METRICS_PORT=$((9120 + n)) \
    $MILVUS_HOME/bin/milvus run querynode --run-with-subprocess \
    > /tmp/inst2-qn_$((21400 + n)).log 2>&1 &
  disown
  sleep 3
done
sleep 10

echo '=== 7. proxy ==='
METRICS_PORT=9105 nohup $MILVUS_HOME/bin/milvus run proxy --run-with-subprocess > /tmp/inst2-proxy.log 2>&1 &
sleep 30

echo '=== 8. verify ports ==='
ss -tlnp 2>/dev/null | grep -E '19530|19531|22127|2130[1-8]|2140[12]|2222[0-9]|910[245]|911[1-8]|912[12]'

echo '=== 9. verify UB seed: every SN must show loadedShards=1 expectedShards=8 ==='
grep -h 'UB seed from dir done' /tmp/inst2-sn_*.log 2>/dev/null | tail -20

echo '=== 10. verify 8-way rank split (run after the first search) ==='
for s in $(seq 0 $((SHARDS - 1))); do
  printf 'sn_%s: main=%s\n' "$s" \
    "$(grep -c 'UB SearchEntry: main' /tmp/inst2-sn_$s.log 2>/dev/null || echo 0)"
done
echo '-- must be empty for 1-shard ranks (local readers are not used) --'
grep -h 'serving search from local readers' /tmp/inst2-sn_*.log 2>/dev/null || true
echo '-- URMA read timeouts (tail) --'
grep -h 'event=read_timeout' /tmp/inst2-sn_*.log 2>/dev/null | tail -5 || true
echo '=== DONE ==='
