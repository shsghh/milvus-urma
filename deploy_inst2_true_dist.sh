#!/bin/bash
# instance2 8-rank startup (single machine): 8 streamingnodes = 8 ranks.
#
# Two things this script gets right that bite silently otherwise:
#
#  1) streamingNode.port is a SINGLE fixed port in milvus.yaml (default 22222).
#     Eight `milvus run streamingnode` processes must each get their own, or
#     only the first one binds/registers and the balancer sees one node.
#
#  2) streaming.walBalancer.expectedInitialStreamingNodeNum defaults to 0 (no
#     wait). With 0 the balancer assigns every pchannel to the first streaming
#     node that registers and vchannelFair PRESERVES existing assignments
#     afterwards, so the remaining SNs stay empty forever (rebalance is
#     throttled by minRebalanceIntervalThreshold, 5m per WAL). Setting it to
#     SHARDS makes the balancer wait until all ranks are up before the first
#     assignment, which is what produces one pchannel per SN.
#
# /tmp/inst2-sn_<i>.log   streamingnode i (embedded QN = rank i, seeds shard i)
# /tmp/inst2-qn_2140N.log plain querynode, segment host only (no UB seed)
MILVUS_HOME=/home/s00800245/milvus-3.0.0
cd $MILVUS_HOME
export LD_LIBRARY_PATH=$MILVUS_HOME/internal/core/output/lib/:$LD_LIBRARY_PATH
export LD_PRELOAD=$MILVUS_HOME/internal/core/output/lib/libjemalloc.so
export MALLOC_CONF=background_thread:true
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY ftp_proxy
export no_proxy='127.0.0.1,localhost,192.168.124.1,90.91.177.161,controller,'

SHARDS=8
UB_SEED_DIR=/home/s00800245/index/gist_8_urma
# Keep the UB thread budget small: the auto formula derives
# search = cpus - distance - read - rpc - reserve, which on a 96-CPU cgroup
# starts ~90 threads per process; 10 processes would fight for one cpuset.
UB_SEARCH=8
UB_DISTANCE=1
UB_READ=2
UB_RPC=1
UB_MAX_ACTIVE=8

echo '=== 0. containers ==='
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

echo '=== 3. mixcoord FIRST (waits for all SHARDS streaming nodes before assigning) ==='
MILVUS_CONF_rootcoordDmlChannelNum=$SHARDS \
MILVUS_CONF_streamingwalBalancerexpectedInitialStreamingNodeNum=$SHARDS \
METRICS_PORT=9104 nohup $MILVUS_HOME/bin/milvus run mixcoord --run-with-subprocess > /tmp/inst2-mixcoord.log 2>&1 &
sleep 20
ss -tlnp 2>/dev/null | grep ':22127 ' >/dev/null && echo 'mixcoord up' || echo 'MIXCOORD NOT UP'

echo '=== 4. datanode ==='
METRICS_PORT=9102 nohup $MILVUS_HOME/bin/milvus run datanode --run-with-subprocess > /tmp/inst2-datanode.log 2>&1 &
sleep 8

echo '=== 5. 8 streamingnodes = 8 ranks (each seeds ONE shard) ==='
for s in $(seq 0 $((SHARDS - 1))); do
  MILVUS_CONF_querynodeport=$((21301 + s)) \
  MILVUS_CONF_streamingnodeport=$((22222 + s)) \
  MILVUS_CONF_querynodeUbSeedDir=$UB_SEED_DIR \
  MILVUS_CONF_querynodeUbShardIds=$s \
  MILVUS_CONF_querynodeUbCpuBindMode=none \
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

echo '=== 8. ports (expect 8 listeners on 2222x and 8 on 2130x) ==='
ss -tlnp 2>/dev/null | grep -E '19530|19531|22127|2130[1-8]|2140[12]|2222[0-9]|910[245]|911[1-8]|912[12]'

echo '=== 9. streaming node registration + pchannel spread ==='
echo -n 'streamingnode sessions in etcd: '
docker exec milvus2-etcd-1 /usr/local/bin/etcdctl --user=etcdadmin:etcdadmin \
  --endpoints http://127.0.0.1:2379 get --prefix --keys-only by-dev/meta/session 2>/dev/null \
  | grep -c streamingnode
grep -h 'expected initial streaming node number reached' /tmp/inst2-mixcoord.log | tail -2
grep -h 'streaming node number is not enough' /tmp/inst2-mixcoord.log | tail -1

echo '=== 10. per-SN UB seed + rank split ==='
grep -h 'UB seed from dir done' /tmp/inst2-sn_*.log 2>/dev/null | tail -10
for s in $(seq 0 $((SHARDS - 1))); do
  printf 'sn_%s: main=%s\n' "$s" \
    "$(grep -c 'UB SearchEntry: main' /tmp/inst2-sn_$s.log 2>/dev/null || echo 0)"
done
grep -h 'serving search inline from local readers' /tmp/inst2-sn_*.log 2>/dev/null || true
echo '=== DONE ==='
