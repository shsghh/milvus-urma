#!/bin/bash
# 8x streamingnode (= 8 UB ranks) preflight + failure diagnosis.
#
# Run this AFTER deploy_inst2_true_dist.sh (or your equivalent) and AFTER you
# have sent at least a few search requests. It prints, per streamingnode, the
# five facts that decide whether the 8-rank layout is actually working:
#
#   1. which shard this SN seeded            (UB seed: shardIds / loadedShards)
#   2. whether its URMA data plane is ready  (UB EnableDistributed: ready / missing)
#   3. how many searches it answered         (UB SearchEntry: main / not main)
#   4. which req_id % 8 residues it answered (target shard histogram)
#   5. whether it silently fell back         (falling back to native / Create failed)
#
# Decision table (see the comments at the bottom of the output).
set -u

MILVUS_HOME=${MILVUS_HOME:-/home/s00800245/milvus-3.0.0}
SHARDS=${SHARDS:-8}
# Accept both the rewritten script (/tmp/inst2-sn_*.log) and the old one
# (/tmp/inst2-streamingnode.log).
LOG_GLOB=${LOG_GLOB:-/tmp/inst2-sn_*.log}
if ! ls $LOG_GLOB >/dev/null 2>&1; then
  LOG_GLOB=/tmp/inst2-streamingnode.log
fi
ETCD_CONTAINER=${ETCD_CONTAINER:-milvus2-etcd-1}
ETCDCTL="docker exec $ETCD_CONTAINER /usr/local/bin/etcdctl --user=etcdadmin:etcdadmin --endpoints http://127.0.0.1:2379"

count() { # count <pattern> <file>
  grep -c -- "$1" "$2" 2>/dev/null || true
}

echo "===== 1. processes ====="
ps -eo pid,args | grep 'milvus run' | grep -v grep | \
  sed -E 's/.*milvus run ([a-z]+).*/\1/' | sort | uniq -c
echo "queryNode.port listeners (should be 8 distinct SN ports + segment hosts):"
ss -tlnp 2>/dev/null | grep -oE ':(213[0-9][0-9]|214[0-9][0-9]) ' | sort -u | tr '\n' ' '; echo

echo
echo "===== 2. pchannel / vchannel fan-out ====="
echo -n "etcd WAL pchannels (want $SHARDS): "
$ETCDCTL get --prefix --keys-only by-dev/meta/streamingnode-meta/wal/ 2>/dev/null \
  | grep -oE 'rootcoord-dml_[0-9]+' | sort -u | wc -l
echo -n "etcd collections: "
$ETCDCTL get --prefix --keys-only by-dev/meta/root-coord/collection 2>/dev/null | wc -l
echo "NOTE: with dmlChannelNum=8 and 8 SNs each SN must own exactly 1 pchannel;"
echo "      if you changed dmlChannelNum after the collection existed, the old"
echo "      8 vchannels are still pinned to the old pchannels -> wipe meta and"
echo "      recreate the collection (WIPE_META=1 ./recreate_middleware.sh)."

echo
echo "===== 3. per-streamingnode ====="
TOTAL_MAIN=0
for f in $LOG_GLOB; do
  [ -f "$f" ] || continue
  seed=$(grep -h 'UB seed from dir done' "$f" 2>/dev/null | tail -1)
  enable=$(grep -h 'UB EnableDistributed: world_size=' "$f" 2>/dev/null | tail -1)
  main=$(count 'UB SearchEntry: main' "$f")
  notmain=$(count 'UB SearchEntry: not main' "$f")
  createfail=$(count 'HnswSearchTask::Create failed' "$f")
  fallback=$(count 'falling back to native' "$f")
  inline=$(count 'serving search inline from local readers' "$f")
  deferred=$(count 'URMA data plane deferred' "$f")
  TOTAL_MAIN=$((TOTAL_MAIN + main))

  echo "--- $(basename "$f")"
  echo "    seed    : ${seed:-<none!>}"
  echo "    dataplane: ${enable:-<none!>}  deferred_warn=$deferred"
  echo "    search  : main=$main not_main=$notmain create_failed=$createfail fallback_native=$fallback inline_local=$inline"
  # Which pchannels / vchannels this process actually talks about. This is the
  # decisive evidence for "did the 8 vchannels really spread over 8 SNs":
  # pchannel names are by-dev-rootcoord-dml_<N>, vchannels add _<collID>v<idx>.
  pch=$(grep -ohE 'rootcoord-dml_[0-9]+' "$f" 2>/dev/null | sort -u | tr '\n' ' ')
  vch=$(grep -ohE 'rootcoord-dml_[0-9]+_[0-9]+v[0-9]+' "$f" 2>/dev/null | sort -u | wc -l)
  echo "    pchannels seen: ${pch:-<none>}   vchannels seen: $vch"
  if [ "$main" -gt 0 ]; then
    echo -n "    answered residues (target shard): "
    grep -h 'UB SearchEntry: main' "$f" 2>/dev/null \
      | grep -oE 'target shard [0-9]+' | awk '{print $3}' | sort -n | uniq -c | tr '\n' ' '
    echo
  fi
done

echo
echo "===== 4. totals ====="
echo "sum(main) over all SNs = $TOTAL_MAIN   (should equal the number of queries you sent)"
echo "sum(not main)          = $(for f in $LOG_GLOB; do count 'UB SearchEntry: not main' "$f"; done | paste -sd+ | bc 2>/dev/null || echo '?')"

echo
echo "===== how to read this ====="
cat <<'EOT'
A) all 8 SNs have main counts, sum(main) == queries sent, residues 0..7 each
   on exactly one SN  ->  the 8-way split works; look elsewhere for recall.

B) sum(main) ~= queries/8 and only ONE log has main counts
   ->  only one SN is a vchannel leader. pchannels were not spread 1:1 over
       the 8 SNs (dmlChannelNum not 8, or stale meta). Wipe meta, recreate
       the collection, re-check.

C) every SN has main counts but sum(main) ~= queries/8, and every SN answered
   the SAME residue  ->  queryNode.ub.shardIds did not take effect (all SNs
   seeded the same shard). Check the `seed` line above: shardIds must differ.

D) `dataplane` shows ready=0 / missing=7, or `URMA data plane deferred`
   appears  ->  peer handles incomplete: EnableDistributedSearch failed, the
   SN fell back to native per-segment search (wrong recall). Check that every
   SN published its graph+vector resource records (grep 'UB shard resources
   published') and that all 8 SNs are up before the first search.

E) create_failed>0  ->  LocalGraphReader was selected while the thread group
   is running. That is the bug fixed by ub-fix-inline-local-search.patch;
   make sure core was rebuilt after applying it.

F) fallback_native>0 with main==0 on some SNs  ->  those SNs never entered the
   UB path for the queries they own; check `UB prepare` errors in the same log.
EOT
