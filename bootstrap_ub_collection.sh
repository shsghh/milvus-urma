#!/bin/bash
# Gate 2 + Gate 3 of the 8-rank UB bootstrap, one command:
#
#   gate 2  all 8 pchannels are on 8 *different* streamingnodes
#   gate 3  the UB collection exists, is Loaded, and (optionally) has the
#           pre-built triple injected for the native fallback
#
# Usage (after WIPE_META + deploy_inst2_true_dist.sh):
#   python3 create_ub_collection.py --drop      # if you want a clean recreation
#   ./bootstrap_ub_collection.sh                # gate2 check + create + load
#
# Optional legacy injection (only needed if you want the *native* per-segment
# fallback to be comparable; the UB fast path never reads it):
#   INJECT=1 SEGMENT_IDS=id0,...,id7 ./bootstrap_ub_collection.sh
#
# Knobs: SKIP_GATE=1, SKIP_CREATE=1, COLL=ub_gist, DIM=960, SHARDS=8,
#        METRIC=L2, PYTHON=python3
set -u

MILVUS_HOME=${MILVUS_HOME:-/home/s00800245/milvus-3.0.0}
SHARDS=${SHARDS:-8}
DIM=${DIM:-960}
METRIC=${METRIC:-L2}
COLL=${COLL:-ub_gist}
PROXY_PORT=${PROXY_PORT:-19531}
PYTHON=${PYTHON:-python3}
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TRIPLE_DIR=${TRIPLE_DIR:-/home/s00800245/index/gist_8_urma}
ETCD_CONTAINER=${ETCD_CONTAINER:-milvus2-etcd-1}
INDEX_VER=${INDEX_VER:-8}
BUILD_ID_BASE=${BUILD_ID_BASE:-200200000}
INJECT_BIN=${INJECT_BIN:-$MILVUS_HOME/bin/ub_inject}
LOG_GLOB=${LOG_GLOB:-/tmp/inst2-sn_*.log}

ETCDCTL="docker exec $ETCD_CONTAINER /usr/local/bin/etcdctl --user=etcdadmin:etcdadmin --endpoints http://127.0.0.1:2379"

die() { echo "ERROR: $*" >&2; exit 1; }
say() { echo; echo "=== $* ==="; }

# ------------------------------------------------------------------ gate 2
if [ "${SKIP_GATE:-0}" != "1" ]; then
  say "gate 2: pchannel -> streamingnode spread"
  [ -f "$(ls $LOG_GLOB 2>/dev/null | head -1)" ] || die "no streamingnode logs ($LOG_GLOB); run deploy_inst2_true_dist.sh first"
  seen_all=""
  for f in $LOG_GLOB; do
    [ -f "$f" ] || continue
    pch=$(grep -ohE 'rootcoord-dml_[0-9]+' "$f" 2>/dev/null | sort -u | tr '\n' ' ')
    printf '  %-28s pchannels: %s\n' "$(basename "$f")" "${pch:-<none>}"
    seen_all="$seen_all $pch"
  done
  unique=$(echo $seen_all | tr ' ' '\n' | sort -u | grep -c . || true)
  echo "  distinct pchannels across all SNs: $unique (want $SHARDS)"
  if [ "$unique" -ne "$SHARDS" ]; then
    cat >&2 <<'EOT'
  REFUSING to touch the collection: the pchannel layout is not 1:1 yet.
  If you create/load the collection now, all its vchannels bind to whichever
  SN owns the pchannels, and you get the "all main on sn0 / 875 empty" result.
  Wait 60-120 s (the balancer rebalances on a timer and on node change), or
  restart mixcoord once with all 8 SNs already up, then re-run this script.
EOT
    exit 3
  fi
fi

# ------------------------------------------------------------------ gate 3
if [ "${SKIP_CREATE:-0}" != "1" ]; then
  say "gate 3a: create collection $COLL (shards=$SHARDS dim=$DIM metric=$METRIC)"
  COLLECTION_ID=$("$PYTHON" "$SCRIPT_DIR/create_ub_collection.py" \
    --port "$PROXY_PORT" --name "$COLL" --dim "$DIM" --shards "$SHARDS" \
    --metric "$METRIC" --print-id | tail -1) || die "create_ub_collection.py failed"
  case "$COLLECTION_ID" in
    ''|*[!0-9]*) die "could not parse collection id (got: $COLLECTION_ID)" ;;
  esac
  echo "  collection_id=$COLLECTION_ID"

  say "gate 3b: load $COLL"
  "$PYTHON" "$SCRIPT_DIR/create_ub_collection.py" \
    --port "$PROXY_PORT" --name "$COLL" --dim "$DIM" --shards "$SHARDS" \
    --metric "$METRIC" || die "load failed"
else
  COLLECTION_ID=${COLLECTION_ID:-}
fi

# --------------------------------------------------------- optional inject
if [ "${INJECT:-0}" = "1" ]; then
  say "gate 3c: inject the pre-built triple (native fallback only)"
  [ -n "${COLLECTION_ID:-}" ] || die "INJECT=1 needs COLLECTION_ID (or SKIP_CREATE=0)"
  : "${PARTITION_ID:=$((COLLECTION_ID + 1))}"
  : "${SEGMENT_IDS:?INJECT=1 needs SEGMENT_IDS=id0,...,id7 in shard order}"
  IFS=',' read -r -a SEGS <<< "$SEGMENT_IDS"
  [ "${#SEGS[@]}" -eq "$SHARDS" ] || die "SEGMENT_IDS has ${#SEGS[@]} entries, need $SHARDS"

  echo "  NOTE: ub_inject only checks the *count* of ids, not that they belong"
  echo "  to collection $COLLECTION_ID. Empty collections have no segments, so"
  echo "  injection is a no-op there; skip INJECT=1 unless you created $SHARDS"
  echo "  real segments for this collection."

  for i in $(seq 0 $((SHARDS - 1))); do
    seg=${SEGS[$i]}
    build=$((BUILD_ID_BASE + i))
    base="files/index_v1/$COLLECTION_ID/$PARTITION_ID/$seg/$build/$INDEX_VER"
    for leaf in global.meta upper_graph.index "shard_$i.index"; do
      [ -f "$TRIPLE_DIR/$leaf" ] || die "$TRIPLE_DIR/$leaf missing"
      if command -v mc >/dev/null 2>&1 && [ -n "${MC_ALIAS:-}" ]; then
        mc cp "$TRIPLE_DIR/$leaf" "$MC_ALIAS/$base/$leaf" >/dev/null || die "mc cp $base/$leaf"
      else
        echo "  mc cp $TRIPLE_DIR/$leaf ${MC_ALIAS:-<alias>}/$base/$leaf"
      fi
    done
  done

  [ -x "$INJECT_BIN" ] || ( cd "$MILVUS_HOME" && go build -o "$INJECT_BIN" ./cmd/tools/ub_inject ) \
    || die "go build ub_inject failed"
  "$INJECT_BIN" --mode=v2 --dir "$TRIPLE_DIR" --build-id-base "$BUILD_ID_BASE" \
    --collection-id "$COLLECTION_ID" --partition-id "$PARTITION_ID" \
    --segment-ids "$SEGMENT_IDS" || die "ub_inject failed"
  n=$($ETCDCTL get --prefix --keys-only "by-dev/meta/segment-index/$COLLECTION_ID/" 2>/dev/null | grep -c . || true)
  echo "  segment-index rows: $n (want $SHARDS)"
  [ "$n" -eq "$SHARDS" ] || die "injection incomplete"
  echo "  restart datacoord/milvus so the metastore snapshot picks the keys up."
fi

# ------------------------------------------------------------------ next
say "next"
cat <<EOT
  1) send a few searches to $COLL
  2) bash ~/check_8sn.sh
     - every SN has 'UB SearchEntry: main', sum(main) ~= queries sent
     - target shard histogram: residues 0..7, each answered by exactly one SN
     - no 'falling back to native', no 'HnswSearchTask::Create failed'
     - no 'serving search inline from local readers' (1-shard ranks must use URMA)
EOT
