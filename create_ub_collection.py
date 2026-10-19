#!/usr/bin/env python3
"""Create (or recreate) the UB collection and load it.

Why: the UB fast path (delegator -> ub.RunSearch) never touches Milvus
segments, so it does NOT need ub_inject metadata, placeholder segments or
MinIO index files. It needs exactly three things:

  1. a collection with SHARDS shards (== UB shard count == number of SNs),
  2. that collection Loaded, so QueryCoord gives each SN's embedded QN exactly
     one vchannel and the delegator accepts searches,
  3. queryNode.ub.seedDir on the SNs pointing at the matching triple.

So after WIPE_META you can bootstrap with this script alone; run ub_inject only
if you also want the native per-segment fallback to be comparable.

The vector field must be the 2nd field (pk=100, vector=101): cmd/tools/ub_inject
hardcodes fieldID=101 (main.go:32).

Usage:
  python3 create_ub_collection.py                 # create if missing, then load
  python3 create_ub_collection.py --drop          # drop and recreate
  python3 create_ub_collection.py --name ub_gist --dim 960 --shards 8
"""
import argparse
import sys
import time

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default="19531")
    ap.add_argument("--name", default="ub_gist")
    ap.add_argument("--dim", type=int, default=960)
    ap.add_argument("--shards", type=int, default=8)
    ap.add_argument("--metric", default="L2", choices=["L2", "IP", "COSINE"])
    ap.add_argument("--drop", action="store_true", help="drop first if it exists")
    ap.add_argument("--skip-load", action="store_true")
    ap.add_argument("--timeout", type=int, default=300)
    ap.add_argument(
        "--print-id",
        action="store_true",
        help="create if missing, print only the collection id, do not load",
    )
    args = ap.parse_args()

    connections.connect(host=args.host, port=args.port, timeout=30)
    print(f"connected to {args.host}:{args.port}")

    exists = utility.has_collection(args.name)
    if exists and args.drop:
        print(f"dropping existing collection {args.name}")
        try:
            Collection(args.name).release()
        except Exception as exc:  # noqa: BLE001 - release of a released coll is fine
            print("release:", exc)
        utility.drop_collection(args.name)
        exists = False

    if not exists:
        fields = [
            # pk is field 100, so the vector becomes field 101, which is what
            # ub_inject expects.
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name="vec", dtype=DataType.FLOAT_VECTOR, dim=args.dim),
        ]
        schema = CollectionSchema(fields=fields, description="UB global index")
        col = Collection(name=args.name, schema=schema, shards_num=args.shards)
        print(f"created collection {args.name}: id={col.id} shards={args.shards} dim={args.dim}")
    else:
        col = Collection(args.name)
        print(f"collection {args.name} already exists: id={col.id}")

    if args.print_id:
        # Machine-readable: the shell wrapper captures this to pass
        # --collection-id to ub_inject.
        print(int(col.id))
        return 0

    # No index on purpose: the UB path bypasses segments. Creating a UB_HNSW
    # index here would require MinIO files + injected metadata; load without an
    # index is allowed (native brute force), which keeps the fallback usable.
    if not args.skip_load:
        try:
            col.release()
        except Exception as exc:  # noqa: BLE001
            print("release:", exc)
        time.sleep(2)
        col.load(replica_number=1)
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            state = str(utility.load_state(args.name))
            print("load state:", state)
            if state.endswith("Loaded"):
                print(f"LOADED collection_id={col.id}")
                print("next: send a few searches, then run check_8sn.sh")
                return 0
            if "Fail" in state or "fail" in state:
                print("LOAD_FAILED", file=sys.stderr)
                return 1
            time.sleep(5)
        print("TIMEOUT waiting for load", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
