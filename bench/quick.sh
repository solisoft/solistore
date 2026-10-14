#!/usr/bin/env bash
# A short side-by-side run, for iterating: 1M documents, 20,000 fsync'd
# commits (2,000 is too few to time), 10 s of mixed load. Same pinning as
# run.sh. Prints one line per phase and engine.
#
#   bench/quick.sh <binary> <work-dir> <out-dir> [engines...]
set -u
BIN=$1 WORK=$2 OUT=$3; shift 3
ENGINES=("${@:-rocks solistore-eventual solistore}")
[ $# -eq 0 ] && ENGINES=(rocks solistore-eventual solistore)
CORES=${CORES:-16-23}
mkdir -p "$OUT"
for e in "${ENGINES[@]}"; do
  rm -rf "$WORK/quick"
  timeout 1800 nice -n 10 ionice -c2 -n7 taskset -c "$CORES" "$BIN" run --engine "$e" --dir "$WORK/quick" \
    --docs 1000000 --gets 1000000 --updates 300000 --scans 20000 --sync-commits 20000 \
    --mixed-secs 10 --settle-secs 5 > "$OUT/quick-$e.jsonl" 2> "$OUT/quick-$e.err" || echo "$e: exit $?"
done
rm -rf "$WORK/quick"
python3 - "$OUT" "${ENGINES[@]}" <<'PY'
import json, sys
out, engines = sys.argv[1], sys.argv[2:]
runs = {e: {json.loads(l)["phase"]: json.loads(l) for l in open(f"{out}/quick-{e}.jsonl") if l.strip()} for e in engines}
phases = [p for p in runs[engines[0]] if not p.startswith(("settle", "reclaim", "drop", "create", "close"))]
print(f"{'phase':16}" + "".join(f"{e[:18]:>20}{'p99':>6}" for e in engines))
for p in phases:
    print(f"{p:16}" + "".join(f"{runs[e][p]['ops_per_sec']:20.0f}{runs[e][p]['p99_us']:6}" for e in engines))
print("hwm MB  " + "  ".join(f"{e}={max(d['hwm_mb'] for d in runs[e].values())}" for e in engines))
PY
