#!/usr/bin/env bash
# Correctness gates for one engine: N crash runs (SIGKILL mid-commit, then
# check no acknowledged batch is lost and none is torn) and one visibility
# run (readers racing writers). Pinned and deprioritised like run.sh.
#
#   bench/gate.sh <binary> <engine> <work-dir> <results-dir> [crash-runs] [extra flags…]
#
# Extra flags go to every run, e.g. `--sx-buffer-mb 2` so that solistore
# flushes many times during each 3-second crash run.
set -u
BIN=$1 ENGINE=$2 WORK=$3 OUT=$4 RUNS=${5:-100}
shift 5 2>/dev/null || shift $#
EXTRA=("$@")
CORES=${CORES:-16-23}
RUN=(nice -n 10 ionice -c2 -n7 taskset -c "$CORES")
mkdir -p "$WORK" "$OUT"
: > "$OUT/crash-$ENGINE.jsonl"
for i in $(seq 1 "$RUNS"); do
  timeout 300 "${RUN[@]}" "$BIN" crash --engine "$ENGINE" --label "$ENGINE-$i" --dir "$WORK/crash" --secs 3 "${EXTRA[@]}" \
    >> "$OUT/crash-$ENGINE.jsonl" 2>> "$OUT/crash-$ENGINE.err" || echo "crash run $i: exit $?" >> "$OUT/status"
done
rm -rf "$WORK/crash"
timeout 300 "${RUN[@]}" "$BIN" visibility --engine "$ENGINE" --dir "$WORK/vis" --secs 15 --collections 64 --hot 16 "${EXTRA[@]}" \
  > "$OUT/visibility-$ENGINE.jsonl" 2> "$OUT/visibility-$ENGINE.err" || echo "visibility: exit $?" >> "$OUT/status"
rm -rf "$WORK/vis"
python3 - "$OUT/crash-$ENGINE.jsonl" "$OUT/visibility-$ENGINE.jsonl" <<'PY'
import json, sys
crash = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
vis = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
lost = sum(c["lost_acked"] for c in crash); torn = sum(c["torn_batch"] for c in crash)
reopen = max((c["reopen_secs"] for c in crash), default=0)
print(f"crash: {len(crash)} runs, acked {sum(c['acked'] for c in crash)}, lost {lost}, torn {torn}, max reopen {reopen:.3f}s")
for v in vis:
    print(f"visibility: written {v['written']}, checked {v['checked']}, missing_acked {v['missing_acked']}, torn {v['torn_visible']}")
ok = crash and not lost and not torn and vis and all(v["missing_acked"] == 0 and v["torn_visible"] == 0 for v in vis)
print("PASS" if ok else "FAIL")
PY
