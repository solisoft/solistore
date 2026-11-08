#!/usr/bin/env bash
# The full-scan phases side by side: DOCS documents of ~430 B in one
# collection, read warm, with a block cache that holds them all and with
# SoliDB's 512 MiB. Prints the median and best pass per phase, engine and
# cache size.
#
#   bench/scan.sh <binary> <work-dir> <out-dir> [engines...]
#
# Each engine's data directory ($WORK/scan-<engine>) is loaded once and
# reused by later runs with the same DOCS, so every binary reads the same
# tables; delete it to load again. CORES pins the runs (taskset) when set.
set -u
BIN=$1 WORK=$2 OUT=$3; shift 3
ENGINES=("$@")
[ $# -eq 0 ] && ENGINES=(rocks solistore-eventual)
DOCS=${DOCS:-1000000} PASSES=${PASSES:-7} CACHES=${CACHES:-4096 512}
RUN=(nice -n 10 ionice -c2 -n7)
[ -n "${CORES:-}" ] && RUN+=(taskset -c "$CORES")
mkdir -p "$WORK" "$OUT"
for cache in $CACHES; do
  for e in "${ENGINES[@]}"; do
    timeout 1800 "${RUN[@]}" "$BIN" scan --engine "$e" --dir "$WORK/scan-$e" --docs "$DOCS" \
      --passes "$PASSES" --cache-mb "$cache" --settle-secs 20 --gets "${GETS:-1000000}" \
      > "$OUT/scan-$e-$cache.jsonl" 2> "$OUT/scan-$e-$cache.err" || echo "$e $cache: exit $?"
  done
done
python3 - "$OUT" "$CACHES" "${ENGINES[@]}" <<'PY'
import json, sys
out, caches, engines = sys.argv[1], sys.argv[2].split(), sys.argv[3:]
print(f"{'phase':20}{'cache':>7}" + "".join(f"{e[:18]:>20}{'best':>8}" for e in engines))
print("  scans: ms per pass, median and best; point reads: thousands of documents a second, p50/p99 us")
rows = {}
for c in caches:
    for e in engines:
        for l in open(f"{out}/scan-{e}-{c}.jsonl"):
            d = json.loads(l)
            if "pass_ms" in d:
                rows.setdefault((d["phase"], c), {})[e] = f"{d['p50_us'] / 1000:20.1f}{min(d['pass_ms']):8.1f}"
            elif d["phase"].startswith(("get_", "multi_get_")):
                rows.setdefault((d["phase"], c), {})[e] = f"{d['ops_per_sec'] / 1000:20.0f}{str(d['p50_us']) + '/' + str(d['p99_us']):>8}"
for (phase, c), r in rows.items():
    print(f"{phase:20}{c:>7}" + "".join(r.get(e, f"{'-':>28}") for e in engines))
PY
