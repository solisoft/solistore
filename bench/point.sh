#!/usr/bin/env bash
# The point-read phases side by side: gets and multi_gets of 10 and 20
# random documents, warm, on one thread and on THREADS, over the scan
# mode's collection (DOCS documents of ~430 B), then gets of documents
# still in the memtable. Prints ns per document read, instructions per
# document (Linux) and the per-call p50 per phase and engine.
#
#   bench/point.sh <binary> <work-dir> <out-dir> [engines...]
#
# Shares its data directories ($WORK/scan-<engine>) with scan.sh: loaded
# once and reused by later runs with the same DOCS. CORES pins the runs
# (taskset) when set.
set -u
BIN=$1 WORK=$2 OUT=$3; shift 3
ENGINES=("$@")
[ $# -eq 0 ] && ENGINES=(rocks solistore-eventual)
DOCS=${DOCS:-1000000} CACHE=${CACHE:-4096} GETS=${GETS:-2000000} THREADS=${THREADS:-8}
RUN=(nice -n 10 ionice -c2 -n7)
[ -n "${CORES:-}" ] && RUN+=(taskset -c "$CORES")
mkdir -p "$WORK" "$OUT"
for e in "${ENGINES[@]}"; do
  timeout 1800 "${RUN[@]}" "$BIN" point --engine "$e" --dir "$WORK/scan-$e" --docs "$DOCS" \
    --cache-mb "$CACHE" --settle-secs 20 --gets "$GETS" --threads "$THREADS" \
    ${ONLY:+--only "$ONLY"} > "$OUT/point-$e.jsonl" 2> "$OUT/point-$e.err" || echo "$e: exit $?"
done
python3 - "$OUT" "${ENGINES[@]}" <<'PY'
import json, sys
out, engines = sys.argv[1], sys.argv[2:]
rows = {}
for e in engines:
    for l in open(f"{out}/point-{e}.jsonl"):
        d = json.loads(l)
        if "ns_per_doc" in d:
            rows.setdefault(d["phase"], {})[e] = d
print(f"{'phase':22}" + "".join(f"{e[:18]:>20}{'instr':>7}{'p50':>8}" for e in engines))
print("  ns per document (thread time), instructions per document, per-call p50 in ns")
for phase, r in rows.items():
    cells = ""
    for e in engines:
        d = r.get(e)
        cells += f"{d['ns_per_doc']:20.0f}{d['instr_per_doc']:7.0f}{d['call_p50_ns']:8}" if d else f"{'-':>35}"
    print(f"{phase:22}" + cells)
PY
