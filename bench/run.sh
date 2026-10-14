#!/usr/bin/env bash
# RocksDB benchmark: SoliDB's prod profile on RocksDB 10.10.1 and 11.8.1, and
# candidate memory settings, each run REPS times in a fresh process and data
# directory; then one crash and one visibility test per configuration; then,
# for the configurations that matter most, one run at LARGE_DOCS documents,
# where index and filter memory has room to grow.
#
#   BIN10=<engine-bench built with rust-rocksdb 0.46> \
#   BIN11=<engine-bench built with rust-rocksdb 0.53> \
#   engine-bench/run.sh <work-dir> <results-dir> [reps]
#
# Runs pinned to 8 cores (taskset), at lowered CPU and I/O priority, so a
# shared machine is disturbed as little as possible. The data directories are
# deleted after each run; only the JSON lines are kept.
set -u

WORK=$1 OUT=$2 REPS=${3:-3}
CORES=${CORES:-16-23}
LARGE_DOCS=${LARGE_DOCS:-30000000}
: "${BIN10:?set BIN10}" "${BIN11:?set BIN11}"
mkdir -p "$WORK" "$OUT"

CACHED="--rx-cache-index 1"
PARTITIONED="$CACHED --rx-partitioned 1"
CAPPED="$PARTITIONED --rx-wbm-mb 512"

# label|binary|flags
CONFIGS=(
  "rocks10|$BIN10|"
  "rocks11|$BIN11|"
  "rocks11-cacheidx|$BIN11|$CACHED"
  "rocks11-partitioned|$BIN11|$PARTITIONED"
  "rocks11-capped|$BIN11|$CAPPED"
  "rocks11-capped-hcc|$BIN11|$CAPPED --rx-hcc 1"
  "rocks10-capped|$BIN10|$CAPPED"
)
LARGE=(
  "rocks10|$BIN10|"
  "rocks11|$BIN11|"
  "rocks11-capped|$BIN11|$CAPPED"
  "rocks10-capped|$BIN10|$CAPPED"
)

# An array, not a function: `timeout` cannot run shell functions.
RUN=(nice -n 10 ionice -c2 -n7 taskset -c "$CORES")

{
  echo "date: $(date -u +%FT%TZ)"
  echo "host: $(hostname)"
  echo "cpu: $(lscpu | sed -n 's/^Model name: *//p')"
  echo "cores: $CORES of $(nproc)"
  echo "kernel: $(uname -r)"
  echo "memory: $(free -g | awk '/Mem:/{print $2" GiB"}')"
  echo "fs: $(findmnt -no FSTYPE,SOURCE -T "$WORK")"
  echo "bin10: $(sha256sum "$BIN10" | cut -c1-16)"
  echo "bin11: $(sha256sum "$BIN11" | cut -c1-16)"
  echo "reps: $REPS, large: $LARGE_DOCS docs"
} > "$OUT/machine.txt"

one_run() { # label binary flags out-name extra-args...
  local label=$1 bin=$2 flags=$3 name=$4; shift 4
  rm -rf "$WORK/data"
  echo "$(date +%T) run $name"
  # shellcheck disable=SC2086
  timeout 3600 "${RUN[@]}" "$bin" run --engine rocks --label "$label" --dir "$WORK/data" $flags "$@" \
    > "$OUT/run-$name.jsonl" 2> "$OUT/run-$name.err"
  echo "  exit $?" | tee -a "$OUT/status"
}

for rep in $(seq 1 "$REPS"); do
  for cfg in "${CONFIGS[@]}"; do
    IFS='|' read -r label bin flags <<< "$cfg"
    one_run "$label" "$bin" "$flags" "$label-$rep"
  done
done

for cfg in "${CONFIGS[@]}"; do
  IFS='|' read -r label bin flags <<< "$cfg"
  echo "$(date +%T) crash + visibility $label"
  # shellcheck disable=SC2086
  timeout 300 "${RUN[@]}" "$bin" crash --engine rocks --label "$label" --dir "$WORK/crash" --secs 3 $flags \
    > "$OUT/crash-$label.jsonl" 2> "$OUT/crash-$label.err"
  # shellcheck disable=SC2086
  timeout 300 "${RUN[@]}" "$bin" visibility --engine rocks --label "$label" --dir "$WORK/vis" --secs 15 \
    --collections 64 --hot 16 $flags > "$OUT/visibility-$label.jsonl" 2> "$OUT/visibility-$label.err"
  rm -rf "$WORK/crash" "$WORK/vis"
done

for cfg in "${LARGE[@]}"; do
  IFS='|' read -r label bin flags <<< "$cfg"
  one_run "$label" "$bin" "$flags" "large-$label" --docs "$LARGE_DOCS"
done
rm -rf "$WORK/data"
echo "$(date +%T) done"
