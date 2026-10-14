#!/bin/bash
# SoliDB server on each storage engine, same binary, interleaved runs.
# Usage: solidb-engine-bench.sh <out_dir> [docs] [workers] [secs] [reps]
set -u
OUT=$1; DOCS=${2:-1000000}; WORKERS=${3:-64}; SECS=${4:-15}; REPS=${5:-2}
BIN=~/src/soli/db-kv/target/release/solidb
BENCH=~/src/soli/db-kv/target/release/examples/engine_bench
ROOT=~/solidb-engine-bench; PORT=6851; PW=bench-pw
SERVER_CPUS=16-23; CLIENT_CPUS=24-31
mkdir -p "$OUT" "$ROOT"

start_server() { # engine data_dir log
  SOLIDB_ENGINE=$1 SOLIDB_ADMIN_PASSWORD=$PW RUST_LOG=warn \
    taskset -c $SERVER_CPUS "$BIN" --port $PORT --data-dir "$2" > "$3" 2>&1 &
  PID=$!
  local t0=$(date +%s.%N)
  for _ in $(seq 1 600); do
    (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break
    sleep 0.05
  done
  READY=$(awk -v a="$(date +%s.%N)" -v b="$t0" 'BEGIN{printf "%.3f", a-b}')
}

stop_server() {
  kill -TERM $PID; wait $PID 2>/dev/null
}

for rep in $(seq 1 $REPS); do
  for engine in rocksdb solistore; do
    tag=$engine-$rep; data=$ROOT/data-$tag
    rm -rf "$data"
    start_server $engine "$data" "$OUT/server-$tag.log"
    taskset -c $CLIENT_CPUS "$BENCH" 127.0.0.1:$PORT $PW $DOCS $WORKERS $SECS \
      > "$OUT/run-$tag.jsonl" 2> "$OUT/run-$tag.err"
    hwm=$(grep VmHWM /proc/$PID/status | awk '{print $2}')
    rss=$(grep VmRSS /proc/$PID/status | awk '{print $2}')
    stop_server
    disk=$(du -sk "$data" | cut -f1)
    start_server $engine "$data" "$OUT/server-$tag-reopen.log"
    reopen=$READY
    stop_server
    echo "{\"engine\":\"$engine\",\"rep\":$rep,\"hwm_kb\":$hwm,\"rss_kb\":$rss,\"disk_kb\":$disk,\"reopen_s\":$reopen,\"engine_file\":\"$(cat $data/ENGINE 2>/dev/null | tr -d '\n')\"}" >> "$OUT/summary.jsonl"
    rm -rf "$data"
  done
done
