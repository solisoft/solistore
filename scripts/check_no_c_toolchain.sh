#!/usr/bin/env bash
# Fails if anything in solistore's dependency graph, on any target, needs a C
# or C++ compiler to build. That is RocksDB's cost this crate exists to remove
# (CI spent 95 minutes compiling SoliDB against 18 seconds of tests), and one
# careless dependency would bring it back without anyone noticing.
#
# `cc`, `cmake` and `bindgen` are what every *-sys crate builds through, so
# their absence from the graph is the property; crate names ending in -sys are
# not (windows-sys and linux-raw-sys are plain Rust).
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
for tool in cc cmake bindgen; do
    if out=$(cargo tree -p solistore --target all -e normal,build -i "$tool" 2>/dev/null); then
        echo "FAIL: solistore depends on \`$tool\`:" >&2
        echo "$out" >&2
        status=1
    else
        echo "  ok   no \`$tool\` in the dependency graph"
    fi
done
exit $status
