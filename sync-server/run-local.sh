#!/bin/sh

set -eu

temporary_data_dir=$(mktemp -d "${TMPDIR:-/tmp}/ecopaste-sync.XXXXXX")

cleanup() {
  rm -rf -- "$temporary_data_dir"
}

trap cleanup EXIT HUP INT TERM

echo "ECOPASTE_LOCAL_DATA_DIR=$temporary_data_dir"
cargo run --release -- --data-dir "$temporary_data_dir" --bind 0.0.0.0:4443
