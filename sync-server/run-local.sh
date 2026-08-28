#!/bin/sh

set -eu

if [ -n "${ECOPASTE_LOCAL_DATA_DIR:-}" ]; then
  data_dir=$ECOPASTE_LOCAL_DATA_DIR
else
  case $(uname -s) in
    Darwin)
      data_dir="${HOME:?}/Library/Application Support/EcoPaste Sync Server"
      ;;
    MINGW* | MSYS* | CYGWIN*)
      data_dir="${LOCALAPPDATA:?}/EcoPaste Sync Server"
      ;;
    *)
      data_root=${XDG_DATA_HOME:-"${HOME:?}/.local/share"}
      data_dir="$data_root/ecopaste-sync-server"
      ;;
  esac
fi

mkdir -p "$data_dir"
echo "ECOPASTE_LOCAL_DATA_DIR=$data_dir"
cargo run --release -- --data-dir "$data_dir" --bind 0.0.0.0:4443
