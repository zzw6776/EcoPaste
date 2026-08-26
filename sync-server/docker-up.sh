#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
builder_name="ecopaste-sync-slim"
cache_limit="3gb"
image_name="ecopaste-sync-server:local"
cache_pruned=false

prune_builder_cache() {
  docker buildx prune \
    --builder "$builder_name" \
    --force \
    --max-used-space "$cache_limit"
}

cleanup_builder() {
  if [ "$cache_pruned" != "true" ]; then
    prune_builder_cache >/dev/null 2>&1 || true
  fi
  docker buildx stop "$builder_name" >/dev/null 2>&1 || true
}

trap cleanup_builder EXIT HUP INT TERM

previous_image_id=$(docker image inspect "$image_name" --format '{{.Id}}' 2>/dev/null || true)

if ! docker buildx inspect "$builder_name" >/dev/null 2>&1; then
  docker buildx create --name "$builder_name" --driver docker-container >/dev/null
fi
docker buildx inspect "$builder_name" --bootstrap >/dev/null
docker buildx build \
  --builder "$builder_name" \
  --load \
  --progress plain \
  --tag "$image_name" \
  "$script_dir"
docker compose \
  --project-directory "$script_dir" \
  --file "$script_dir/docker-compose.yml" \
  up --detach --no-build

current_image_id=$(docker image inspect "$image_name" --format '{{.Id}}')
if [ -n "$previous_image_id" ] && [ "$previous_image_id" != "$current_image_id" ]; then
  docker image rm "$previous_image_id" >/dev/null
fi
docker image prune \
  --force \
  --filter "label=io.github.ecopaste.component=sync-server" >/dev/null
prune_builder_cache >/dev/null
cache_pruned=true
