#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
image_name="zzw6776/ecopaste-sync-server:${ECOPASTE_SYNC_IMAGE_TAG:-latest}"
legacy_container_name="ecopaste-sync-persistent"
data_volume_name="ecopaste-sync-data"
service_name="ecopaste-sync"
service_port="4443"

compose() {
  docker compose \
    --project-directory "$script_dir" \
    --file "$script_dir/docker-compose.yml" \
    "$@"
}

is_compose_container() {
  candidate_id=$1

  for compose_id in $(compose ps --all --quiet "$service_name" 2>/dev/null || true); do
    if [ "$candidate_id" = "$compose_id" ]; then
      return 0
    fi
  done

  return 1
}

prepare_service_port() {
  if docker container inspect "$legacy_container_name" >/dev/null 2>&1; then
    echo "Removing legacy EcoPaste sync container: $legacy_container_name"
    docker container rm --force "$legacy_container_name" >/dev/null
  fi

  for container_id in $(docker ps --no-trunc --quiet); do
    published_ports=$(docker inspect \
      --format '{{with index .NetworkSettings.Ports "4443/udp"}}{{range .}}{{println .HostPort}}{{end}}{{end}}' \
      "$container_id")
    for published_port in $published_ports; do
      if [ "$published_port" != "$service_port" ] || is_compose_container "$container_id"; then
        continue
      fi

      container_name=$(docker inspect --format '{{.Name}}' "$container_id")
      container_name=${container_name#/}
      echo "UDP port $service_port is already used by Docker container: $container_name" >&2
      echo "Stop that container before starting EcoPaste sync server." >&2
      exit 1
    done
  done
}

ensure_data_volume() {
  if docker volume inspect "$data_volume_name" >/dev/null 2>&1; then
    return
  fi

  echo "Creating persistent EcoPaste sync data volume: $data_volume_name"
  docker volume create "$data_volume_name" >/dev/null
}

ensure_data_volume
prepare_service_port

previous_image_id=$(docker image inspect "$image_name" --format '{{.Id}}' 2>/dev/null || true)

compose pull "$service_name"
compose up --detach --force-recreate --no-build

current_image_id=$(docker image inspect "$image_name" --format '{{.Id}}')
if [ -n "$previous_image_id" ] && [ "$previous_image_id" != "$current_image_id" ]; then
  docker image rm "$previous_image_id" >/dev/null
fi
docker image prune \
  --force \
  --filter "label=io.github.ecopaste.component=sync-server" >/dev/null
