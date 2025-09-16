#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")"/.. && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
PROJECT_NAME="realtime-ju"

# 기본 프로파일은 collector + dev. 필요시 인자로 다른 프로파일을 넘길 수 있음.
if [ "$#" -eq 0 ]; then
  PROFILES=("collector" "dev")
else
  PROFILES=("$@")
fi

PROFILE_ARGS=()
for p in "${PROFILES[@]}"; do
  PROFILE_ARGS+=(--profile "$p")
done

docker compose -f "$COMPOSE_FILE" --project-name "$PROJECT_NAME" "${PROFILE_ARGS[@]}" up -d


