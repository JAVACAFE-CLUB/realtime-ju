#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")"/.. && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
PROJECT_NAME="realtime-ju"

docker compose -f "$COMPOSE_FILE" --project-name "$PROJECT_NAME" down -v --remove-orphans


