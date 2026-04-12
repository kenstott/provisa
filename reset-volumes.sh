#!/usr/bin/env bash
# Destroy all Docker Compose volumes and recreate from scratch.
# WARNING: All persisted data (postgres, redis, minio, kafka, neo4j, etc.) will be lost.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILES="-f docker-compose.core.yml -f docker-compose.dev.yml"

echo "WARNING: This will destroy ALL data volumes. This cannot be undone."
echo "Press Ctrl+C within 5 seconds to abort..."
for i in 5 4 3 2 1; do printf "\r  %s..." "$i"; sleep 1; done
echo ""

cd "$SCRIPT_DIR"
docker compose $COMPOSE_FILES down --remove-orphans
docker compose $COMPOSE_FILES down -v --remove-orphans
echo "Volumes removed. Run start-ui.sh to reinitialize."
