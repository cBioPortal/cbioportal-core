#!/usr/bin/env bash
set -euo pipefail

if [[ "${CBIOPORTAL_TEST_DB_SKIP:-}" == "1" || "${CBIOPORTAL_TEST_DB_SKIP:-}" == "true" ]]; then
  exit 0
fi

CONTAINER_NAME="${CBIOPORTAL_TEST_DB_CONTAINER:-cbioportal-core-test-mysql}"

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  docker rm -f "$CONTAINER_NAME"
fi
