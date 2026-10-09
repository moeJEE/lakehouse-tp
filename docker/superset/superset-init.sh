#!/usr/bin/env bash
set -e
superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname Lakehouse \
  --email admin@example.org --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true
superset init
exec /usr/bin/run-server.sh
