#!/usr/bin/env bash
#
# One-command deploy for the BULKSTATS backend on the Hetzner box.
#
# Pulls the latest main, rebuilds ONLY the app image (which compiles the
# TypeScript), and recreates ONLY the app container. Postgres (db) and Caddy
# are left running so the database volume and TLS certs persist untouched.
#
# New DB tables are created automatically on app boot by initializeDatabase()
# (all CREATE TABLE IF NOT EXISTS), so there is no separate migration step.
#
# Usage (on the box, from the repo root — /home/deploy/bulk-terminal-backend):
#   ./deploy.sh
#
set -euo pipefail
cd "$(dirname "$0")"

echo "▶ Pulling latest main…"
git pull origin main

echo "▶ Building app image (compiles TypeScript)…"
docker compose build app

echo "▶ Recreating app container (db + caddy left running)…"
docker compose up -d app

echo "▶ Recent app logs:"
docker compose logs app --since 30s --tail 40 || true

echo
echo "✅ Deploy complete."
echo "   Verify:  curl -s https://api.bulkstats.com/api/explorer/throughput"
