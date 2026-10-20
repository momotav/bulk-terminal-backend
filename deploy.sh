#!/usr/bin/env bash
#
# One-command deploy for the BULKSTATS backend on the Hetzner box.
#
# Pulls the latest main, rebuilds ONLY the app image (which compiles the
# TypeScript), and recreates ONLY the app container. Postgres (db) is left
# running so the database volume persists untouched. Caddy is left running
# too, UNLESS the Caddyfile changed in this pull (see below).
#
# New DB tables are created automatically on app boot by initializeDatabase()
# (all CREATE TABLE IF NOT EXISTS), so there is no separate migration step.
#
# Usage (on the box, from the repo root — /home/deploy/bulk-terminal-backend):
#   ./deploy.sh
#
set -euo pipefail

# The whole script lives in a function that is only CALLED on the last line.
# bash reads scripts incrementally while executing them, so when `git pull`
# below rewrites deploy.sh mid-run, a plain top-to-bottom script would execute
# a mix of old and new lines (that is exactly what happened the first time the
# Caddy step was added: the pull brought it in, but the running copy never ran
# it). A function body is parsed in full before any of it runs, so the version
# that started is the version that finishes; the new one applies next deploy.
main() {
  cd "$(dirname "$0")"

  echo "▶ Pulling latest main…"
  local before
  before="$(git rev-parse HEAD)"
  git pull origin main
  local after
  after="$(git rev-parse HEAD)"

  echo "▶ Building app image (compiles TypeScript)…"
  docker compose build app

  echo "▶ Recreating app container (db + caddy left running)…"
  docker compose up -d app

  # The Caddyfile is bind-mounted into the caddy container AS A SINGLE FILE.
  # git writes files by rename, so a pull that touches it leaves the container's
  # mount pointing at the OLD inode — `caddy reload` then re-reads stale config
  # and nothing changes. The only reliable way to pick up a new Caddyfile is to
  # recreate the container, which re-resolves the bind mount. ~2-3s of downtime,
  # and the TLS certs survive (they live in the caddy_data volume, not the
  # container). Skipped entirely when the Caddyfile didn't change.
  if ! git diff --quiet "$before" "$after" -- Caddyfile; then
    echo "▶ Caddyfile changed in this pull — recreating caddy (certs persist in volume)…"
    docker compose up -d --force-recreate caddy
  else
    echo "▶ Caddyfile unchanged — caddy left running."
  fi

  echo "▶ Recent app logs:"
  docker compose logs app --since 30s --tail 40 || true

  echo
  echo "✅ Deploy complete."
  echo "   Verify:  curl -s https://api.bulkstats.com/api/explorer/throughput"
}

main "$@"
