# BULKSTATS backend — ops & deploy notes

Node + TypeScript service (Express REST + `ws`) that proxies/indexes BULK for the
bulkstats frontend. Express routes in `src/routes`, logic in `src/services`,
cron/background workers in `src/jobs`, Postgres schema + helpers in `src/db`.

## Deployment: Hetzner (NOT Railway)

Production runs on a **Hetzner** box via Docker Compose. **Railway is only a
secondary/fallback and is not the live path.**

- **Host:** `deploy@178.105.22.22` (key-based SSH)
- **Repo on box:** `/home/deploy/bulk-terminal-backend`
- **Compose services:** `app` (this repo, built from `Dockerfile`, listens on 3001),
  `db` (postgres:16, persistent `pgdata` volume), `caddy` (auto-TLS, ports 80/443).
- **Caddy** reverse-proxies `api.bulkstats.com` → `app:3001` (see `Caddyfile`).
- **Frontend** (`momotav/bulk-terminal`, deployed on Vercel) calls this backend at
  `https://api.bulkstats.com` via the `NEXT_PUBLIC_API_URL` env var.

### To deploy
1. Push changes to `main` on `github.com/momotav/bulk-terminal-backend` (the box
   pulls from here).
2. On the box, from the repo root, run **`./deploy.sh`** — it pulls, rebuilds
   ONLY the `app` image, and recreates ONLY the `app` container. **`db` and
   `caddy` are left running** so data + certs persist.

Equivalent manual steps: `git pull origin main && docker compose build app && docker compose up -d app`.

### DB migrations
None manual. All tables are `CREATE TABLE IF NOT EXISTS` inside
`initializeDatabase()` (`src/db/index.ts`) and run on every app boot.

### Secrets
`.env` on the box holds Postgres + app secrets (Compose reads it via `env_file`).
It is **not** in the repo and must not be committed or overwritten.

## Network metrics (analytics Network page)
- Tables: `network_metrics` (60s throughput/block-time snapshots) and
  `network_action_metrics` (sampled per-type action/tx counts).
- Writer: `src/jobs/networkMetricsCollector.ts` (60s cron, started in `index.ts`).
- Read endpoints: `GET /api/explorer/network-history|action-breakdown|action-history`.
- History builds **forward from deploy** — there is no backfill.

## Backlog / known issues
- Pre-existing log noise: `relation "global_stats" does not exist` — unrelated to
  network metrics; a `global_stats` table is referenced but never created.
- Optional: prune raw `network_metrics` / `network_action_metrics` older than ~90
  days (a nightly job) once volume grows. Not needed yet.
