# KBO Backfill Workflow

This repository now includes a GitHub Action for KBO game-history backfills:

- Workflow: `.github/workflows/kbo_backfill.yml`
- Ingest module: `mobile_api.ingest.kbo.backfill`
- DDL module: `mobile_api.ingest.kbo.apply_ddl`

## What it creates in BigQuery

Dataset:
- `kbo_data`

Tables:
- `kbo_data.games` — one row per game (`game_date`, teams, score, outcome, status)
- `kbo_data.team_summary` — season + team rollups (W/L/T, runs scored/allowed, averages)

## Manual run (local)

```bash
python -m mobile_api.ingest.kbo.apply_ddl
python -m mobile_api.ingest.kbo.backfill --start-month 2024-01 --end-month 2025-12 --truncate-first
```

Use `--dry-run` to fetch/parse only.
