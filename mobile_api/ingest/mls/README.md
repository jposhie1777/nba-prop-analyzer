# MLS ingest DDL vs hard reset

## Which should I run?

- **Normal usage (safe):** run `python -m mobile_api.ingest.mls.apply_ddl`
  - Creates missing dataset/tables.
  - Reconciles schema to the expected MLS ingest shape.
  - Intended for regular backfills and refresh jobs.

- **Break-glass usage (destructive):** run `mobile_api/ingest/mls/hard_reset.sql`
  - Drops and recreates all MLS ingest tables.
  - Deletes all historical data currently in those tables.
  - Use only when a table is in a bad state (for example schema/type drift) and a normal `apply_ddl` + backfill is not recovering.

## Recommended recovery flow

1. Run `python -m mobile_api.ingest.mls.apply_ddl`.
2. Retry MLS backfill.
3. If the same schema/type error persists, run the hard reset SQL once.
4. Run MLS backfill again.
5. Continue using `apply_ddl` as the normal pre-backfill step.


## True wipe + backfill (mlssoccer website ingest)

- Use the backfill CLI with `--truncate-first` to empty the five mlssoccer raw tables before repopulating:
  - `python -m mobile_api.ingest.mls.mls_website_ingest --mode backfill --start-season 2024 --end-season 2026 --truncate-first` (or `--wipe`)
- Safety guard: if `--dry-run` is also provided, truncate is skipped and only fetch/count behavior runs.

- `--wipe` is a shorthand alias for `--truncate-first`.

## Oddspedia MLS -> BigQuery workflow

Use this workflow to load saved Oddspedia MLS captures into dataset `oddspedia` with `mls_`-prefixed tables.

Command:

```bash
python -m mobile_api.ingest.mls.oddspedia_mls_bq_workflow --input-dir website_responses/mls
```

Dry-run (parse + row counts only, no BigQuery write):

```bash
python -m mobile_api.ingest.mls.oddspedia_mls_bq_workflow --input-dir website_responses/mls --dry-run
```

Default target dataset/location:
- dataset: `oddspedia`
- location: `US`

Override with env vars:
- `ODDSPEDIA_DATASET`
- `ODDSPEDIA_BQ_LOCATION`
- `GCP_PROJECT` (or `GOOGLE_CLOUD_PROJECT`)

Tables created and loaded:
- `mls_odds_1x2`
- `mls_odds_btts`
- `mls_odds_draw_no_bet`
- `mls_odds_double_chance`
- `mls_odds_european_handicap`
- `mls_odds_total_corners`
- `mls_match_info`
- `mls_match_keys`
- `mls_statistics_tokens`

Behavior:
- Automatically creates dataset/table if missing.
- Truncates each target table before loading the latest snapshot.

## GitHub Actions access

A workflow is available in GitHub Actions as **Oddspedia MLS Ingest** (`.github/workflows/oddspedia_mls_ingest.yml`).

It supports:
- Manual run via **Actions → Oddspedia MLS Ingest → Run workflow**
- Optional daily schedule
- Inputs:
  - `input_dir` (defaults to `website_responses/mls`)
  - `dry_run` (parse only, no BigQuery writes)

Required secrets:
- `PROJECT_ID`
- `GOOGLE_APPLICATION_CREDENTIALS_JSON` (needed for non-dry-run writes)
