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
