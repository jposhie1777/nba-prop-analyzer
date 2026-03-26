# Pulse Sports Analytics
## EPL BigQuery daily pipeline

See `docs/epl_bigquery_daily_pipeline.md` for a once-daily flatten (5:00 AM ET) and analysis (5:30 AM ET) workflow.

## MLB PropFinder cron workflow

Run the full PropFinder MLB workflow (ingest then model) with one command:

```bash
python3 propfinder/run_workflow.py
```

Optional flags:

- `--setup` : run `setup_bq.py` before ingest/model
- `--skip-ingest` : skip ingest step
- `--skip-model` : skip model step
- `--lock-file /tmp/propfinder_workflow.lock` : custom lock path
- `--on-lock skip|fail` : behavior if another run is already active

Example cron (every 3 hours):

```cron
0 */3 * * * cd /workspace && /usr/bin/python3 propfinder/run_workflow.py >> /tmp/propfinder_workflow.log 2>&1
```

## KBO backfill

See `docs/kbo_backfill.md` for the KBO DailySchedule backfill workflow and BigQuery table layout.
