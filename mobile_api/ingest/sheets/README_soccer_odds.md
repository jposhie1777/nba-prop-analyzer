# Soccer odds sheets sync

This flow lets you ingest multiple soccer odds tabs from one Google Sheet into a single BigQuery table for a unified odds board.

## Default tabs
- `EPL Odds`
- `La Liga Odds`
- `MLS Odds`

## Required column format (all tabs)
`Game, Start Time (ET), Home, Away, Bookmaker, Market, Outcome, Line, Price`

## Run manually
```bash
python mobile_api/ingest/sheets/sync_soccer_odds_to_bq.py
```

## API endpoints
- `POST /ingest/soccer/odds-from-sheets` to pull the latest sheet rows into BQ.
- `GET /soccer/odds-board` to query a unified board (optional filters: `league`, `market`, `bookmaker`, `limit`).

## Env vars
- `SOCCER_ODDS_SHEETS_SPREADSHEET_ID` (falls back to `SHEETS_SPREADSHEET_ID`)
- `SOCCER_ODDS_WORKSHEETS` (comma-separated, default `EPL Odds,La Liga Odds,MLS Odds`)
- `SOCCER_ODDS_BQ_DATASET` (default `soccer_data`)
- `SOCCER_ODDS_BQ_TABLE` (default `soccer_data.odds_lines`)
- `SOCCER_ODDS_TRUNCATE_BEFORE_LOAD` (default `true`)
- `SOCCER_ODDS_BQ_LOCATION` (default `US`)


## Scheduling
- In smart scheduler mode (`USE_SMART_SCHEDULER=true`), the API startup now runs this sync daily at **6:15 AM ET**.

- GitHub Actions alternative: `.github/workflows/soccer_odds_sheets_sync.yml` schedules the same sync around **6:15 AM ET** each day (UTC split for EST/EDT).
