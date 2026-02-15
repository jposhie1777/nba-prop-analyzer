# Daily matchups to Google Sheets

This script pulls **today's PGA and ATP matchups** and writes them to two
worksheets in a Google Sheet.

## Usage

```bash
python -m mobile_api.scripts.daily_matchups_sheet \
  --sheet-id "<SHEET_ID>" \
  --service-account "$GCP_SERVICE_ACCOUNT"
```

### Optional flags

- `--date YYYY-MM-DD` (defaults to today in `America/New_York`)
- `--pga-tab "Golf Matchups"` (worksheet name)
- `--atp-tab "Tennis Matchups"` (worksheet name)
- `--pga-source api|bq` (defaults to `api`)
- `--max-pages 10`
- `--dry-run`

## Credentials

Provide a service account JSON string (or file path):

- `--service-account "$GCP_SERVICE_ACCOUNT"`
- or set `GOOGLE_APPLICATION_CREDENTIALS` to a file path

The sheet ID can come from:

- `--sheet-id "<SHEET_ID>"`
- or `MATCHUPS_SHEET_ID` / `SPREADSHEET_ID`

