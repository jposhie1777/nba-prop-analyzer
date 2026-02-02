# Cloud Run deployment (mobile_api)

This service can run on Cloud Run with background ingestion loops enabled.
To keep the live ingest running without paying for Cloud Workstations, deploy
as a Cloud Run **service** with **CPU always allocated** and **min instances**.

## Required environment variables

- `BALLDONTLIE_API_KEY` (required)
- `USE_SMART_SCHEDULER=true` (required for smart live ingest)
- `ENABLE_LIVE_INGEST=true` (required for legacy mode; harmless with smart mode)

## Optional environment variables

- `LIVE_INGEST_INTERVAL_SEC` (default `60`)
- `LIVE_INGEST_PRE_GAME_MINUTES` (default `0`)
  - Set to `0` to start only when games go LIVE
  - Set to `15` (or any positive number) to start before tip-off
- `ENABLE_PLAYER_STATS_INGEST=true` (optional)
- `GCP_PROJECT` or `GOOGLE_CLOUD_PROJECT` (optional)

## Recommended Cloud Run settings

These settings keep **one** instance warm and allow background tasks to run
even when there are no HTTP requests.

- `--min-instances=1`
- `--max-instances=1` (avoids duplicate ingest loops)
- `--no-cpu-throttling` (CPU always allocated)

## Example deploy command

Replace `<PROJECT>`, `<REGION>`, and `<IMAGE>` with your values.

```bash
gcloud run deploy mobile-api \
  --project <PROJECT> \
  --region <REGION> \
  --image <IMAGE> \
  --platform managed \
  --allow-unauthenticated \
  --min-instances=1 \
  --max-instances=1 \
  --no-cpu-throttling \
  --set-env-vars USE_SMART_SCHEDULER=true,ENABLE_LIVE_INGEST=true \
  --set-env-vars BALLDONTLIE_API_KEY=YOUR_KEY \
  --set-env-vars LIVE_INGEST_INTERVAL_SEC=60,LIVE_INGEST_PRE_GAME_MINUTES=0
```

## Notes

- The ingest loops run only during live games when `LIVE_INGEST_PRE_GAME_MINUTES=0`.
- If Cloud Run scales to zero, background tasks will stop. Keep `min-instances=1`.
- The health check endpoint is `GET /health`.
