# Cloud Run deployment (mobile_api)

This service can run on Cloud Run in two modes:

- **Option A (always-on service):** Run the API + ingest loops as a Cloud Run
  service with min instances + CPU always allocated.
- **Option B (near-zero outside games):** Run ingest as Cloud Run **jobs** and
  trigger them via **Cloud Scheduler** only during live windows.

## Required environment variables (both options)

- `BALLDONTLIE_API_KEY` (required)
- `USE_SMART_SCHEDULER=true` (required for smart live ingest)
- `ENABLE_LIVE_INGEST=true` (required for legacy mode; harmless with smart mode)

## Optional environment variables (both options)

- `LIVE_INGEST_INTERVAL_SEC` (default `60`)
- `LIVE_INGEST_PRE_GAME_MINUTES` (default `0`)
  - Set to `0` to start only when games go LIVE
  - Set to `15` (or any positive number) to start before tip-off
- `ENABLE_PLAYER_STATS_INGEST=true` (optional)
- `GCP_PROJECT` or `GOOGLE_CLOUD_PROJECT` (optional)

## Option A: Always-on service (simple, but not zero-cost)

These settings keep **one** instance warm and allow background tasks to run
even when there are no HTTP requests.

- `--min-instances=1`
- `--max-instances=1` (avoids duplicate ingest loops)
- `--no-cpu-throttling` (CPU always allocated)

### Example deploy command

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

### Notes

- The ingest loops run only during live games when `LIVE_INGEST_PRE_GAME_MINUTES=0`.
- If Cloud Run scales to zero, background tasks will stop. Keep `min-instances=1`.
- The health check endpoint is `GET /health`.

## Option B: Cloud Run Jobs + Cloud Scheduler (near-zero outside games)

This option avoids paying for a 24/7 instance. Instead:

- A **gatekeeper job** runs on a fixed schedule (e.g. every 2–5 minutes).
- It checks for live games (or a pre-game window) and only then starts
  the **ingest runner job**.
- The ingest runner does cycles every 60 seconds and exits after a short
  duration (default 10 minutes).

### Job environment variables

**Ingest runner job:**
- `BALLDONTLIE_API_KEY` (required)
- `LIVE_INGEST_INTERVAL_SEC` (default `60`)
- `LIVE_INGEST_PRE_GAME_MINUTES` (default `0`)
- `LIVE_INGEST_JOB_MAX_MINUTES` (default `10`)
- `ENABLE_PLAYER_STATS_INGEST=true` (optional)

**Gatekeeper job:**
- `BALLDONTLIE_API_KEY` (required)
- `LIVE_INGEST_PRE_GAME_MINUTES` (default `0`)
- `CLOUD_RUN_REGION` (required)
- `CLOUD_RUN_INGEST_JOB` (required, name of ingest runner job)
- `GCP_PROJECT` or `GOOGLE_CLOUD_PROJECT` (required)

### IAM requirements

The gatekeeper job must be able to run the ingest job:

- Grant the gatekeeper job service account **roles/run.developer**
  (or a custom role with `run.jobs.run` and `run.executions.list`).

### One-click deploy (jobs + scheduler)

Use the script below to build the image, deploy both jobs, and create
the scheduler trigger.

```bash
cd mobile_api
chmod +x deploy_cloudrun_jobs.sh

PROJECT_ID="<PROJECT>"
REGION="<REGION>"
BALLDONTLIE_API_KEY="YOUR_KEY"

./deploy_cloudrun_jobs.sh \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --bdl-key "$BALLDONTLIE_API_KEY"
```

Notes:
- Default scheduler cadence is every 2 minutes. To change it:
  `--schedule "*/1 * * * *"` (every minute) or `--schedule "*/5 * * * *"`.
- The ingest job runs on a 60s interval while active.
