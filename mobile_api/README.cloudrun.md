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
- A **schedule refresh job** runs once daily to write today's games to BigQuery.

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

**Schedule refresh job:**
- `BALLDONTLIE_API_KEY` (required)
- `GCP_PROJECT` or `GOOGLE_CLOUD_PROJECT` (required)

### IAM requirements

The gatekeeper job must be able to run the ingest job:

- Grant the gatekeeper job service account **roles/run.developer**
  (or a custom role with `run.jobs.run` and `run.executions.list`).

### One-click deploy (jobs + scheduler)

Use the script below to build the image, deploy all jobs, and create
the scheduler triggers.

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

Defaults (America/New_York):
- Daily schedule refresh at **5:30 AM ET**
- Gatekeeper cadence:
  - 6:00–11:59 AM: every 10 minutes
  - 12:00–2:59 PM: every 5 minutes
- 3:00 PM–3:59 AM: every 2 minutes

To customize, pass:
- `--schedule-refresh "30 6 * * *"` (daily refresh at 6:30 AM)
- `--schedule-early "*/15 6-11 * * *"` (slower mornings)
- `--schedule-pre "*/5 12-14 * * *"` (noon to 2:59 PM)
- `--schedule-live "*/2 15-23,0-3 * * *"` (3 PM to 3:59 AM)
- `--time-zone "America/New_York"`

The ingest job runs on a 60s interval while active.

### GitHub Actions deploy (recommended)

This lets you deploy **strictly from GitHub**. After setup, you only push
changes to GitHub and the workflow updates Cloud Run.

Workflow file:
- `.github/workflows/cloudrun_jobs_deploy.yml`

Required GitHub Secrets:
- `GOOGLE_APPLICATION_CREDENTIALS_JSON`
- `BALLDONTLIE_API_KEY`

Required GCP roles for the service account in the JSON:
- `roles/run.developer`
- `roles/cloudscheduler.admin`
- `roles/iam.serviceAccountUser`
- `roles/cloudbuild.builds.editor`

Notes:
- The workflow is **manual-only** (no auto deploys on push).
- Project and region are set in the workflow env block:
  `PROJECT_ID=graphite-flare-477419-h7`, `REGION=us-central1`.
  
To deploy on demand:
- GitHub UI: Actions → **Deploy Cloud Run Jobs** → Run workflow
- CLI: `gh workflow run "Deploy Cloud Run Jobs"`

### YAML-based deploy (jobs + scheduler)

Cloud Run Jobs and Cloud Scheduler also support YAML-based deploys.
You can use:

```bash
gcloud run jobs replace job.yaml
gcloud scheduler jobs import scheduler-job.yaml
```

Minimal Job YAML (template):

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: mobile-api-ingest
  namespace: YOUR_PROJECT
  labels:
    cloud.googleapis.com/location: YOUR_REGION
spec:
  template:
    template:
      containers:
        - image: gcr.io/YOUR_PROJECT/mobile-api-jobs:latest
          command: ["python", "-m", "jobs.ingest_runner"]
          env:
            - name: BALLDONTLIE_API_KEY
              value: "YOUR_KEY"
            - name: LIVE_INGEST_INTERVAL_SEC
              value: "60"
            - name: LIVE_INGEST_PRE_GAME_MINUTES
              value: "0"
            - name: LIVE_INGEST_JOB_MAX_MINUTES
              value: "10"
            - name: GCP_PROJECT
              value: "YOUR_PROJECT"
```

Minimal Scheduler YAML (template):

```yaml
name: projects/YOUR_PROJECT/locations/YOUR_REGION/jobs/mobile-api-gatekeeper-live
schedule: "*/2 15-23,0-3 * * *"
timeZone: "America/New_York"
httpTarget:
  uri: https://run.googleapis.com/v2/projects/YOUR_PROJECT/locations/YOUR_REGION/jobs/mobile-api-gatekeeper:run
  httpMethod: POST
  oauthToken:
    serviceAccountEmail: mobile-api-jobs-sa@YOUR_PROJECT.iam.gserviceaccount.com
    scope: https://www.googleapis.com/auth/cloud-platform
```

Replace the placeholders before running the commands.
