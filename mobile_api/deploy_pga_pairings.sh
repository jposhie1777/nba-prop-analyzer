#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy_pga_pairings.sh
#
# Builds the container image, deploys the PGA pairings Cloud Run Job, and
# creates/updates the five Cloud Scheduler triggers that fire it on the
# correct days and times.
#
# Round publication schedule (all America/New_York)
# -------------------------------------------------
#   Wednesday  10 am – 11 pm  hourly       R1 + R2 publication window
#   Thursday   8 am            once        R1 safety refresh
#   Friday     8 am            once        R2 safety refresh
#   Saturday   2 pm –  9 pm   every 30 min R3 published after R2 ends
#   Sunday     2 pm –  9 pm   every 30 min R4 published after R3 ends
#
# Usage
# -----
#   ./deploy_pga_pairings.sh \
#       --project  my-gcp-project \
#       --region   us-central1 \
#       --tournament R2026010
#
# Update --tournament at the start of each week to the current tournament ID.
# Find it in the URL on pgatour.com, e.g. R2026010.
# ---------------------------------------------------------------------------
set -euo pipefail

# ── Defaults ────────────────────────────────────────────────────────────────
PROJECT=""
REGION=""
TOURNAMENT_ID=""
IMAGE=""
JOB_NAME="pga-pairings-job"
SA_NAME="mobile-api-jobs-sa"
SA_EMAIL_OVERRIDE=""
TIME_ZONE="America/New_York"
DRY_RUN="false"

# Cloud Scheduler job names
SCHED_WED="pga-pairings-wednesday"
SCHED_THU="pga-pairings-thursday"
SCHED_FRI="pga-pairings-friday"
SCHED_SAT="pga-pairings-saturday"
SCHED_SUN="pga-pairings-sunday"

# Cron expressions (America/New_York)
#   Wed: every hour 10 am – 11 pm
#   Thu: 8 am only
#   Fri: 8 am only
#   Sat: every 30 min 2 pm – 9 pm  (R3 published after R2 finishes ~3–6 pm ET)
#   Sun: every 30 min 2 pm – 9 pm  (R4 published after R3 finishes ~3–7 pm ET)
CRON_WED="0 10-23 * * 3"
CRON_THU="0 8 * * 4"
CRON_FRI="0 8 * * 5"
CRON_SAT="*/30 14-21 * * 6"
CRON_SUN="*/30 14-21 * * 0"

# ── Argument parsing ─────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: $0 --project <PROJECT> --region <REGION> --tournament <ID> [options]

Required:
  --project    <PROJECT>   GCP project ID
  --region     <REGION>    Cloud Run / Scheduler region (e.g. us-central1)
  --tournament <ID>        Current PGA Tour tournament ID (e.g. R2026010)
                           Update this each week. Find it in the URL on pgatour.com.

Optional:
  --image      <IMAGE>     Container image (default: gcr.io/<PROJECT>/mobile-api-jobs:latest)
  --job-name   <NAME>      Cloud Run Job name (default: ${JOB_NAME})
  --sa-name    <NAME>      Service account name (default: ${SA_NAME})
  --sa-email   <EMAIL>     Use existing SA email (skips create)
  --dry-run                Deploy with PGA_DRY_RUN=true (fetches but won't write to BQ)
  --skip-build             Skip image build (use existing image)
  --time-zone  <TZ>        Scheduler timezone (default: ${TIME_ZONE})

Scheduler name overrides:
  --sched-wed  <NAME>      (default: ${SCHED_WED})
  --sched-thu  <NAME>      (default: ${SCHED_THU})
  --sched-fri  <NAME>      (default: ${SCHED_FRI})
  --sched-sat  <NAME>      (default: ${SCHED_SAT})
  --sched-sun  <NAME>      (default: ${SCHED_SUN})
EOF
}

SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)    PROJECT="$2";        shift 2 ;;
    --region)     REGION="$2";         shift 2 ;;
    --tournament) TOURNAMENT_ID="$2";  shift 2 ;;
    --image)      IMAGE="$2";          shift 2 ;;
    --job-name)   JOB_NAME="$2";       shift 2 ;;
    --sa-name)    SA_NAME="$2";        shift 2 ;;
    --sa-email)   SA_EMAIL_OVERRIDE="$2"; shift 2 ;;
    --dry-run)    DRY_RUN="true";      shift 1 ;;
    --skip-build) SKIP_BUILD=true;     shift 1 ;;
    --time-zone)  TIME_ZONE="$2";      shift 2 ;;
    --sched-wed)  SCHED_WED="$2";      shift 2 ;;
    --sched-thu)  SCHED_THU="$2";      shift 2 ;;
    --sched-fri)  SCHED_FRI="$2";      shift 2 ;;
    --sched-sat)  SCHED_SAT="$2";      shift 2 ;;
    --sched-sun)  SCHED_SUN="$2";      shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "$PROJECT" || -z "$REGION" || -z "$TOURNAMENT_ID" ]]; then
  echo "ERROR: --project, --region, and --tournament are required."
  usage; exit 1
fi

[[ -z "$IMAGE" ]] && IMAGE="gcr.io/${PROJECT}/mobile-api-jobs:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Build ────────────────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> Building image ${IMAGE}"
  BUILD_ID=$(gcloud builds submit \
    --project "${PROJECT}" \
    --tag "${IMAGE}" \
    --async \
    --format="value(id)" \
    "${SCRIPT_DIR}")

  echo "==> Build started: ${BUILD_ID}"
  while true; do
    STATUS=$(gcloud builds describe "${BUILD_ID}" \
      --project "${PROJECT}" \
      --format="value(status)")
    case "${STATUS}" in
      SUCCESS)  echo "==> Build completed"; break ;;
      FAILURE|CANCELLED|TIMEOUT)
        echo "==> Build failed: ${STATUS}"; exit 1 ;;
      *) echo "==> Build status: ${STATUS} (waiting...)"; sleep 5 ;;
    esac
  done
else
  echo "==> Skipping build (--skip-build)"
fi

# ── Service account ──────────────────────────────────────────────────────────
SA_EMAIL="${SA_EMAIL_OVERRIDE}"
if [[ -z "$SA_EMAIL" ]]; then
  SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
  echo "==> Ensuring service account ${SA_EMAIL}"
  if ! gcloud iam service-accounts describe "${SA_EMAIL}" \
       --project "${PROJECT}" >/dev/null 2>&1; then
    gcloud iam service-accounts create "${SA_NAME}" --project "${PROJECT}" || true
  fi
fi

bind_role() {
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member "serviceAccount:${SA_EMAIL}" \
    --role "$1" >/dev/null 2>&1 \
  || echo "==> WARNING: could not grant $1 (may already exist)"
}
echo "==> Granting IAM roles (best-effort)"
bind_role "roles/run.invoker"
bind_role "roles/bigquery.dataEditor"
bind_role "roles/bigquery.jobUser"

# ── Cloud Run Job ────────────────────────────────────────────────────────────
echo "==> Deploying Cloud Run Job: ${JOB_NAME}"
gcloud run jobs deploy "${JOB_NAME}" \
  --project  "${PROJECT}" \
  --region   "${REGION}" \
  --image    "${IMAGE}" \
  --command  "python" \
  --args     "-m,jobs.pga_pairings_job" \
  --service-account "${SA_EMAIL}" \
  --set-env-vars "GCP_PROJECT=${PROJECT},PGA_CURRENT_TOURNAMENT_ID=${TOURNAMENT_ID},PGA_DRY_RUN=${DRY_RUN}" \
  --max-retries 2 \
  --task-timeout 300

JOB_URI="https://run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${JOB_NAME}:run"

# ── Cloud Scheduler helpers ──────────────────────────────────────────────────
upsert_scheduler() {
  local name="$1"
  local cron="$2"
  local desc="$3"

  if gcloud scheduler jobs describe "${name}" \
     --project "${PROJECT}" --location "${REGION}" >/dev/null 2>&1; then
    echo "==> Updating scheduler: ${name}  (${cron})"
    gcloud scheduler jobs update http "${name}" \
      --project  "${PROJECT}" \
      --location "${REGION}" \
      --schedule "${cron}" \
      --time-zone "${TIME_ZONE}" \
      --uri "${JOB_URI}" \
      --http-method POST \
      --oauth-service-account-email "${SA_EMAIL}" \
      --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" \
      --description "${desc}"
  else
    echo "==> Creating scheduler: ${name}  (${cron})"
    gcloud scheduler jobs create http "${name}" \
      --project  "${PROJECT}" \
      --location "${REGION}" \
      --schedule "${cron}" \
      --time-zone "${TIME_ZONE}" \
      --uri "${JOB_URI}" \
      --http-method POST \
      --oauth-service-account-email "${SA_EMAIL}" \
      --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" \
      --description "${desc}"
  fi
}

echo ""
echo "==> Creating/updating Cloud Scheduler jobs (tz: ${TIME_ZONE})"
upsert_scheduler "${SCHED_WED}" "${CRON_WED}" \
  "PGA pairings - Wednesday R1+R2 publish window (hourly 10am-11pm ET)"

upsert_scheduler "${SCHED_THU}" "${CRON_THU}" \
  "PGA pairings - Thursday R1 safety refresh (8am ET)"

upsert_scheduler "${SCHED_FRI}" "${CRON_FRI}" \
  "PGA pairings - Friday R2 safety refresh (8am ET)"

upsert_scheduler "${SCHED_SAT}" "${CRON_SAT}" \
  "PGA pairings - Saturday R3 publish window (every 30min 2-9pm ET)"

upsert_scheduler "${SCHED_SUN}" "${CRON_SUN}" \
  "PGA pairings - Sunday R4 publish window (every 30min 2-9pm ET)"

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "==> Deployment complete"
echo ""
echo "    Job           : ${JOB_NAME}"
echo "    Tournament    : ${TOURNAMENT_ID}"
echo "    Dry run       : ${DRY_RUN}"
echo ""
echo "    Schedules (${TIME_ZONE}):"
echo "      ${SCHED_WED}   : ${CRON_WED}   (R1+R2, hourly Wed 10am-11pm)"
echo "      ${SCHED_THU}  : ${CRON_THU}         (R1 safety, Thu 8am)"
echo "      ${SCHED_FRI}    : ${CRON_FRI}         (R2 safety, Fri 8am)"
echo "      ${SCHED_SAT}  : ${CRON_SAT}  (R3, every 30min Sat 2-9pm)"
echo "      ${SCHED_SUN}    : ${CRON_SUN}  (R4, every 30min Sun 2-9pm)"
echo ""
echo "    To update the tournament ID next week:"
echo "      ./deploy_pga_pairings.sh --project ${PROJECT} --region ${REGION} \\"
echo "          --tournament <NEW_ID> --skip-build"
echo ""
