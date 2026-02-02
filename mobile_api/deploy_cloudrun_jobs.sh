#!/usr/bin/env bash
set -euo pipefail

PROJECT=""
REGION=""
BDL_KEY=""

IMAGE=""
INGEST_JOB="mobile-api-ingest"
GATEKEEPER_JOB="mobile-api-gatekeeper"
SCHEDULER_NAME="mobile-api-gatekeeper"
SCHEDULER_SA_NAME="mobile-api-jobs-sa"

SCHEDULE="*/2 * * * *"
PRE_GAME_MINUTES="0"
INTERVAL_SEC="60"
JOB_MAX_MINUTES="10"

usage() {
  cat <<EOF
Usage: $0 --project <PROJECT> --region <REGION> --bdl-key <KEY> [options]

Options:
  --image <IMAGE>                 Container image (default: gcr.io/<PROJECT>/mobile-api-jobs:latest)
  --ingest-job <NAME>             Ingest job name (default: ${INGEST_JOB})
  --gatekeeper-job <NAME>         Gatekeeper job name (default: ${GATEKEEPER_JOB})
  --scheduler-name <NAME>         Scheduler job name (default: ${SCHEDULER_NAME})
  --schedule "<CRON>"             Scheduler cron (default: "${SCHEDULE}")
  --pre-game-minutes <N>          Lead time before games (default: ${PRE_GAME_MINUTES})
  --interval-sec <N>              Ingest interval seconds (default: ${INTERVAL_SEC})
  --job-max-minutes <N>           Ingest job max minutes (default: ${JOB_MAX_MINUTES})
  --service-account <NAME>        Service account name (default: ${SCHEDULER_SA_NAME})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --bdl-key) BDL_KEY="$2"; shift 2;;
    --image) IMAGE="$2"; shift 2;;
    --ingest-job) INGEST_JOB="$2"; shift 2;;
    --gatekeeper-job) GATEKEEPER_JOB="$2"; shift 2;;
    --scheduler-name) SCHEDULER_NAME="$2"; shift 2;;
    --schedule) SCHEDULE="$2"; shift 2;;
    --pre-game-minutes) PRE_GAME_MINUTES="$2"; shift 2;;
    --interval-sec) INTERVAL_SEC="$2"; shift 2;;
    --job-max-minutes) JOB_MAX_MINUTES="$2"; shift 2;;
    --service-account) SCHEDULER_SA_NAME="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

if [[ -z "${PROJECT}" || -z "${REGION}" || -z "${BDL_KEY}" ]]; then
  usage
  exit 1
fi

if [[ -z "${IMAGE}" ]]; then
  IMAGE="gcr.io/${PROJECT}/mobile-api-jobs:latest"
fi

SCHEDULER_SA_EMAIL="${SCHEDULER_SA_NAME}@${PROJECT}.iam.gserviceaccount.com"

echo "==> Building image ${IMAGE}"
gcloud builds submit --project "${PROJECT}" --tag "${IMAGE}"

echo "==> Ensuring service account ${SCHEDULER_SA_EMAIL}"
if ! gcloud iam service-accounts describe "${SCHEDULER_SA_EMAIL}" --project "${PROJECT}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SCHEDULER_SA_NAME}" --project "${PROJECT}"
fi

echo "==> Granting IAM roles to service account"
gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member "serviceAccount:${SCHEDULER_SA_EMAIL}" \
  --role "roles/run.developer" >/dev/null

echo "==> Deploying ingest job ${INGEST_JOB}"
gcloud run jobs deploy "${INGEST_JOB}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --command "python" \
  --args "-m","jobs.ingest_runner" \
  --set-env-vars "BALLDONTLIE_API_KEY=${BDL_KEY},GCP_PROJECT=${PROJECT}" \
  --set-env-vars "LIVE_INGEST_INTERVAL_SEC=${INTERVAL_SEC},LIVE_INGEST_PRE_GAME_MINUTES=${PRE_GAME_MINUTES},LIVE_INGEST_JOB_MAX_MINUTES=${JOB_MAX_MINUTES}"

echo "==> Deploying gatekeeper job ${GATEKEEPER_JOB}"
gcloud run jobs deploy "${GATEKEEPER_JOB}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --command "python" \
  --args "-m","jobs.gatekeeper" \
  --service-account "${SCHEDULER_SA_EMAIL}" \
  --set-env-vars "BALLDONTLIE_API_KEY=${BDL_KEY},GCP_PROJECT=${PROJECT}" \
  --set-env-vars "LIVE_INGEST_PRE_GAME_MINUTES=${PRE_GAME_MINUTES}" \
  --set-env-vars "CLOUD_RUN_REGION=${REGION},CLOUD_RUN_INGEST_JOB=${INGEST_JOB}"

echo "==> Creating/updating Cloud Scheduler job ${SCHEDULER_NAME}"
SCHEDULER_URI="https://run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${GATEKEEPER_JOB}:run"

if gcloud scheduler jobs describe "${SCHEDULER_NAME}" --project "${PROJECT}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${SCHEDULER_NAME}" \
    --project "${PROJECT}" \
    --schedule "${SCHEDULE}" \
    --uri "${SCHEDULER_URI}" \
    --http-method POST \
    --oauth-service-account-email "${SCHEDULER_SA_EMAIL}" \
    --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
else
  gcloud scheduler jobs create http "${SCHEDULER_NAME}" \
    --project "${PROJECT}" \
    --schedule "${SCHEDULE}" \
    --uri "${SCHEDULER_URI}" \
    --http-method POST \
    --oauth-service-account-email "${SCHEDULER_SA_EMAIL}" \
    --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
fi

echo "==> Done"
