#!/usr/bin/env bash
set -euo pipefail

PROJECT=""
REGION=""
BDL_KEY=""

IMAGE=""
SERVICE_NAME="mobile-api"
MIN_INSTANCES="0"
MAX_INSTANCES="1"
ALLOW_UNAUTHENTICATED="true"

usage() {
  cat <<EOF
Usage: $0 --project <PROJECT> --region <REGION> --bdl-key <KEY> [options]

Options:
  --image <IMAGE>                 Container image (default: gcr.io/<PROJECT>/mobile-api:latest)
  --service-name <NAME>           Service name (default: ${SERVICE_NAME})
  --min-instances <N>             Min instances (default: ${MIN_INSTANCES})
  --max-instances <N>             Max instances (default: ${MAX_INSTANCES})
  --allow-unauthenticated <BOOL>  Allow unauthenticated (default: ${ALLOW_UNAUTHENTICATED})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --bdl-key) BDL_KEY="$2"; shift 2;;
    --image) IMAGE="$2"; shift 2;;
    --service-name) SERVICE_NAME="$2"; shift 2;;
    --min-instances) MIN_INSTANCES="$2"; shift 2;;
    --max-instances) MAX_INSTANCES="$2"; shift 2;;
    --allow-unauthenticated) ALLOW_UNAUTHENTICATED="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

if [[ -z "${PROJECT}" || -z "${REGION}" || -z "${BDL_KEY}" ]]; then
  usage
  exit 1
fi

if [[ -z "${IMAGE}" ]]; then
  IMAGE="gcr.io/${PROJECT}/mobile-api:latest"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Building image ${IMAGE}"
BUILD_ID=$(gcloud builds submit \
  --project "${PROJECT}" \
  --tag "${IMAGE}" \
  --async \
  --format="value(id)" \
  "${SCRIPT_DIR}")

if [[ -z "${BUILD_ID}" ]]; then
  echo "Build did not start (no build id returned)."
  exit 1
fi

echo "==> Build started: ${BUILD_ID}"

while true; do
  STATUS=$(gcloud builds describe "${BUILD_ID}" \
    --project "${PROJECT}" \
    --format="value(status)")

  case "${STATUS}" in
    SUCCESS)
      echo "==> Build completed successfully"
      break
      ;;
    FAILURE|CANCELLED|TIMEOUT)
      echo "==> Build failed with status: ${STATUS}"
      exit 1
      ;;
    *)
      echo "==> Build status: ${STATUS} (waiting...)"
      sleep 5
      ;;
  esac
done

ALLOW_FLAG="--no-allow-unauthenticated"
if [[ "${ALLOW_UNAUTHENTICATED}" == "true" ]]; then
  ALLOW_FLAG="--allow-unauthenticated"
fi

echo "==> Deploying service ${SERVICE_NAME}"
gcloud run deploy "${SERVICE_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --platform managed \
  --image "${IMAGE}" \
  ${ALLOW_FLAG} \
  --min-instances "${MIN_INSTANCES}" \
  --max-instances "${MAX_INSTANCES}" \
  --set-env-vars "BALLDONTLIE_API_KEY=${BDL_KEY},GCP_PROJECT=${PROJECT}" \
  --set-env-vars "USE_SMART_SCHEDULER=false,ENABLE_LIVE_INGEST=false,ENABLE_LIVE_GAMES_SNAPSHOT=true,LIVE_GAMES_SNAPSHOT_INTERVAL_SEC=300"

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --format="value(status.url)")

echo "==> Service URL: ${SERVICE_URL}"
