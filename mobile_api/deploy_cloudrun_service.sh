#!/usr/bin/env bash
set -euo pipefail

PROJECT=""
REGION=""
PGA_KEY=""
ATP_KEY=""
THE_ODDS_API_KEYS="${THE_ODDS_API_KEYS:-}"

IMAGE=""
SERVICE_NAME="mobile-api"
MIN_INSTANCES="0"
MAX_INSTANCES="1"
ALLOW_UNAUTHENTICATED="true"
MODE="snapshot"
CPU_ALWAYS="false"
LIVE_GAMES_SNAPSHOT_INTERVAL_SEC="300"
LIVE_INGEST_INTERVAL_SEC="60"
LIVE_INGEST_PRE_GAME_MINUTES="0"

usage() {
  cat <<EOF
Usage: $0 --project <PROJECT> --region <REGION> --pga-key <KEY> --atp-key <KEY> [options]

Options:
  --image <IMAGE>                 Container image (default: gcr.io/<PROJECT>/mobile-api:latest)
  --service-name <NAME>           Service name (default: ${SERVICE_NAME})
  --min-instances <N>             Min instances (default: ${MIN_INSTANCES})
  --max-instances <N>             Max instances (default: ${MAX_INSTANCES})
  --allow-unauthenticated <BOOL>  Allow unauthenticated (default: ${ALLOW_UNAUTHENTICATED})
  --mode <snapshot|live>          Deploy mode (default: ${MODE})
  --cpu-always <BOOL>             Disable CPU throttling (default: ${CPU_ALWAYS})
  --live-games-snapshot-interval-sec <N>  Live games snapshot interval (default: ${LIVE_GAMES_SNAPSHOT_INTERVAL_SEC})
  --live-ingest-interval-sec <N>          Live ingest interval (default: ${LIVE_INGEST_INTERVAL_SEC})
  --live-ingest-pre-game-minutes <N>      Live ingest pre-game lead (default: ${LIVE_INGEST_PRE_GAME_MINUTES})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --pga-key) PGA_KEY="$2"; shift 2;;
    --atp-key) ATP_KEY="$2"; shift 2;;
    --image) IMAGE="$2"; shift 2;;
    --service-name) SERVICE_NAME="$2"; shift 2;;
    --min-instances) MIN_INSTANCES="$2"; shift 2;;
    --max-instances) MAX_INSTANCES="$2"; shift 2;;
    --allow-unauthenticated) ALLOW_UNAUTHENTICATED="$2"; shift 2;;
    --mode) MODE="$2"; shift 2;;
    --cpu-always) CPU_ALWAYS="$2"; shift 2;;
    --live-games-snapshot-interval-sec) LIVE_GAMES_SNAPSHOT_INTERVAL_SEC="$2"; shift 2;;
    --live-ingest-interval-sec) LIVE_INGEST_INTERVAL_SEC="$2"; shift 2;;
    --live-ingest-pre-game-minutes) LIVE_INGEST_PRE_GAME_MINUTES="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

MISSING_ARGS=()
if [[ -z "${PROJECT}" ]]; then
  MISSING_ARGS+=("--project")
fi
if [[ -z "${REGION}" ]]; then
  MISSING_ARGS+=("--region")
fi
if [[ -z "${PGA_KEY}" ]]; then
  MISSING_ARGS+=("--pga-key")
fi
if [[ -z "${ATP_KEY}" ]]; then
  MISSING_ARGS+=("--atp-key")
fi

if (( ${#MISSING_ARGS[@]} > 0 )); then
  echo "Missing required arguments: ${MISSING_ARGS[*]}"
  usage
  exit 1
fi

if [[ -z "${IMAGE}" ]]; then
  IMAGE="gcr.io/${PROJECT}/mobile-api:latest"
fi

if [[ -z "${THE_ODDS_API_KEYS}" ]]; then
  THE_ODDS_API_KEYS="${ODDS_API_KEYS:-}"
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

CPU_FLAG=""
if [[ "${CPU_ALWAYS}" == "true" ]]; then
  CPU_FLAG="--no-cpu-throttling"
fi

case "${MODE}" in
  snapshot)
    ENV_VARS=(
      "BDL_PGA_API_KEY=${PGA_KEY}"
      "BDL_ATP_API_KEY=${ATP_KEY}"
      "GCP_PROJECT=${PROJECT}"
      "USE_SMART_SCHEDULER=false"
      "ENABLE_LIVE_INGEST=false"
      "ENABLE_LIVE_GAMES_SNAPSHOT=true"
      "LIVE_GAMES_SNAPSHOT_INTERVAL_SEC=${LIVE_GAMES_SNAPSHOT_INTERVAL_SEC}"
    )
    ;;
  live)
    ENV_VARS=(
      "BDL_PGA_API_KEY=${PGA_KEY}"
      "BDL_ATP_API_KEY=${ATP_KEY}"
      "GCP_PROJECT=${PROJECT}"
      "USE_SMART_SCHEDULER=true"
      "ENABLE_LIVE_INGEST=true"
      "ENABLE_LIVE_GAMES_SNAPSHOT=true"
      "LIVE_GAMES_SNAPSHOT_INTERVAL_SEC=${LIVE_GAMES_SNAPSHOT_INTERVAL_SEC}"
      "LIVE_INGEST_INTERVAL_SEC=${LIVE_INGEST_INTERVAL_SEC}"
      "LIVE_INGEST_PRE_GAME_MINUTES=${LIVE_INGEST_PRE_GAME_MINUTES}"
      "ENABLE_PLAYER_STATS_INGEST=true"
    )
    ;;
  *)
    echo "Unknown mode: ${MODE}"
    exit 1
    ;;
esac

if [[ -n "${THE_ODDS_API_KEYS}" ]]; then
  ENV_VARS+=("THE_ODDS_API_KEYS=${THE_ODDS_API_KEYS}")
fi

ENV_VARS_CSV=$(IFS=, ; echo "${ENV_VARS[*]}")

echo "==> Deploying service ${SERVICE_NAME}"
gcloud run deploy "${SERVICE_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --platform managed \
  --image "${IMAGE}" \
  ${ALLOW_FLAG} \
  ${CPU_FLAG} \
  --min-instances "${MIN_INSTANCES}" \
  --max-instances "${MAX_INSTANCES}" \
  --set-env-vars "${ENV_VARS_CSV}"

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --format="value(status.url)")

echo "==> Service URL: ${SERVICE_URL}"
