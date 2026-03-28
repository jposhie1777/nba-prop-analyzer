#!/usr/bin/env bash
# Deploy the Camoufox Browser Proxy to Cloud Run.
#
# Mirrors the pattern used in mobile_api/deploy_cloudrun_service.sh:
#   1. gcloud builds submit  → builds + pushes to gcr.io
#   2. gcloud run deploy     → deploys to Cloud Run
#
# Usage:
#   ./deploy.sh --project graphite-flare-477419-h7 --region us-central1 \
#               [--min-instances 1] [--max-instances 2] \
#               [--github-token <TOKEN>]

set -euo pipefail

PROJECT=""
REGION="us-central1"
IMAGE=""
SERVICE_NAME="camoufox-proxy"
MIN_INSTANCES="1"   # always-on — avoids cold-start delay for scraper calls
MAX_INSTANCES="2"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

usage() {
  cat <<EOF
Usage: $0 --project <PROJECT> --region <REGION> [options]

Options:
  --image <IMAGE>            Container image URI (default: gcr.io/<PROJECT>/camoufox-proxy:latest)
  --service-name <NAME>      Cloud Run service name (default: ${SERVICE_NAME})
  --min-instances <N>        Min instances (default: ${MIN_INSTANCES})
  --max-instances <N>        Max instances (default: ${MAX_INSTANCES})
  --github-token <TOKEN>     GitHub token for Camoufox binary download (build arg)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)        PROJECT="$2";        shift 2 ;;
    --region)         REGION="$2";         shift 2 ;;
    --image)          IMAGE="$2";          shift 2 ;;
    --service-name)   SERVICE_NAME="$2";   shift 2 ;;
    --min-instances)  MIN_INSTANCES="$2";  shift 2 ;;
    --max-instances)  MAX_INSTANCES="$2";  shift 2 ;;
    --github-token)   GITHUB_TOKEN="$2";   shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "${PROJECT}" || -z "${REGION}" ]]; then
  usage
  exit 1
fi

if [[ -z "${IMAGE}" ]]; then
  IMAGE="gcr.io/${PROJECT}/camoufox-proxy:latest"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Verify Cloud Build bucket access (same check as mobile_api/deploy_cloudrun_service.sh)
CLOUDBUILD_BUCKET="gs://${PROJECT}_cloudbuild"
echo "==> Checking Cloud Build bucket: ${CLOUDBUILD_BUCKET}"
if ! gsutil ls "${CLOUDBUILD_BUCKET}" >/dev/null 2>&1; then
  echo "ERROR: Cannot access ${CLOUDBUILD_BUCKET}."
  echo "Grant the deploy identity storage access to the Cloud Build bucket."
  exit 1
fi

# Build and push image via Cloud Build.
# Pass GITHUB_TOKEN as a substitution so it's available to the Dockerfile ARG
# during the build without being stored in the final image layers.
SUBSTITUTIONS=""
if [[ -n "${GITHUB_TOKEN}" ]]; then
  SUBSTITUTIONS="--substitutions=_GITHUB_TOKEN=${GITHUB_TOKEN}"
fi

echo "==> Building image ${IMAGE}"
BUILD_ID=$(gcloud builds submit \
  --project "${PROJECT}" \
  --tag "${IMAGE}" \
  --async \
  --format="value(id)" \
  ${SUBSTITUTIONS} \
  "${SCRIPT_DIR}")

if [[ -z "${BUILD_ID}" ]]; then
  echo "ERROR: Build did not start (no build id returned)."
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
      echo "==> Build failed: ${STATUS}"
      gcloud builds log "${BUILD_ID}" --project "${PROJECT}" || true
      exit 1
      ;;
    *)
      echo "==> Build status: ${STATUS} (waiting...)"
      sleep 10
      ;;
  esac
done

echo "==> Deploying service ${SERVICE_NAME}"
gcloud run deploy "${SERVICE_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --platform managed \
  --image "${IMAGE}" \
  --no-allow-unauthenticated \
  --min-instances "${MIN_INSTANCES}" \
  --max-instances "${MAX_INSTANCES}" \
  --memory 2Gi \
  --cpu 2 \
  --timeout 300 \
  --concurrency 1 \
  --set-env-vars "GCP_PROJECT=${PROJECT}"

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --format="value(status.url)")

echo ""
echo "==> Service deployed: ${SERVICE_URL}"
echo ""
echo "Add this URL as a GitHub Actions secret:"
echo "  Secret name : CAMOUFOX_SERVICE_URL"
echo "  Secret value: ${SERVICE_URL}"
