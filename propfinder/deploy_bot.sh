#!/usr/bin/env bash
set -euo pipefail

PROJECT="graphite-flare-477419-h7"
REGION="us-east4"
SERVICE="pulse-discord-bot"
IMAGE="gcr.io/$PROJECT/$SERVICE"

echo "Building bot image..."
gcloud builds submit \
  --project "$PROJECT" \
  --tag "$IMAGE" \
  --dockerfile propfinder/Dockerfile.bot \
  .

echo "Deploying to Cloud Run..."
gcloud run deploy "$SERVICE" \
  --project "$PROJECT" \
  --region "$REGION" \
  --image "$IMAGE" \
  --platform managed \
  --no-allow-unauthenticated \
  --memory 256Mi \
  --cpu 0.25 \
  --min-instances 0 \
  --max-instances 1 \
  --no-cpu-throttling \
  --set-env-vars "DISCORD_BOT_TOKEN=${DISCORD_BOT_TOKEN}" \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=$PROJECT" \
  --timeout 3600

echo "Setting up Cloud Scheduler for wake/sleep..."

# Wake up at 10 AM ET (14:00 UTC during EDT)
gcloud scheduler jobs create http "pulse-bot-wake" \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule "0 14 * * *" \
  --description "Scale up Discord bot for game hours" \
  --uri "https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/services/$SERVICE" \
  --http-method PATCH \
  --headers "Content-Type=application/json" \
  --message-body '{"scaling":{"minInstanceCount":1}}' \
  --oauth-service-account-email "${PROJECT}@appspot.gserviceaccount.com" \
  --quiet 2>/dev/null || \
gcloud scheduler jobs update http "pulse-bot-wake" \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule "0 14 * * *" \
  --quiet

# Sleep at midnight ET (04:00 UTC during EDT)
gcloud scheduler jobs create http "pulse-bot-sleep" \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule "0 4 * * *" \
  --description "Scale down Discord bot after games" \
  --uri "https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/services/$SERVICE" \
  --http-method PATCH \
  --headers "Content-Type=application/json" \
  --message-body '{"scaling":{"minInstanceCount":0}}' \
  --oauth-service-account-email "${PROJECT}@appspot.gserviceaccount.com" \
  --quiet 2>/dev/null || \
gcloud scheduler jobs update http "pulse-bot-sleep" \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule "0 4 * * *" \
  --quiet

echo ""
echo "Done! Bot will be alive 10 AM - midnight ET daily."
echo "Estimated cost: ~\$1-2/month"
