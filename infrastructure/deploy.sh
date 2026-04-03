#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# CIP Deployment Script — Cloud Run + IAP + Cloud Scheduler
# =============================================================================
# Usage:
#   ./infrastructure/deploy.sh                  # Deploy everything
#   ./infrastructure/deploy.sh --backend-only   # Backend only
#   ./infrastructure/deploy.sh --frontend-only  # Frontend only
#   ./infrastructure/deploy.sh --scheduler-only # Scheduler only
# =============================================================================

PROJECT_ID="point-blank-ada"
REGION="northamerica-northeast1"
BACKEND_SERVICE="cip-backend"
FRONTEND_SERVICE="cip-frontend"
BACKEND_IMAGE="gcr.io/${PROJECT_ID}/${BACKEND_SERVICE}"
FRONTEND_IMAGE="gcr.io/${PROJECT_ID}/${FRONTEND_SERVICE}"
SA_EMAIL="cip-sheets-reader@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== CIP Deployment ==="
echo "Project: ${PROJECT_ID}"
echo "Region:  ${REGION}"
echo ""

# ── Helper: check if a gcloud command succeeded ─────────────────────
check() { if [ $? -ne 0 ]; then echo "ERROR: $1" >&2; exit 1; fi; }

# ── Parse arguments ──────────────────────────────────────────────────
DEPLOY_BACKEND=true
DEPLOY_FRONTEND=true
DEPLOY_SCHEDULER=true

for arg in "$@"; do
  case $arg in
    --backend-only)  DEPLOY_FRONTEND=false; DEPLOY_SCHEDULER=false ;;
    --frontend-only) DEPLOY_BACKEND=false;  DEPLOY_SCHEDULER=false ;;
    --scheduler-only) DEPLOY_BACKEND=false; DEPLOY_FRONTEND=false ;;
  esac
done

# =============================================================================
# 1. SECRETS
# =============================================================================
echo "── Step 1: Secrets Manager ──"

# Sheets service account key
if gcloud secrets describe cip-sheets-reader-key --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Secret 'cip-sheets-reader-key' already exists"
else
  if [ -f infrastructure/secrets/cip-sheets-reader.json ]; then
    gcloud secrets create cip-sheets-reader-key \
      --project="${PROJECT_ID}" \
      --replication-policy="user-managed" \
      --locations="${REGION}" \
      --data-file=infrastructure/secrets/cip-sheets-reader.json
    echo "  Created secret 'cip-sheets-reader-key'"
  else
    echo "  WARNING: infrastructure/secrets/cip-sheets-reader.json not found — skipping"
  fi
fi

# Slack bot token
if gcloud secrets describe cip-slack-bot-token --project="${PROJECT_ID}" &>/dev/null; then
  echo "  Secret 'cip-slack-bot-token' already exists"
else
  if [ -n "${SLACK_BOT_TOKEN:-}" ]; then
    echo -n "${SLACK_BOT_TOKEN}" | gcloud secrets create cip-slack-bot-token \
      --project="${PROJECT_ID}" \
      --replication-policy="user-managed" \
      --locations="${REGION}" \
      --data-file=-
    echo "  Created secret 'cip-slack-bot-token'"
  else
    echo "  WARNING: SLACK_BOT_TOKEN env var not set — skipping (set it and re-run)"
  fi
fi

# Grant service account access to secrets
gcloud secrets add-iam-policy-binding cip-sheets-reader-key \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" \
  --quiet 2>/dev/null || true

gcloud secrets add-iam-policy-binding cip-slack-bot-token \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" \
  --quiet 2>/dev/null || true

echo ""

# =============================================================================
# 2. BACKEND
# =============================================================================
if [ "$DEPLOY_BACKEND" = true ]; then
  echo "── Step 2: Backend Container ──"

  echo "  Building backend image..."
  gcloud builds submit \
    --project="${PROJECT_ID}" \
    --tag "${BACKEND_IMAGE}" \
    --timeout=600 \
    .

  echo "  Deploying to Cloud Run..."
  gcloud run deploy "${BACKEND_SERVICE}" \
    --project="${PROJECT_ID}" \
    --image="${BACKEND_IMAGE}" \
    --region="${REGION}" \
    --platform=managed \
    --port=8000 \
    --service-account="${SA_EMAIL}" \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},GCP_PROJECT_ID=${PROJECT_ID},GCP_REGION=${REGION},BIGQUERY_DATASET=cip,SHEETS_SERVICE_ACCOUNT_FILE=/secrets/cip-sheets-reader.json,APP_ENV=production" \
    --update-secrets="/secrets/cip-sheets-reader.json=cip-sheets-reader-key:latest,SLACK_BOT_TOKEN=cip-slack-bot-token:latest" \
    --memory=1Gi \
    --cpu=1 \
    --timeout=300 \
    --min-instances=0 \
    --max-instances=5 \
    --allow-unauthenticated

  BACKEND_URL=$(gcloud run services describe "${BACKEND_SERVICE}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --format='value(status.url)')
  echo "  Backend deployed: ${BACKEND_URL}"
  echo ""
fi

# =============================================================================
# 3. FRONTEND
# =============================================================================
if [ "$DEPLOY_FRONTEND" = true ]; then
  echo "── Step 3: Frontend Container ──"

  # Get the backend URL if not set
  if [ -z "${BACKEND_URL:-}" ]; then
    BACKEND_URL=$(gcloud run services describe "${BACKEND_SERVICE}" \
      --project="${PROJECT_ID}" \
      --region="${REGION}" \
      --format='value(status.url)' 2>/dev/null || echo "")
  fi

  if [ -z "${BACKEND_URL}" ]; then
    echo "  ERROR: Backend URL not found — deploy backend first"
    exit 1
  fi

  echo "  Building frontend image (API_URL=${BACKEND_URL})..."
  gcloud builds submit \
    --project="${PROJECT_ID}" \
    --config=frontend/cloudbuild.yaml \
    --substitutions=_NEXT_PUBLIC_API_URL=${BACKEND_URL},_IMAGE_TAG=${FRONTEND_IMAGE} \
    --timeout=600 \
    ./frontend

  echo "  Deploying to Cloud Run..."
  gcloud run deploy "${FRONTEND_SERVICE}" \
    --project="${PROJECT_ID}" \
    --image="${FRONTEND_IMAGE}" \
    --region="${REGION}" \
    --platform=managed \
    --port=3000 \
    --set-env-vars="NODE_ENV=production" \
    --memory=512Mi \
    --cpu=1 \
    --timeout=60 \
    --min-instances=0 \
    --max-instances=3 \
    --allow-unauthenticated

  # Get old-format URL from gcloud
  OLD_URL=$(gcloud run services describe "${FRONTEND_SERVICE}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --format='value(status.url)')
  echo "  Frontend deployed (old format): ${OLD_URL}"

  # Construct new-format URL: https://{service}-{project_number}.{region}.run.app
  PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')
  NEW_URL="https://${FRONTEND_SERVICE}-${PROJECT_NUMBER}.${REGION}.run.app"
  echo "  Frontend deployed (new format): ${NEW_URL}"

  # Update backend CORS to include both frontend URL formats
  echo "  Updating backend CORS (both URL formats)..."
  gcloud run services update "${BACKEND_SERVICE}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --update-env-vars='CORS_ORIGINS=["'"${OLD_URL}"'","'"${NEW_URL}"'"],FRONTEND_URL='"${NEW_URL}"''

  echo ""
fi

# =============================================================================
# 4. IAP SETUP
# =============================================================================
echo "── Step 4: IAP Configuration ──"
echo "  IAP must be configured manually via the GCP Console:"
echo "    1. Go to Security > Identity-Aware Proxy"
echo "    2. Enable IAP on both ${BACKEND_SERVICE} and ${FRONTEND_SERVICE}"
echo "    3. Configure OAuth consent screen (internal, pointblankcreative.ca)"
echo "    4. Add @pointblankcreative.ca as IAP-secured Web App User"
echo ""

# =============================================================================
# 5. CLOUD SCHEDULER
# =============================================================================
if [ "$DEPLOY_SCHEDULER" = true ]; then
  echo "── Step 5: Cloud Scheduler ──"

  if [ -z "${BACKEND_URL:-}" ]; then
    BACKEND_URL=$(gcloud run services describe "${BACKEND_SERVICE}" \
      --project="${PROJECT_ID}" \
      --region="${REGION}" \
      --format='value(status.url)' 2>/dev/null || echo "")
  fi

  if [ -z "${BACKEND_URL}" ]; then
    echo "  ERROR: Backend URL not found — deploy backend first"
    exit 1
  fi

  # Grant Cloud Run invoker to the service account
  gcloud run services add-iam-policy-binding "${BACKEND_SERVICE}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.invoker" \
    --quiet 2>/dev/null || true

  # Delete old single daily job if it exists
  if gcloud scheduler jobs describe cip-daily-run \
    --project="${PROJECT_ID}" \
    --location="${REGION}" &>/dev/null; then
    echo "  Deleting old single daily job..."
    gcloud scheduler jobs delete cip-daily-run \
      --project="${PROJECT_ID}" \
      --location="${REGION}" \
      --quiet
  fi

  # Morning run: 5:30 AM ET (picks up Funnel.io 2 AM PT sync)
  if gcloud scheduler jobs describe cip-morning-run \
    --project="${PROJECT_ID}" \
    --location="${REGION}" &>/dev/null; then
    echo "  Updating morning scheduler job..."
    gcloud scheduler jobs update http cip-morning-run \
      --project="${PROJECT_ID}" \
      --location="${REGION}" \
      --schedule="30 5 * * *" \
      --time-zone="America/Toronto" \
      --uri="${BACKEND_URL}/api/admin/daily-run" \
      --http-method=POST \
      --oidc-service-account-email="${SA_EMAIL}"
  else
    echo "  Creating morning scheduler job..."
    gcloud scheduler jobs create http cip-morning-run \
      --project="${PROJECT_ID}" \
      --location="${REGION}" \
      --schedule="30 5 * * *" \
      --time-zone="America/Toronto" \
      --uri="${BACKEND_URL}/api/admin/daily-run" \
      --http-method=POST \
      --oidc-service-account-email="${SA_EMAIL}"
  fi

  # Afternoon run: 5:30 PM ET (picks up Funnel.io 2 PM PT sync)
  if gcloud scheduler jobs describe cip-afternoon-run \
    --project="${PROJECT_ID}" \
    --location="${REGION}" &>/dev/null; then
    echo "  Updating afternoon scheduler job..."
    gcloud scheduler jobs update http cip-afternoon-run \
      --project="${PROJECT_ID}" \
      --location="${REGION}" \
      --schedule="30 17 * * *" \
      --time-zone="America/Toronto" \
      --uri="${BACKEND_URL}/api/admin/daily-run" \
      --http-method=POST \
      --oidc-service-account-email="${SA_EMAIL}"
  else
    echo "  Creating afternoon scheduler job..."
    gcloud scheduler jobs create http cip-afternoon-run \
      --project="${PROJECT_ID}" \
      --location="${REGION}" \
      --schedule="30 17 * * *" \
      --time-zone="America/Toronto" \
      --uri="${BACKEND_URL}/api/admin/daily-run" \
      --http-method=POST \
      --oidc-service-account-email="${SA_EMAIL}"
  fi

  echo "  Scheduler: cip-morning-run (5:30 AM ET) + cip-afternoon-run (5:30 PM ET)"
  echo ""
fi

# =============================================================================
# 6. VERIFICATION
# =============================================================================
echo "── Verification ──"
if [ -n "${BACKEND_URL:-}" ]; then
  echo "  Backend URL:  ${BACKEND_URL}"
  echo "  Frontend URL: ${FRONTEND_URL:-not deployed}"
  echo ""
  echo "  Test commands:"
  echo "    curl ${BACKEND_URL}/health"
  echo "    curl ${BACKEND_URL}/api/projects/"
  echo "    curl ${BACKEND_URL}/api/pacing/26009"
fi

echo ""
echo "=== Deployment complete ==="
