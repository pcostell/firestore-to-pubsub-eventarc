#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Deploys the test Firestore Native Enterprise database, Eventarc trigger(s),
# Cloud Run function, Pub/Sub topic/subscription, Load Generator Job, and
# Subscriber Validator Job in a specified GCP project, executes the 1000 QPS
# test, and verifies the metrics across Firestore, Cloud Run, and Pub/Sub.

set -euo pipefail

PROJECT_ID="${1:-pcostello-cloud}"
REGION="${REGION:-us-central1}"
TARGET_QPS="${TARGET_QPS:-1000}"
DURATION_SECONDS="${DURATION_SECONDS:-20}"
DESTROY_ON_EXIT="${DESTROY_ON_EXIT:-false}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export TF_CLI_CONFIG_FILE=/dev/null
export TF_DATA_DIR="${TF_DATA_DIR:-/tmp/tf_data_fseap_${PROJECT_ID}}"
TF_STATE_FILE="${TF_STATE_FILE:-/tmp/tf_state_fseap_${PROJECT_ID}.tfstate}"

echo "=================================================================="
echo " Deploying Firestore -> Eventarc -> Cloud Run -> Pub/Sub E2E Test"
echo " Project:          ${PROJECT_ID}"
echo " Region:           ${REGION}"
echo " Target QPS:       ${TARGET_QPS}"
echo " Duration:         ${DURATION_SECONDS}s"
echo "=================================================================="

terraform init -input=false

terraform apply -auto-approve -input=false \
  -state="${TF_STATE_FILE}" \
  -var="project_id=${PROJECT_ID}" \
  -var="region=${REGION}" \
  -var="target_qps=${TARGET_QPS}" \
  -var="duration_seconds=${DURATION_SECONDS}"

DATABASE_ID="$(terraform output -state="${TF_STATE_FILE}" -raw database_id)"
FUNCTION_NAME="$(terraform output -state="${TF_STATE_FILE}" -raw function_name)"
TOPIC_ID="$(terraform output -state="${TF_STATE_FILE}" -raw pubsub_topic_id)"
TOPIC_SHORT_NAME="${TOPIC_ID##*/}"
SUB_ID="$(terraform output -state="${TF_STATE_FILE}" -raw pubsub_subscription_id)"
SUB_SHORT_NAME="${SUB_ID##*/}"
LOAD_JOB_NAME="$(terraform output -state="${TF_STATE_FILE}" -raw load_generator_job_name)"
SUB_JOB_NAME="$(terraform output -state="${TF_STATE_FILE}" -raw subscriber_job_name)"

echo "Waiting 15s for Eventarc trigger readiness on database ${DATABASE_ID}..."
sleep 15

RUN_ID="run-$(date -u +%s)"
JOB_TIMEOUT="$((DURATION_SECONDS + 900))"
MAX_RUNTIME="$((DURATION_SECONDS + 600))"
START_TIME="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Test window start time: ${START_TIME} (RUN_ID=${RUN_ID})"

echo "Starting Pub/Sub Subscriber & Validator Cloud Run Job (${SUB_JOB_NAME})..."
gcloud run jobs execute "${SUB_JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --task-timeout="${JOB_TIMEOUT}s" \
  --update-env-vars="RUN_ID=${RUN_ID},EXPECTED_DURATION_SECONDS=${DURATION_SECONDS},MAX_RUNTIME_SECONDS=${MAX_RUNTIME}" \
  --async

echo "Starting Firestore Load Generator Cloud Run Job (${LOAD_JOB_NAME}) at up to ${TARGET_QPS} QPS for ${DURATION_SECONDS}s..."
gcloud run jobs execute "${LOAD_JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --task-timeout="${JOB_TIMEOUT}s" \
  --update-env-vars="RUN_ID=${RUN_ID},DURATION_SECONDS=${DURATION_SECONDS},TARGET_QPS=${TARGET_QPS}" \
  --async

wait_for_job() {
  local job_name="$1"
  echo "Waiting for Cloud Run Job ${job_name} latest execution to complete..."
  while true; do
    local comp_time=""
    comp_time="$(gcloud run jobs executions list \
      --job="${job_name}" \
      --project="${PROJECT_ID}" \
      --region="${REGION}" \
      --limit=1 \
      --format="value(status.completionTime)" 2>/dev/null || true)"
    if [[ -n "${comp_time}" ]]; then
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Job ${job_name} completed at ${comp_time}"
      break
    fi
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Job ${job_name} still running..."
    sleep 30
  done
}

wait_for_job "${LOAD_JOB_NAME}"
wait_for_job "${SUB_JOB_NAME}"

python3 "${SCRIPT_DIR}/scripts/verify_metrics.py" \
  --project="${PROJECT_ID}" \
  --database="${DATABASE_ID}" \
  --function-name="${FUNCTION_NAME}" \
  --topic-name="${TOPIC_SHORT_NAME}" \
  --subscription-name="${SUB_SHORT_NAME}" \
  --load-job-name="${LOAD_JOB_NAME}" \
  --subscriber-job-name="${SUB_JOB_NAME}" \
  --start-time="${START_TIME}" \
  --run-id="${RUN_ID}"

if [[ "${DESTROY_ON_EXIT}" == "true" ]]; then
  echo "Cleaning up test resources..."
  terraform destroy -auto-approve -input=false \
    -state="${TF_STATE_FILE}" \
    -var="project_id=${PROJECT_ID}" \
    -var="region=${REGION}" \
    -var="target_qps=${TARGET_QPS}" \
    -var="duration_seconds=${DURATION_SECONDS}"
fi
