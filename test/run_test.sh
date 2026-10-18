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
START_TIME="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Test window start time: ${START_TIME} (RUN_ID=${RUN_ID})"

echo "Starting Pub/Sub Subscriber & Validator Cloud Run Job (${SUB_JOB_NAME})..."
gcloud run jobs execute "${SUB_JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --update-env-vars="RUN_ID=${RUN_ID}" \
  --wait &
SUB_PID=$!

echo "Executing Firestore Load Generator Cloud Run Job (${LOAD_JOB_NAME}) at up to ${TARGET_QPS} QPS..."
gcloud run jobs execute "${LOAD_JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --update-env-vars="RUN_ID=${RUN_ID}" \
  --wait

echo "Load Generator finished. Waiting for Pub/Sub Subscriber Job (${SUB_JOB_NAME}) to drain and validate sample..."
wait "${SUB_PID}"

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
