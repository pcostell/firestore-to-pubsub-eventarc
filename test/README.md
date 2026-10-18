# End-to-End 1,000 QPS Test Harness (`test/`)

This directory contains a self-contained Terraform test harness and runner script that verifies the `firestore_eventarc_pubsub` module at **1,000 QPS** in a command-line specified GCP project.

## What the Test Does

1. **Provisions a Test Firestore Native Enterprise Database**:
   - Creates a `google_firestore_database` with `type = "FIRESTORE_NATIVE"`, `database_edition = "ENTERPRISE"`, and `firestore_data_access_mode = "DATA_ACCESS_MODE_ENABLED"`.
2. **Deploys the `firestore_eventarc_pubsub` Module**:
   - Configures Eventarc trigger(s), a 2nd Gen Cloud Run Function (`2` vCPU, `1Gi` memory, `concurrency = 80`, `min_instances = 10`, `max_instances = 100`), and the target Pub/Sub topic.
3. **Deploys & Executes the Load Generator Cloud Run Job ([`scripts/load_generator.py`](./scripts/load_generator.py))**:
   - Runs 4 parallel Cloud Run Job tasks (`250 QPS` per task = `1,000 QPS` total) writing documents across root (`orders/{docId}`) and nested subcollection (`tenants/{tenantId}/inventory/{docId}`) paths.
   - Embeds a deterministic SHA-256 `payload_hash` (`sha256(run_id:collection_group:doc_id:seq)`) in every document.
4. **Deploys & Executes the Pub/Sub Subscriber & Validator Cloud Run Job ([`scripts/subscriber.py`](./scripts/subscriber.py))**:
   - Streams all messages from the test Pub/Sub subscription.
   - Verifies the CloudEvent envelope and SHA-256 `payload_hash` on every message.
   - Randomly samples 5% of received messages (up to 500 documents) and fetches each sampled document directly from Firestore to verify field-by-field equality (`doc_id`, `collection_group`, `run_id`, `seq`, `payload_hash`).
5. **Verifies End-to-End Metrics ([`scripts/verify_metrics.py`](./scripts/verify_metrics.py))**:
   - Queries Cloud Logging for the Load Generator and Subscriber job summaries.
   - Polls the Cloud Monitoring API (`monitoring.googleapis.com/v3`) until time-series points are ingested for:
     - `firestore.googleapis.com/document/write_ops_count`
     - `run.googleapis.com/request_count` (`2xx`)
     - `pubsub.googleapis.com/topic/send_message_operation_count`
     - `pubsub.googleapis.com/subscription/sent_message_count`
     - `pubsub.googleapis.com/subscription/ack_message_count`
   - Asserts that the subscriber processed roughly the same number of documents as written to Firestore (`>= 90%` delivery ratio) and that `sampled_docs_failed == 0`.

---

## Usage

### 1. Run the Full End-to-End Test

Pass the target GCP project ID as the first argument to [`run_test.sh`](./run_test.sh):

```bash
./run_test.sh pcostello-cloud
```

### 2. Configurable Environment Variables

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `REGION` | `"us-central1"` | GCP region for the test database, Eventarc trigger, and Cloud Run jobs/function. |
| `TARGET_QPS` | `"1000"` | Total target write QPS across all load generator tasks. |
| `DURATION_SECONDS` | `"20"` | Load generation duration in seconds. |
| `DESTROY_ON_EXIT` | `"false"` | Set to `"true"` to automatically run `terraform destroy` after metric verification completes. |
| `TF_DATA_DIR` | `"/tmp/tf_data_fseap_<PROJECT_ID>"` | Directory where Terraform stores provider binaries. |
| `TF_STATE_FILE` | `"/tmp/tf_state_fseap_<PROJECT_ID>.tfstate"` | Path to the Terraform state file. |

Example with automatic teardown:

```bash
DESTROY_ON_EXIT=true TARGET_QPS=1000 DURATION_SECONDS=20 ./run_test.sh pcostello-cloud
```

### 3. Re-running Only the Metric Verification Script

If you want to re-query Cloud Monitoring after a test run without re-running the load generator:

```bash
python3 ./scripts/verify_metrics.py \
  --project="pcostello-cloud" \
  --database="<DATABASE_ID>" \
  --function-name="<FUNCTION_NAME>" \
  --topic-name="<TOPIC_SHORT_NAME>" \
  --subscription-name="<SUBSCRIPTION_SHORT_NAME>" \
  --load-job-name="<LOAD_JOB_NAME>" \
  --subscriber-job-name="<SUBSCRIBER_JOB_NAME>" \
  --start-time="2026-10-17T00:08:00Z" \
  --wait-monitoring-seconds=0
```

### 4. Cleaning Up Test Resources Manually

If `DESTROY_ON_EXIT` was left as `false`, you can tear down all test infrastructure in the project by running:

```bash
PROJECT_ID="pcostello-cloud"
TF_CLI_CONFIG_FILE=/dev/null \
TF_DATA_DIR="/tmp/tf_data_fseap_${PROJECT_ID}" \
terraform destroy -auto-approve \
  -state="/tmp/tf_state_fseap_${PROJECT_ID}.tfstate" \
  -var="project_id=${PROJECT_ID}"
```
