# Firestore Eventarc to Pub/Sub Terraform Project & Module

This repository provides a reusable Terraform module (`./modules/firestore_eventarc_pubsub`), a deployable root Terraform project, and a 1,000 QPS end-to-end load & validation test suite (`./test`) that streams **Cloud Firestore** document changes (`google.cloud.firestore.document.v1.written`) via **Eventarc** and a **2nd Gen Cloud Run Function** to a **Cloud Pub/Sub** topic.

---

## Directory Structure

```text
firestore_eventarc_pubsub/
├── main.tf                         # Root Terraform project invoking the module
├── variables.tf                    # Root project configurable variables
├── outputs.tf                      # Root project outputs
├── terraform.tfvars.example        # Example variable values
├── modules/
│   └── firestore_eventarc_pubsub/  # Reusable Terraform module
│       ├── main.tf                 # Eventarc, Cloud Run Function (2nd Gen), Pub/Sub, IAM
│       ├── variables.tf            # Module input variables
│       ├── outputs.tf              # Module outputs
│       ├── versions.tf             # Required Terraform & provider versions
│       └── function_source/
│           ├── main.py             # Python 3.12 Cloud Run Function forwarding events to Pub/Sub
│           └── requirements.txt    # Function Python dependencies
└── test/                           # 1,000 QPS E2E test harness (Firestore Enterprise + Load Gen + Subscriber)
    ├── main.tf                     # Deploys Enterprise DB, module, Load Generator Job, Subscriber Job
    ├── variables.tf                # Test configuration variables
    ├── outputs.tf                  # Test resource outputs
    ├── run_test.sh                 # Automated E2E deployment, execution, and metric verification script
    └── scripts/
        ├── load_generator.py       # Multi-task 1,000+ QPS Firestore document load generator
        ├── subscriber.py           # Streaming Pub/Sub consumer & live Firestore sample validator
        └── verify_metrics.py       # Cloud Monitoring & Cloud Logging E2E verification script
```

---

## Architecture

1. **Cloud Firestore Database**: Emits `google.cloud.firestore.document.v1.written` (or `.withAuthContext`) events on every document creation, update, and deletion.
2. **Eventarc Trigger(s)**:
   - **All Changes Mode (`collection_groups = []`)**: Creates a single database-wide `google_eventarc_trigger` matching all document changes in `var.database`.
   - **Collection Group Subset Mode (`collection_groups = ["orders", "inventory"]`)**: Creates an Eventarc trigger per collection group using the `match-path-pattern` operator with pattern `**/<collection_group>/{docId}`, matching documents in that collection group whether it is a root collection (`orders/123`) or a nested subcollection at any depth (`tenants/t1/orders/123`).
3. **Cloud Run Function (2nd Gen, Python 3.12)**:
   - Receives `application/protobuf` `DocumentEventData` CloudEvents from Eventarc.
   - Extracts the relative document path, `collection_group`, `document_id`, and `operation_type` (`CREATE`, `UPDATE`, or `DELETE`).
   - Enforces defense-in-depth collection group filtering via `COLLECTION_GROUPS`.
   - Deserializes the Protobuf `DocumentEventData` (`value`, `oldValue`, `updateMask`) into a structured JSON envelope (or forwards raw Protobuf bytes when `pubsub_payload_format = "protobuf"`) and publishes it to the target Pub/Sub topic using high-throughput `BatchSettings`.
4. **Cloud Pub/Sub Topic**: Receives the published change events with rich attributes for downstream filtering and routing.

---

## Prerequisites

1. **Terraform** `>= 1.5.0` installed and available in `PATH`.
2. **Google Cloud SDK (`gcloud`)** authenticated with Application Default Credentials:
   ```bash
   gcloud config set project <YOUR_PROJECT_ID>
   ```
3. **IAM Permissions**: The deploying principal needs permissions to manage Firestore databases (if creating one), Eventarc triggers, Cloud Run functions/services, Pub/Sub topics, Cloud Storage buckets, and Service Accounts/IAM bindings in the target project.

---

## Quick Start: Deploying the Terraform Project

### 1. Configure Variables

Copy [`terraform.tfvars.example`](./terraform.tfvars.example) to `terraform.tfvars` (or pass `-var` flags on the command line):

```hcl
project_id          = "my-gcp-project"
region              = "us-central1"
database            = "(default)"
firestore_location  = "nam5"
pubsub_topic        = "firestore-changes"
create_pubsub_topic = true

# Optional: sync only specific collection groups, or set to [] for all collections
collection_groups   = ["orders", "inventory"]
```

### 2. Initialize and Apply

```bash
export TF_CLI_CONFIG_FILE=/dev/null
export TF_DATA_DIR="/tmp/tf_firestore_eventarc_pubsub"

terraform init
terraform plan
terraform apply
```

---

## Embedding the Module in Another Terraform Configuration

You can reference `./modules/firestore_eventarc_pubsub` directly from your own Terraform configuration:

```hcl
module "firestore_cdc_to_pubsub" {
  source = "./modules/firestore_eventarc_pubsub"

  project_id          = "my-gcp-project"
  region              = "us-central1"
  database            = "my-firestore-db"
  firestore_location  = "us-central1"
  pubsub_topic        = "firestore-cdc-events"
  create_pubsub_topic = true

  # Leave empty ([]) to stream all document changes in the database,
  # or specify a subset of collection groups:
  collection_groups = ["orders", "inventory"]

  # Optional Cloud Run scaling settings for high-throughput workloads:
  function_cpu           = "2"
  function_memory        = "1Gi"
  function_concurrency   = 80
  function_min_instances = 2
  function_max_instances = 100
}
```

---

## Inputs & Outputs

### Input Variables

| Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `project_id` | `string` | *(required)* | The GCP project ID where resources will be deployed. |
| `database` | `string` | `"(default)"` | The Firestore database ID to monitor for changes. |
| `pubsub_topic` | `string` | *(required)* | Target Pub/Sub topic name or full resource ID (`projects/<project>/topics/<topic>`). |
| `collection_groups` | `list(string)` | `[]` | Optional subset of Firestore collection groups to sync. Leave empty (`[]`) to sync all changes across the database. |
| `region` | `string` | `"us-central1"` | GCP region for the Cloud Run function and source archive bucket. |
| `firestore_location` | `string` | `""` | Location of the Firestore database for the Eventarc trigger(s) (e.g., `"nam5"`, `"eur3"`, `"us-central1"`). Defaults to `region` when empty. |
| `create_pubsub_topic` | `bool` | `true` | Whether Terraform should create the target Pub/Sub topic (`false` to use an existing topic). |
| `create_database` | `bool` | `false` | *(Root project only)* Whether Terraform should create the Firestore database (`false` to use an existing database). |
| `include_auth_context` | `bool` | `false` | Whether to subscribe to `google.cloud.firestore.document.v1.written.withAuthContext`. |
| `pubsub_payload_format` | `string` | `"json"` | Payload format published to Pub/Sub: `"json"` (structured CloudEvent + `DocumentEventData` JSON) or `"protobuf"` (raw `DocumentEventData` protobuf bytes). |
| `function_cpu` | `string` | `"2"` | CPU allocated to each Cloud Run function instance (module variable). |
| `function_memory` | `string` | `"1Gi"` | Memory allocated to each Cloud Run function instance (module variable). |
| `function_concurrency` | `number` | `80` | Max concurrent requests per Cloud Run function instance (module variable). |
| `function_min_instances` | `number` | `0` | Minimum warm instances for the Cloud Run function (module variable). |
| `function_max_instances` | `number` | `100` | Maximum autoscaling instances for the Cloud Run function (module variable). |

### Outputs

| Output | Description |
| :--- | :--- |
| `pubsub_topic_id` | Full resource ID of the target Pub/Sub topic (`projects/<project>/topics/<topic>`). |
| `function_name` | Name of the deployed 2nd Gen Cloud Run function. |
| `function_uri` | Internal service URI of the deployed Cloud Run function. |
| `eventarc_trigger_names` | Map of collection group keys (or `"all"`) to deployed Eventarc trigger names. |

---

## Pub/Sub Message Format

### Message Attributes

Every published Pub/Sub message includes the following attributes so subscribers can filter without parsing the message body:

| Attribute | Example Value | Description |
| :--- | :--- | :--- |
| `database` | `"my-firestore-db"` | Source Firestore database ID. |
| `document` | `"tenants/t1/orders/doc-123"` | Document path relative to `documents/`. |
| `collection_group` | `"orders"` | Leaf collection ID (collection group) of the modified document. |
| `document_id` | `"doc-123"` | Document ID. |
| `operation_type` | `"CREATE"` \| `"UPDATE"` \| `"DELETE"` | Mutation type inferred from `value` and `oldValue`. |
| `payload_format` | `"json"` \| `"protobuf"` | Format of the message body. |
| `ce_id`, `ce_type`, `ce_source`, `ce_subject`, `ce_time` | CloudEvent metadata | Original Eventarc CloudEvent headers. |

### JSON Message Body (`pubsub_payload_format = "json"`)

```json
{
  "id": "84920192-...",
  "type": "google.cloud.firestore.document.v1.written",
  "source": "//firestore.googleapis.com/projects/my-project/databases/my-db",
  "subject": "documents/orders/doc-123",
  "time": "2026-10-17T00:08:21.123456Z",
  "database": "my-db",
  "document": "orders/doc-123",
  "collectionGroup": "orders",
  "documentId": "doc-123",
  "operationType": "CREATE",
  "data": {
    "value": {
      "name": "projects/my-project/databases/my-db/documents/orders/doc-123",
      "fields": {
        "doc_id": { "stringValue": "doc-123" },
        "seq": { "integerValue": "1" }
      },
      "createTime": "2026-10-17T00:08:21.100000Z",
      "updateTime": "2026-10-17T00:08:21.100000Z"
    }
  }
}
```

---

## Running the 1,000 QPS End-to-End Load & Validation Test

The [`./test`](./test) directory contains a turnkey end-to-end test harness that:
1. Deploys a **Firestore Native Enterprise** database in a command-line specified GCP project.
2. Deploys the `firestore_eventarc_pubsub` module (with 10 warm Cloud Run instances configured for 1,000+ QPS).
3. Deploys and runs a 4-task **Load Generator Cloud Run Job** driving **1,000 QPS** against root and nested collections.
4. Deploys and runs a **Pub/Sub Subscriber & Validator Cloud Run Job** that consumes all messages, checks SHA-256 payload hashes, and validates a random 5% sample of documents directly against live Firestore.
5. Queries **Cloud Monitoring** and **Cloud Logging** to verify that Firestore document writes, Cloud Run function invocations, Pub/Sub published/acknowledged messages, and subscriber unique document counts all match.

### Run Command

Pass the target GCP project ID as the first argument to [`test/run_test.sh`](./test/run_test.sh):

```bash
./test/run_test.sh <YOUR_PROJECT_ID>
```

### Customizing Test Parameters

You can override environment variables when invoking `run_test.sh`:

```bash
REGION="us-central1" \
TARGET_QPS="1000" \
DURATION_SECONDS="20" \
DESTROY_ON_EXIT="true" \
./test/run_test.sh <YOUR_PROJECT_ID>
```

See [`test/README.md`](./test/README.md) for detailed test documentation and manual cleanup instructions.
