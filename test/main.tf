terraform {
  required_version = ">= 1.5.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4.0"
    }
    google = {
      source  = "hashicorp/google"
      version = ">= 5.30.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 5.30.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

resource "random_id" "test_suffix" {
  byte_length = 2
}

locals {
  database_id       = "${var.database_id}-${random_id.test_suffix.hex}"
  pubsub_topic_name = "${var.pubsub_topic}-${random_id.test_suffix.hex}"
  name_prefix       = "fseap-test-${random_id.test_suffix.hex}"
}

# ------------------------------------------------------------------------------
# 1. Test Firestore Native Enterprise Database
# ------------------------------------------------------------------------------
resource "google_firestore_database" "test_db" {
  provider = google-beta

  project                    = var.project_id
  name                       = local.database_id
  location_id                = var.region
  type                       = "FIRESTORE_NATIVE"
  database_edition           = "ENTERPRISE"
  firestore_data_access_mode = "DATA_ACCESS_MODE_ENABLED"
  delete_protection_state    = "DELETE_PROTECTION_DISABLED"
  deletion_policy            = "DELETE"
}

# ------------------------------------------------------------------------------
# 2. Deploy the Firestore -> Eventarc -> Cloud Run Function -> Pub/Sub Module
# ------------------------------------------------------------------------------
module "firestore_eventarc_pubsub" {
  source = "../modules/firestore_eventarc_pubsub"

  project_id             = var.project_id
  region                 = var.region
  database               = google_firestore_database.test_db.name
  firestore_location     = google_firestore_database.test_db.location_id
  pubsub_topic           = local.pubsub_topic_name
  create_pubsub_topic    = true
  collection_groups      = var.collection_groups
  name_prefix            = local.name_prefix
  function_cpu           = "2"
  function_memory        = "1Gi"
  function_concurrency   = 80
  function_min_instances = 10
  function_max_instances = 100
}

# ------------------------------------------------------------------------------
# 3. Pub/Sub Subscription for the Test Subscriber Job
# ------------------------------------------------------------------------------
resource "google_pubsub_subscription" "test_sub" {
  project              = var.project_id
  name                 = "${local.pubsub_topic_name}-sub"
  topic                = module.firestore_eventarc_pubsub.pubsub_topic_id
  ack_deadline_seconds = 60
}

# ------------------------------------------------------------------------------
# 4. Service Account & IAM for Load Generator and Subscriber Jobs
# ------------------------------------------------------------------------------
resource "google_service_account" "test_jobs_sa" {
  project      = var.project_id
  account_id   = "${local.name_prefix}-jobs-sa"
  display_name = "Firestore Eventarc Pub/Sub Test Jobs SA"
}

resource "google_project_iam_member" "test_jobs_datastore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.test_jobs_sa.email}"
}

resource "google_pubsub_subscription_iam_member" "test_jobs_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.test_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.test_jobs_sa.email}"
}

resource "google_project_iam_member" "test_jobs_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.test_jobs_sa.email}"
}

# ------------------------------------------------------------------------------
# 5. Cloud Run v2 Job: Firestore Load Generator (up to 1000 QPS)
# ------------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "load_generator" {
  project             = var.project_id
  name                = "${local.name_prefix}-load-gen"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = var.load_generator_tasks
    parallelism = var.load_generator_tasks

    template {
      service_account = google_service_account.test_jobs_sa.email
      timeout         = "${max(600, var.duration_seconds + 900)}s"
      max_retries     = 0

      containers {
        image   = "mirror.gcr.io/library/python:3.12-slim"
        command = ["/bin/sh", "-c"]
        args = [
          "pip install --no-cache-dir -q google-cloud-firestore && printf '%s' \"$PYTHON_SCRIPT\" > /tmp/load_generator.py && python3 -u /tmp/load_generator.py"
        ]

        resources {
          limits = {
            cpu    = "4"
            memory = "4Gi"
          }
        }

        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "FIRESTORE_DATABASE"
          value = google_firestore_database.test_db.name
        }
        env {
          name  = "TARGET_QPS"
          value = tostring(var.target_qps)
        }
        env {
          name  = "DURATION_SECONDS"
          value = tostring(var.duration_seconds)
        }
        env {
          name  = "COLLECTION_GROUPS"
          value = length(var.collection_groups) > 0 ? join(",", var.collection_groups) : "orders,inventory"
        }
        env {
          name  = "RUN_ID"
          value = "run-${random_id.test_suffix.hex}"
        }
        env {
          name  = "PYTHON_SCRIPT"
          value = file("${path.module}/scripts/load_generator.py")
        }
      }
    }
  }

  depends_on = [
    module.firestore_eventarc_pubsub,
    google_project_iam_member.test_jobs_datastore_user,
  ]
}

# ------------------------------------------------------------------------------
# 6. Cloud Run v2 Job: Pub/Sub Subscriber & Document Sample Validator
# ------------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "pubsub_subscriber" {
  project             = var.project_id
  name                = "${local.name_prefix}-subscriber"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.test_jobs_sa.email
      timeout         = "${max(600, var.duration_seconds + 900)}s"
      max_retries     = 0

      containers {
        image   = "mirror.gcr.io/library/python:3.12-slim"
        command = ["/bin/sh", "-c"]
        args = [
          "pip install --no-cache-dir -q google-cloud-firestore google-cloud-pubsub && printf '%s' \"$PYTHON_SCRIPT\" > /tmp/subscriber.py && python3 -u /tmp/subscriber.py"
        ]

        resources {
          limits = {
            cpu    = "8"
            memory = "8Gi"
          }
        }

        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "FIRESTORE_DATABASE"
          value = google_firestore_database.test_db.name
        }
        env {
          name  = "PUBSUB_SUBSCRIPTION_PATH"
          value = google_pubsub_subscription.test_sub.id
        }
        env {
          name  = "SAMPLE_RATE"
          value = tostring(var.sample_rate)
        }
        env {
          name  = "RUN_ID"
          value = "run-${random_id.test_suffix.hex}"
        }
        env {
          name  = "IDLE_TIMEOUT_SECONDS"
          value = "45"
        }
        env {
          name  = "EXPECTED_DURATION_SECONDS"
          value = tostring(var.duration_seconds)
        }
        env {
          name  = "MAX_RUNTIME_SECONDS"
          value = tostring(max(180, var.duration_seconds + 600))
        }
        env {
          name  = "PYTHON_SCRIPT"
          value = file("${path.module}/scripts/subscriber.py")
        }
      }
    }
  }

  depends_on = [
    google_pubsub_subscription_iam_member.test_jobs_subscriber,
    google_project_iam_member.test_jobs_datastore_user,
  ]
}
