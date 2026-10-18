locals {
  trigger_location  = var.firestore_location != "" ? var.firestore_location : var.region
  event_type        = var.include_auth_context ? "google.cloud.firestore.document.v1.written.withAuthContext" : "google.cloud.firestore.document.v1.written"
  sanitized_db_name = coalesce(trim(replace(lower(var.database), "/[^a-z0-9-]/", "-"), "-"), "default")

  topic_short_name = (
    startswith(var.pubsub_topic, "projects/")
    ? element(split("/", var.pubsub_topic), length(split("/", var.pubsub_topic)) - 1)
    : var.pubsub_topic
  )

  pubsub_topic_path = (
    var.create_pubsub_topic
    ? google_pubsub_topic.firestore_changes[0].id
    : (
      startswith(var.pubsub_topic, "projects/")
      ? var.pubsub_topic
      : "projects/${var.project_id}/topics/${var.pubsub_topic}"
    )
  )

  distinct_collection_groups = distinct([
    for cg in var.collection_groups : trimspace(cg)
    if length(trimspace(cg)) > 0
  ])

  # When collection_groups is empty, configure a single Eventarc trigger for all
  # document changes in the database. When collection_groups is provided, create
  # an Eventarc trigger per collection group using a multi-segment path pattern
  # (**/<collection_group>/{docId}) that matches both root collections and
  # subcollections at any depth.
  trigger_configs = (
    length(local.distinct_collection_groups) == 0
    ? {
      "all" = {
        suffix           = "all"
        document_pattern = null
      }
    }
    : {
      for cg in local.distinct_collection_groups : cg => {
        suffix           = substr(coalesce(trim(replace(lower(cg), "/[^a-z0-9-]/", "-"), "-"), "cg"), 0, 32)
        document_pattern = "**/${cg}/{docId}"
      }
    }
  )

  required_apis = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "eventarc.googleapis.com",
    "eventarcpublishing.googleapis.com",
    "firestore.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
  ])
}

# ------------------------------------------------------------------------------
# Enable Required GCP APIs
# ------------------------------------------------------------------------------
resource "google_project_service" "required_apis" {
  for_each = var.enable_apis ? local.required_apis : toset([])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# ------------------------------------------------------------------------------
# Target Cloud Pub/Sub Topic (Created if var.create_pubsub_topic == true)
# ------------------------------------------------------------------------------
resource "google_pubsub_topic" "firestore_changes" {
  count = var.create_pubsub_topic ? 1 : 0

  project = var.project_id
  name    = local.topic_short_name
  labels  = var.labels

  depends_on = [google_project_service.required_apis]
}

# ------------------------------------------------------------------------------
# Service Accounts & Least-Privilege IAM
# ------------------------------------------------------------------------------
resource "random_id" "suffix" {
  byte_length = 2
}

resource "google_service_account" "function_sa" {
  project      = var.project_id
  account_id   = "${substr(var.name_prefix, 0, 18)}-fn-${random_id.suffix.hex}"
  display_name = "Firestore to Pub/Sub Cloud Run Function SA (${var.database})"
  description  = "Runtime identity for Cloud Run function publishing Firestore Eventarc events to Pub/Sub."
}

resource "google_service_account" "eventarc_trigger_sa" {
  project      = var.project_id
  account_id   = "${substr(var.name_prefix, 0, 18)}-ea-${random_id.suffix.hex}"
  display_name = "Firestore Eventarc Trigger SA (${var.database})"
  description  = "Identity used by Eventarc triggers to invoke the Firestore-to-Pub/Sub Cloud Run function."
}

# Allow the Cloud Run function runtime SA to publish strictly to the target Pub/Sub topic.
resource "google_pubsub_topic_iam_member" "function_publisher" {
  project = var.project_id
  topic   = var.create_pubsub_topic ? google_pubsub_topic.firestore_changes[0].name : local.pubsub_topic_path
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
}

# Allow the Eventarc trigger SA to receive Eventarc events.
resource "google_project_iam_member" "eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.eventarc_trigger_sa.email}"
}

# Ensure the Eventarc P4SA exists and has the Eventarc Service Agent role.
resource "google_project_service_identity" "eventarc_agent" {
  provider = google-beta
  project  = var.project_id
  service  = "eventarc.googleapis.com"

  depends_on = [google_project_service.required_apis]
}

resource "google_project_iam_member" "eventarc_service_agent" {
  project = var.project_id
  role    = "roles/eventarc.serviceAgent"
  member  = "serviceAccount:${google_project_service_identity.eventarc_agent.email}"
}

# ------------------------------------------------------------------------------
# Cloud Run Function (2nd Gen) Source Archive & Bucket
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "function_source_bucket" {
  project                     = var.project_id
  name                        = "${var.project_id}-${var.name_prefix}-src-${random_id.suffix.hex}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels

  depends_on = [google_project_service.required_apis]
}

data "archive_file" "function_source_zip" {
  type        = "zip"
  source_dir  = "${path.module}/function_source"
  output_path = "/tmp/function_source_${random_id.suffix.hex}.zip"
}

resource "google_storage_bucket_object" "function_source_object" {
  name   = "firestore-to-pubsub-${data.archive_file.function_source_zip.output_sha256}.zip"
  bucket = google_storage_bucket.function_source_bucket.name
  source = data.archive_file.function_source_zip.output_path
}

# ------------------------------------------------------------------------------
# Cloud Run Function (2nd Gen)
# ------------------------------------------------------------------------------
resource "google_cloudfunctions2_function" "firestore_to_pubsub" {
  project     = var.project_id
  name        = "${var.name_prefix}-${local.sanitized_db_name}"
  location    = var.region
  description = "Forwards Firestore change events from database '${var.database}' to Pub/Sub topic '${local.topic_short_name}'."
  labels      = var.labels

  build_config {
    runtime     = "python312"
    entry_point = "handle_firestore_event"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source_bucket.name
        object = google_storage_bucket_object.function_source_object.name
      }
    }
  }

  service_config {
    available_cpu                    = var.function_cpu
    available_memory                 = var.function_memory
    max_instance_request_concurrency = var.function_concurrency
    timeout_seconds                  = var.function_timeout_seconds
    min_instance_count               = var.function_min_instances
    max_instance_count               = var.function_max_instances
    ingress_settings                 = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision   = true
    service_account_email            = google_service_account.function_sa.email

    environment_variables = {
      FIRESTORE_DATABASE    = var.database
      PUBSUB_TOPIC_PATH     = local.pubsub_topic_path
      COLLECTION_GROUPS     = join(",", local.distinct_collection_groups)
      PUBSUB_PAYLOAD_FORMAT = lower(var.pubsub_payload_format)
    }
  }

  depends_on = [
    google_project_service.required_apis,
    google_pubsub_topic_iam_member.function_publisher,
  ]
}

# Grant the Eventarc trigger SA permission to invoke the underlying Cloud Run v2 service.
resource "google_cloud_run_v2_service_iam_member" "eventarc_invoker" {
  project  = var.project_id
  location = google_cloudfunctions2_function.firestore_to_pubsub.location
  name     = google_cloudfunctions2_function.firestore_to_pubsub.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.eventarc_trigger_sa.email}"
}

# ------------------------------------------------------------------------------
# Eventarc Trigger(s) for Firestore Document Changes
# ------------------------------------------------------------------------------
resource "google_eventarc_trigger" "firestore_changes" {
  for_each = local.trigger_configs

  project                 = var.project_id
  name                    = substr("${var.name_prefix}-${local.sanitized_db_name}-${each.value.suffix}", 0, 63)
  location                = local.trigger_location
  service_account         = google_service_account.eventarc_trigger_sa.email
  event_data_content_type = "application/protobuf"
  labels                  = var.labels

  matching_criteria {
    attribute = "type"
    value     = local.event_type
  }

  matching_criteria {
    attribute = "database"
    value     = var.database
  }

  dynamic "matching_criteria" {
    for_each = each.value.document_pattern != null ? [each.value.document_pattern] : []
    content {
      attribute = "document"
      operator  = "match-path-pattern"
      value     = matching_criteria.value
    }
  }

  destination {
    cloud_run_service {
      service = google_cloudfunctions2_function.firestore_to_pubsub.name
      region  = var.region
    }
  }

  depends_on = [
    google_cloud_run_v2_service_iam_member.eventarc_invoker,
    google_project_iam_member.eventarc_receiver,
    google_project_iam_member.eventarc_service_agent,
  ]
}
