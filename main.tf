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

# Optional: Create the Firestore database if var.create_database is true.
resource "google_firestore_database" "database" {
  count = var.create_database ? 1 : 0

  project                 = var.project_id
  name                    = var.database
  location_id             = var.firestore_location != "" ? var.firestore_location : var.region
  type                    = "FIRESTORE_NATIVE"
  delete_protection_state = "DELETE_PROTECTION_DISABLED"
  deletion_policy         = "DELETE"
}

module "firestore_eventarc_pubsub" {
  source = "./modules/firestore_eventarc_pubsub"

  project_id            = var.project_id
  region                = var.region
  database              = var.create_database ? google_firestore_database.database[0].name : var.database
  firestore_location    = var.firestore_location
  pubsub_topic          = var.pubsub_topic
  create_pubsub_topic   = var.create_pubsub_topic
  collection_groups     = var.collection_groups
  name_prefix           = var.name_prefix
  include_auth_context  = var.include_auth_context
  pubsub_payload_format = var.pubsub_payload_format
  enable_apis           = var.enable_apis
  labels                = var.labels
}
