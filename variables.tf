variable "project_id" {
  description = "The GCP project ID hosting the Firestore database, Eventarc triggers, Cloud Run function, and Pub/Sub topic."
  type        = string
}

variable "region" {
  description = "The GCP region for deploying the Cloud Run function (2nd Gen) and source bucket."
  type        = string
  default     = "us-central1"
}

variable "database" {
  description = "The Firestore database ID to monitor for changes (e.g., '(default)' or a named database ID)."
  type        = string
  default     = "(default)"
}

variable "create_database" {
  description = "Whether to create the Firestore database as part of this Terraform project (set to false for an existing database)."
  type        = bool
  default     = false
}

variable "firestore_location" {
  description = "Optional location of the Firestore database for the Eventarc trigger(s) (e.g., 'nam5', 'eur3', or 'us-central1'). Defaults to var.region if empty."
  type        = string
  default     = ""
}

variable "pubsub_topic" {
  description = "The Pub/Sub topic name or full resource ID (projects/<project>/topics/<topic>) to publish Firestore changes to."
  type        = string
}

variable "create_pubsub_topic" {
  description = "Whether to create the Pub/Sub topic if it does not already exist."
  type        = bool
  default     = true
}

variable "collection_groups" {
  description = "Optional subset of Firestore collection groups to sync to Pub/Sub. Leave empty ([]) to sync all changes across the entire database."
  type        = list(string)
  default     = []
}

variable "name_prefix" {
  description = "Prefix used for naming the Cloud Run function, Eventarc trigger(s), and service accounts."
  type        = string
  default     = "fs-eventarc-pubsub"
}

variable "include_auth_context" {
  description = "Whether to include authentication context in the Firestore Eventarc events (.withAuthContext)."
  type        = bool
  default     = false
}

variable "pubsub_payload_format" {
  description = "Format of the message payload published to Pub/Sub ('json' or 'protobuf')."
  type        = string
  default     = "json"
}

variable "enable_apis" {
  description = "Whether to automatically enable required GCP APIs on the project."
  type        = bool
  default     = true
}

variable "labels" {
  description = "Optional labels to apply to created resources."
  type        = map(string)
  default     = {}
}
