variable "project_id" {
  description = "The GCP project ID hosting the Firestore database, Eventarc trigger(s), Cloud Run function, and Pub/Sub topic."
  type        = string
}

variable "region" {
  description = "The GCP region where the Cloud Run function (2nd Gen) and source storage bucket will be deployed (e.g., 'us-central1')."
  type        = string
  default     = "us-central1"
}

variable "database" {
  description = "The Firestore database ID to monitor for document changes (e.g., '(default)' or a named database ID)."
  type        = string
  default     = "(default)"

  validation {
    condition     = length(trimspace(var.database)) > 0
    error_message = "The database variable must not be empty."
  }
}

variable "firestore_location" {
  description = "The location of the Firestore database for the Eventarc trigger (e.g., 'nam5', 'eur3', or 'us-central1'). Defaults to var.region if empty."
  type        = string
  default     = ""
}

variable "pubsub_topic" {
  description = "The target Pub/Sub topic name or full resource ID (projects/<project>/topics/<topic>) to which Firestore changes are published."
  type        = string

  validation {
    condition     = length(trimspace(var.pubsub_topic)) > 0
    error_message = "The pubsub_topic variable must not be empty."
  }
}

variable "create_pubsub_topic" {
  description = "Whether this module should create the Pub/Sub topic. Set to false if the topic already exists."
  type        = bool
  default     = true
}

variable "collection_groups" {
  description = "Optional subset of Firestore collection groups to sync to Pub/Sub. When empty ([]), all changes across all collections in the database are synced."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cg in var.collection_groups : length(trimspace(cg)) > 0 && !strcontains(cg, "/")])
    error_message = "Each collection group ID must be non-empty and must not contain '/'."
  }
}

variable "name_prefix" {
  description = "Prefix used for naming the Cloud Run function, Eventarc trigger(s), and service accounts."
  type        = string
  default     = "fs-eventarc-pubsub"
}

variable "include_auth_context" {
  description = "Whether to use the '.withAuthContext' Firestore Eventarc event type (google.cloud.firestore.document.v1.written.withAuthContext)."
  type        = bool
  default     = false
}

variable "pubsub_payload_format" {
  description = "Format of the payload published to Pub/Sub: 'json' (decoded DocumentEventData with CloudEvent metadata envelope) or 'protobuf' (raw DocumentEventData protobuf bytes)."
  type        = string
  default     = "json"

  validation {
    condition     = contains(["json", "protobuf"], lower(var.pubsub_payload_format))
    error_message = "pubsub_payload_format must be either 'json' or 'protobuf'."
  }
}

variable "enable_apis" {
  description = "Whether to automatically enable required GCP APIs on the project."
  type        = bool
  default     = true
}

variable "function_cpu" {
  description = "CPU allocated to the Cloud Run function."
  type        = string
  default     = "2"
}

variable "function_memory" {
  description = "Memory allocated to the Cloud Run function."
  type        = string
  default     = "1Gi"
}

variable "function_concurrency" {
  description = "Maximum concurrent requests per Cloud Run function instance."
  type        = number
  default     = 80
}

variable "function_timeout_seconds" {
  description = "Request timeout in seconds for the Cloud Run function."
  type        = number
  default     = 60
}

variable "function_min_instances" {
  description = "Minimum number of Cloud Run function instances."
  type        = number
  default     = 0
}

variable "function_max_instances" {
  description = "Maximum number of Cloud Run function instances."
  type        = number
  default     = 100
}

variable "labels" {
  description = "Labels to apply to created resources."
  type        = map(string)
  default     = {}
}
