variable "project_id" {
  description = "The GCP project ID in which to deploy the test Firestore Native Enterprise database, Eventarc trigger, Cloud Run function, load generator job, and Pub/Sub subscriber job."
  type        = string
}

variable "region" {
  description = "The GCP region for all test resources."
  type        = string
  default     = "us-central1"
}

variable "database_id" {
  description = "The Firestore Native Enterprise database ID for the test."
  type        = string
  default     = "fs-eventarc-pubsub-test"
}

variable "pubsub_topic" {
  description = "The Pub/Sub topic name for the test."
  type        = string
  default     = "fs-eventarc-pubsub-test-topic"
}

variable "collection_groups" {
  description = "Optional subset of collection groups to sync during the test. Empty list syncs all collections."
  type        = list(string)
  default     = []
}

variable "target_qps" {
  description = "Total target write QPS for the load generator job."
  type        = number
  default     = 1000
}

variable "duration_seconds" {
  description = "Duration in seconds for the load generator job."
  type        = number
  default     = 20
}

variable "load_generator_tasks" {
  description = "Number of parallel Cloud Run Job tasks for the load generator."
  type        = number
  default     = 4
}

variable "sample_rate" {
  description = "Fraction of received Pub/Sub messages whose documents are fetched and validated against Firestore."
  type        = number
  default     = 0.05
}
