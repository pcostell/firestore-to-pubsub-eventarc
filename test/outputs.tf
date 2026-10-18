output "database_id" {
  description = "The deployed Firestore Native Enterprise database ID."
  value       = google_firestore_database.test_db.name
}

output "function_name" {
  description = "The deployed Cloud Run function name."
  value       = module.firestore_eventarc_pubsub.function_name
}

output "pubsub_topic_id" {
  description = "The deployed Pub/Sub topic ID."
  value       = module.firestore_eventarc_pubsub.pubsub_topic_id
}

output "pubsub_subscription_id" {
  description = "The deployed Pub/Sub subscription ID."
  value       = google_pubsub_subscription.test_sub.id
}

output "load_generator_job_name" {
  description = "The deployed Cloud Run load generator job name."
  value       = google_cloud_run_v2_job.load_generator.name
}

output "subscriber_job_name" {
  description = "The deployed Cloud Run Pub/Sub subscriber job name."
  value       = google_cloud_run_v2_job.pubsub_subscriber.name
}

output "eventarc_trigger_names" {
  description = "Map of Eventarc triggers created by the module."
  value       = module.firestore_eventarc_pubsub.eventarc_trigger_names
}
