output "function_name" {
  description = "The name of the deployed Cloud Run function (2nd Gen)."
  value       = google_cloudfunctions2_function.firestore_to_pubsub.name
}

output "function_uri" {
  description = "The URI of the deployed Cloud Run function (2nd Gen)."
  value       = google_cloudfunctions2_function.firestore_to_pubsub.service_config[0].uri
}

output "pubsub_topic_id" {
  description = "The full resource ID of the Pub/Sub topic receiving Firestore change events."
  value       = local.pubsub_topic_path
}

output "eventarc_trigger_names" {
  description = "Map of trigger key ('all' or collection group name) to Eventarc trigger name."
  value       = { for k, v in google_eventarc_trigger.firestore_changes : k => v.name }
}

output "function_service_account_email" {
  description = "The email of the service account used by the Cloud Run function."
  value       = google_service_account.function_sa.email
}

output "eventarc_service_account_email" {
  description = "The email of the service account used by the Eventarc trigger(s)."
  value       = google_service_account.eventarc_trigger_sa.email
}
