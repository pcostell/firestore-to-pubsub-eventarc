output "function_name" {
  description = "The name of the deployed Cloud Run function (2nd Gen)."
  value       = module.firestore_eventarc_pubsub.function_name
}

output "function_uri" {
  description = "The URI of the deployed Cloud Run function (2nd Gen)."
  value       = module.firestore_eventarc_pubsub.function_uri
}

output "pubsub_topic_id" {
  description = "The full resource ID of the target Pub/Sub topic."
  value       = module.firestore_eventarc_pubsub.pubsub_topic_id
}

output "eventarc_trigger_names" {
  description = "Map of trigger key ('all' or collection group ID) to the created Eventarc trigger name."
  value       = module.firestore_eventarc_pubsub.eventarc_trigger_names
}

output "function_service_account_email" {
  description = "The email of the service account used by the Cloud Run function."
  value       = module.firestore_eventarc_pubsub.function_service_account_email
}

output "eventarc_service_account_email" {
  description = "The email of the service account used by the Eventarc trigger(s)."
  value       = module.firestore_eventarc_pubsub.eventarc_service_account_email
}
