output "lake_bucket" {
  description = "Set LAKE_BUCKET to this (and unset S3_ENDPOINT_URL) to use GCS."
  value       = google_storage_bucket.lake.name
}

output "bigquery_datasets" {
  description = "dbt schema → BigQuery dataset."
  value       = { for k, d in google_bigquery_dataset.dbt : k => d.dataset_id }
}

output "pipeline_service_account" {
  description = "Service account the pipeline should run as."
  value       = google_service_account.pipeline.email
}
