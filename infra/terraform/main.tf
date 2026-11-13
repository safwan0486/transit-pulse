locals {
  name = "transit-pulse-${var.environment}"
  labels = {
    project     = "transit-pulse"
    environment = var.environment
    managed_by  = "terraform"
  }
  # dbt schemas → BigQuery datasets (same names as the DuckDB schemas)
  datasets = ["staging", "intermediate", "core", "performance", "monitoring"]
}

# ------------------------------------------------------------------ APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# ------------------------------------------------------------------ data lake
resource "google_storage_bucket" "lake" {
  name                        = "${var.project_id}-${local.name}-lake"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.environment == "dev"
  labels                      = local.labels

  # Raw data gets cheaper with age, then expires. Silver can always be
  # rebuilt from Kafka/bronze; bronze is the replay source of last resort.
  lifecycle_rule {
    condition {
      age            = var.bronze_nearline_after_days
      matches_prefix = ["bronze/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age            = var.bronze_retention_days
      matches_prefix = ["bronze/"]
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age            = var.quarantine_retention_days
      matches_prefix = ["quarantine/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# ------------------------------------------------------------------ warehouse
resource "google_bigquery_dataset" "dbt" {
  for_each = toset(local.datasets)

  dataset_id    = replace("${local.name}_${each.value}", "-", "_")
  friendly_name = "transit-pulse ${each.value}"
  description   = "dbt ${each.value} models for Transit Pulse (${var.environment})"
  location      = var.region
  labels        = local.labels

  delete_contents_on_destroy = var.environment == "dev"

  depends_on = [google_project_service.apis]
}

# ------------------------------------------------------------------ identity
# One service account for the pipeline, with only what it needs:
# read/write objects in THIS bucket and edit THESE datasets (no project-wide
# storage or BigQuery admin).
resource "google_service_account" "pipeline" {
  account_id   = "${local.name}-pipeline"
  display_name = "Transit Pulse pipeline (${var.environment})"
  depends_on   = [google_project_service.apis]
}

resource "google_storage_bucket_iam_member" "pipeline_lake" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_datasets" {
  for_each   = google_bigquery_dataset.dbt
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# Running queries is a project-level permission in BigQuery.
resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ------------------------------------------------------------------ cost guardrail
resource "google_billing_budget" "monthly" {
  count           = var.monthly_budget_myr > 0 ? 1 : 0
  billing_account = var.billing_account_id
  display_name    = "${local.name} monthly budget"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "MYR"
      units         = tostring(var.monthly_budget_myr)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }
}
