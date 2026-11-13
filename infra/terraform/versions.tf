terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.10"
    }
  }

  # For a team, keep state remotely (create this bucket once by hand, then
  # uncomment). Local state is fine for a personal project.
  # backend "gcs" {
  #   bucket = "<project-id>-tfstate"
  #   prefix = "transit-pulse"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
