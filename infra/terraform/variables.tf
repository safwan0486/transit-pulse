variable "project_id" {
  description = "Google Cloud project ID to deploy into."
  type        = string
}

variable "region" {
  description = "Region for the bucket and BigQuery datasets (Singapore is closest to Malaysia)."
  type        = string
  default     = "asia-southeast1"
}

variable "environment" {
  description = "Deployment environment name, used in resource names and labels."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be \"dev\" or \"prod\"."
  }
}

variable "bronze_nearline_after_days" {
  description = "Move raw bronze objects to cheaper Nearline storage after this many days."
  type        = number
  default     = 30
}

variable "bronze_retention_days" {
  description = "Delete raw bronze objects after this many days (silver/gold are kept)."
  type        = number
  default     = 365
}

variable "quarantine_retention_days" {
  description = "Delete quarantined rows after this many days."
  type        = number
  default     = 90
}

variable "monthly_budget_myr" {
  description = "Monthly budget alert threshold in MYR (0 disables the budget)."
  type        = number
  default     = 0
}

variable "billing_account_id" {
  description = "Billing account for the budget alert (only needed when monthly_budget_myr > 0)."
  type        = string
  default     = ""
}
