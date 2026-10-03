variable "bucket_name" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "bronze_ia_after_days" {
  type    = number
  default = 30
}

variable "noncurrent_version_days" {
  type    = number
  default = 30
}

variable "ecr_repositories" {
  type    = list(string)
  default = ["atlaslake-app", "atlaslake-airflow"]
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty bucket / registries (dev only)."
  type        = bool
  default     = false
}

variable "tags" {
  type    = map(string)
  default = {}
}
