variable "name" {
  type = string
}

variable "environment" {
  type = string
}

variable "region" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "ecs_security_group_id" {
  type = string
}

variable "msk_security_group_id" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "bucket_arn" {
  type = string
}

variable "lake_prefixes" {
  description = "Only these prefixes of the lake bucket are readable/writable by the app."
  type        = list(string)
  default     = ["bronze/", "silver/", "gold/", "warehouse/"]
}

variable "db_endpoint" {
  type = string
}

variable "db_username" {
  type = string
}

variable "db_secret_arn" {
  type = string
}

variable "app_image_repository_url" {
  type = string
}

variable "app_image_tag" {
  type    = string
  default = "latest"
}

variable "enable_msk" {
  description = "Create an MSK Serverless cluster (IAM auth, TLS). Off by default: ~$0.75/h per cluster + throughput."
  type        = bool
  default     = false
}

variable "external_kafka_bootstrap" {
  description = "Bootstrap servers of a self-managed Kafka when enable_msk = false."
  type        = string
  default     = "kafka.internal:9092"
}

variable "schema_registry_url" {
  type    = string
  default = "http://schema-registry.internal:8081"
}

variable "relay_cpu" {
  type    = number
  default = 512
}

variable "relay_memory" {
  type    = number
  default = 1024
}

variable "relay_desired_count" {
  type    = number
  default = 0
}

variable "stream_cpu" {
  type    = number
  default = 2048
}

variable "stream_memory" {
  type    = number
  default = 8192
}

variable "stream_desired_count" {
  description = "Keep at 0 or 1: one Spark driver per checkpoint location."
  type        = number
  default     = 0

  validation {
    condition     = var.stream_desired_count <= 1
    error_message = "stream_desired_count must be 0 or 1 (single writer per streaming checkpoint)."
  }
}

variable "container_insights" {
  type    = bool
  default = false
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "tags" {
  type    = map(string)
  default = {}
}
