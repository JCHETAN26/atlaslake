variable "environment" {
  type    = string
  default = "prod-like"
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "az_count" {
  description = "Private subnets/AZs. RDS needs a subnet group spanning >= 2 AZs even when single-AZ."
  type        = number
  default     = 3
}

variable "enable_interface_endpoints" {
  description = "Interface VPC endpoints (ECR/logs/secrets/KMS/STS) replace a NAT gateway for private ECS tasks."
  type        = bool
  default     = true
}

variable "db_instance_class" {
  type    = string
  default = "db.m7g.large"
}

variable "db_allocated_storage_gb" {
  type    = number
  default = 100
}

variable "db_multi_az" {
  type    = bool
  default = true
}

variable "db_backup_retention_days" {
  type    = number
  default = 7
}

variable "db_performance_insights" {
  type    = bool
  default = true
}

variable "deletion_protection" {
  type    = bool
  default = true
}

variable "force_destroy" {
  description = "Allow destroying a non-empty lake bucket / ECR repos."
  type        = bool
  default     = false
}

variable "kms_deletion_window_days" {
  type    = number
  default = 30
}

variable "bronze_ia_after_days" {
  type    = number
  default = 30
}

variable "app_image_tag" {
  type    = string
  default = "latest"
}

variable "enable_msk" {
  description = "Create MSK Serverless (IAM auth + TLS). false = point external_kafka_bootstrap at self-managed Kafka."
  type        = bool
  default     = false
}

variable "external_kafka_bootstrap" {
  type    = string
  default = "kafka.internal:9092"
}

variable "schema_registry_url" {
  type    = string
  default = "http://schema-registry.internal:8081"
}

variable "relay_desired_count" {
  description = "0 = deployed but not running (no Fargate cost)."
  type        = number
  default     = 0
}

variable "stream_desired_count" {
  description = "0 or 1 (single writer per streaming checkpoint)."
  type        = number
  default     = 0
}

variable "stream_cpu" {
  type    = number
  default = 4096
}

variable "stream_memory" {
  type    = number
  default = 16384
}

variable "container_insights" {
  type    = bool
  default = true
}

variable "alert_emails" {
  type    = list(string)
  default = []
}
