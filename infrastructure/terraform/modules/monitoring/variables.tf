variable "name" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "alert_emails" {
  type    = list(string)
  default = []
}

variable "db_instance_id" {
  type = string
}

variable "rds_cpu_threshold" {
  type    = number
  default = 80
}

variable "rds_free_storage_gb" {
  type    = number
  default = 5
}

variable "ecs_cluster_name" {
  type = string
}

variable "ecs_services" {
  description = "service key => { service_name, desired_count }"
  type = map(object({
    service_name  = string
    desired_count = number
  }))
  default = {}
}

variable "stream_log_group" {
  type = string
}

variable "app_log_groups" {
  type    = list(string)
  default = []
}

variable "metric_namespace" {
  type    = string
  default = "AtlasLake"
}

variable "error_threshold" {
  type    = number
  default = 10
}

variable "consumer_lag_threshold" {
  type    = number
  default = 500000
}

variable "tags" {
  type    = map(string)
  default = {}
}
