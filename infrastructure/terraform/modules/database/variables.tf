variable "name" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "db_name" {
  type    = string
  default = "commerce"
}

variable "master_username" {
  type    = string
  default = "atlas_admin"
}

variable "engine_version" {
  type    = string
  default = "16"
}

variable "instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "allocated_storage_gb" {
  type    = number
  default = 20
}

variable "max_allocated_storage_gb" {
  type    = number
  default = 100
}

variable "multi_az" {
  type    = bool
  default = false
}

variable "backup_retention_days" {
  type    = number
  default = 1
}

variable "deletion_protection" {
  type    = bool
  default = false
}

variable "performance_insights" {
  type    = bool
  default = false
}

variable "logical_replication" {
  type    = bool
  default = false
}

variable "secret_recovery_window_days" {
  type    = number
  default = 0
}

variable "tags" {
  type    = map(string)
  default = {}
}
