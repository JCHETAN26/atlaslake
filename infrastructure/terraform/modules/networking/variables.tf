variable "name" {
  type = string
}

variable "region" {
  type = string
}

variable "cidr_block" {
  type    = string
  default = "10.40.0.0/16"
}

variable "az_count" {
  description = "Number of AZs/private subnets (1 for dev, 2+ for prod-like; RDS Multi-AZ needs >= 2)."
  type        = number
  default     = 2
}

variable "enable_interface_endpoints" {
  description = "Interface endpoints cost ~$7/month each per AZ; required for ECS tasks without NAT."
  type        = bool
  default     = true
}

variable "tags" {
  type    = map(string)
  default = {}
}
