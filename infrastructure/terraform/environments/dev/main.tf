locals {
  name = "atlaslake-${var.environment}"
  tags = {
    Project     = "atlaslake"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_caller_identity" "current" {}

module "networking" {
  source                     = "../../modules/networking"
  name                       = local.name
  region                     = var.region
  cidr_block                 = var.vpc_cidr
  az_count                   = var.az_count
  enable_interface_endpoints = var.enable_interface_endpoints
  tags                       = local.tags
}

module "security" {
  source                   = "../../modules/security"
  name                     = local.name
  region                   = var.region
  vpc_id                   = module.networking.vpc_id
  vpc_cidr                 = module.networking.vpc_cidr
  kms_deletion_window_days = var.kms_deletion_window_days
  tags                     = local.tags
}

module "storage" {
  source               = "../../modules/storage"
  bucket_name          = "${local.name}-lake-${data.aws_caller_identity.current.account_id}"
  kms_key_arn          = module.security.kms_key_arn
  bronze_ia_after_days = var.bronze_ia_after_days
  force_destroy        = var.force_destroy
  tags                 = local.tags
}

module "database" {
  source                      = "../../modules/database"
  name                        = local.name
  subnet_ids                  = module.networking.private_subnet_ids
  security_group_id           = module.security.rds_security_group_id
  kms_key_arn                 = module.security.kms_key_arn
  instance_class              = var.db_instance_class
  allocated_storage_gb        = var.db_allocated_storage_gb
  multi_az                    = var.db_multi_az
  backup_retention_days       = var.db_backup_retention_days
  deletion_protection         = var.deletion_protection
  performance_insights        = var.db_performance_insights
  secret_recovery_window_days = var.deletion_protection ? 7 : 0
  tags                        = local.tags
}

module "compute" {
  source                   = "../../modules/compute"
  name                     = local.name
  environment              = var.environment
  region                   = var.region
  subnet_ids               = module.networking.private_subnet_ids
  ecs_security_group_id    = module.security.ecs_security_group_id
  msk_security_group_id    = module.security.msk_security_group_id
  kms_key_arn              = module.security.kms_key_arn
  bucket_name              = module.storage.bucket_name
  bucket_arn               = module.storage.bucket_arn
  db_endpoint              = module.database.endpoint
  db_username              = "atlas_admin"
  db_secret_arn            = module.database.secret_arn
  app_image_repository_url = module.storage.ecr_repository_urls["atlaslake-app"]
  app_image_tag            = var.app_image_tag
  enable_msk               = var.enable_msk
  external_kafka_bootstrap = var.external_kafka_bootstrap
  schema_registry_url      = var.schema_registry_url
  relay_desired_count      = var.relay_desired_count
  stream_desired_count     = var.stream_desired_count
  stream_cpu               = var.stream_cpu
  stream_memory            = var.stream_memory
  container_insights       = var.container_insights
  tags                     = local.tags
}

module "monitoring" {
  source           = "../../modules/monitoring"
  name             = local.name
  kms_key_arn      = module.security.kms_key_arn
  alert_emails     = var.alert_emails
  db_instance_id   = module.database.instance_id
  ecs_cluster_name = module.compute.cluster_name
  ecs_services = {
    relay  = { service_name = module.compute.service_names["relay"], desired_count = var.relay_desired_count }
    stream = { service_name = module.compute.service_names["stream"], desired_count = var.stream_desired_count }
  }
  stream_log_group = module.compute.log_group_names["stream"]
  app_log_groups   = values(module.compute.log_group_names)
  tags             = local.tags
}
