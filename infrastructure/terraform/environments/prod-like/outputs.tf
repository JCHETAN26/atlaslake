output "lake_bucket" {
  value = module.storage.bucket_name
}

output "ecr_repositories" {
  value = module.storage.ecr_repository_urls
}

output "db_endpoint" {
  value = module.database.endpoint
}

output "db_secret_arn" {
  value = module.database.secret_arn
}

output "ecs_cluster" {
  value = module.compute.cluster_name
}

output "kafka_bootstrap" {
  value = module.compute.kafka_bootstrap
}

output "alerts_topic_arn" {
  value = module.monitoring.alerts_topic_arn
}
