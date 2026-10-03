output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "service_names" {
  value = { for k, s in aws_ecs_service.services : k => s.name }
}

output "log_group_names" {
  value = { for k, g in aws_cloudwatch_log_group.services : k => g.name }
}

output "task_role_arn" {
  value = aws_iam_role.task.arn
}

output "kafka_bootstrap" {
  value = local.kafka_bootstrap
}
