output "kms_key_arn" {
  value = aws_kms_key.lake.arn
}

output "ecs_security_group_id" {
  value = aws_security_group.ecs_tasks.id
}

output "rds_security_group_id" {
  value = aws_security_group.rds.id
}

output "msk_security_group_id" {
  value = aws_security_group.msk.id
}
