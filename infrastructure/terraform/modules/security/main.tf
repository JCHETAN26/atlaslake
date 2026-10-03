# Customer-managed KMS key + least-access security groups.
data "aws_caller_identity" "current" {}

resource "aws_kms_key" "lake" {
  description             = "${var.name} lake / secrets / logs encryption"
  enable_key_rotation     = true
  deletion_window_in_days = var.kms_deletion_window_days

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AccountAdmin"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "CloudWatchLogs"
        Effect    = "Allow"
        Principal = { Service = "logs.${var.region}.amazonaws.com" }
        Action    = ["kms:Encrypt*", "kms:Decrypt*", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:Describe*"]
        Resource  = "*"
      },
    ]
  })

  tags = var.tags
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${var.name}-lake"
  target_key_id = aws_kms_key.lake.key_id
}

# ECS tasks: no inbound at all; outbound only HTTPS (endpoints/S3), Postgres and Kafka inside the VPC.
resource "aws_security_group" "ecs_tasks" {
  name        = "${var.name}-ecs-tasks"
  description = "AtlasLake ECS tasks (relay, stream)"
  vpc_id      = var.vpc_id
  tags        = var.tags
}

resource "aws_vpc_security_group_egress_rule" "ecs_https" {
  security_group_id = aws_security_group.ecs_tasks.id
  description       = "HTTPS to VPC endpoints and S3 gateway"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "ecs_postgres" {
  security_group_id            = aws_security_group.ecs_tasks.id
  description                  = "Postgres (RDS)"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.rds.id
}

resource "aws_vpc_security_group_egress_rule" "ecs_kafka" {
  security_group_id = aws_security_group.ecs_tasks.id
  description       = "Kafka (MSK IAM/TLS port range) inside the VPC"
  ip_protocol       = "tcp"
  from_port         = 9092
  to_port           = 9098
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_security_group" "rds" {
  name        = "${var.name}-rds"
  description = "Postgres reachable only from AtlasLake ECS tasks"
  vpc_id      = var.vpc_id
  tags        = var.tags
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_ecs" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from ECS tasks only"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.ecs_tasks.id
}

resource "aws_security_group" "msk" {
  name        = "${var.name}-msk"
  description = "MSK reachable only from AtlasLake ECS tasks"
  vpc_id      = var.vpc_id
  tags        = var.tags
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_ecs" {
  security_group_id            = aws_security_group.msk.id
  description                  = "Kafka IAM/TLS from ECS tasks"
  ip_protocol                  = "tcp"
  from_port                    = 9092
  to_port                      = 9098
  referenced_security_group_id = aws_security_group.ecs_tasks.id
}
