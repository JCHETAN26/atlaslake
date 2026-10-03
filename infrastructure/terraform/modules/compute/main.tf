# ECS Fargate: the outbox relay and the Spark streaming job (Spark local[*] inside one task).
# desired_count defaults to 0 — deploying the stack costs nothing until a service is switched on.
data "aws_caller_identity" "current" {}

locals {
  app_image = "${var.app_image_repository_url}:${var.app_image_tag}"
  kafka_bootstrap = (var.enable_msk
    ? aws_msk_serverless_cluster.this[0].bootstrap_brokers_sasl_iam
  : var.external_kafka_bootstrap)

  common_env = [
    { name = "ATLAS_ENV", value = var.environment },
    { name = "ATLAS_PG_HOST", value = var.db_endpoint },
    { name = "ATLAS_PG_USER", value = var.db_username },
    { name = "ATLAS_S3_ENDPOINT", value = "https://s3.${var.region}.amazonaws.com" },
    { name = "ATLAS_S3_REGION", value = var.region },
    { name = "ATLAS_S3_BUCKET", value = var.bucket_name },
    { name = "ATLAS_KAFKA_BOOTSTRAP", value = local.kafka_bootstrap },
    { name = "ATLAS_SCHEMA_REGISTRY_URL", value = var.schema_registry_url },
    { name = "ATLAS_DATA_DIR", value = "/tmp/atlaslake" },
  ]
  db_password_secret = [
    { name = "ATLAS_PG_PASSWORD", valueFrom = "${var.db_secret_arn}:password::" },
  ]

  services = {
    relay = {
      command = ["atlas", "relay", "--follow"]
      cpu     = var.relay_cpu
      memory  = var.relay_memory
      desired = var.relay_desired_count
    }
    stream = {
      command = ["atlas", "stream", "--trigger-s", "10"]
      cpu     = var.stream_cpu
      memory  = var.stream_memory
      desired = var.stream_desired_count
    }
  }
}

resource "aws_ecs_cluster" "this" {
  name = var.name

  setting {
    name  = "containerInsights"
    value = var.container_insights ? "enabled" : "disabled"
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "services" {
  for_each          = local.services
  name              = "/atlaslake/${var.environment}/${each.key}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

# ------------------------------------------------------------------ IAM
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:*"]
    }
  }
}

# Execution role: pull the image, write logs, inject the DB secret. Nothing else.
resource "aws_iam_role" "execution" {
  name               = "${var.name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secret" {
  statement {
    sid       = "ReadDbSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.db_secret_arn]
  }
  statement {
    sid       = "DecryptSecret"
    actions   = ["kms:Decrypt"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "execution_secret" {
  name   = "db-secret"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secret.json
}

# Task role: what the application itself may do.
resource "aws_iam_role" "task" {
  name               = "${var.name}-ecs-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "task" {
  statement {
    sid       = "ListLakePrefixes"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [for p in var.lake_prefixes : "${p}*"]
    }
  }

  statement {
    sid     = "ReadWriteLakeObjects"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      for p in var.lake_prefixes : "${var.bucket_arn}/${p}*"
    ]
  }

  statement {
    sid       = "LakeKms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid       = "ReadDbSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.db_secret_arn]
  }

  statement {
    sid       = "PublishCustomMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["AtlasLake"]
    }
  }

  dynamic "statement" {
    for_each = var.enable_msk ? [1] : []
    content {
      sid = "MskIamAccess"
      actions = [
        "kafka-cluster:Connect", "kafka-cluster:DescribeCluster",
        "kafka-cluster:DescribeTopic", "kafka-cluster:CreateTopic",
        "kafka-cluster:ReadData", "kafka-cluster:WriteData",
        "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup",
      ]
      resources = [
        aws_msk_serverless_cluster.this[0].arn,
        "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}:topic/${var.name}-msk/*/commerce.*",
        "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}:group/${var.name}-msk/*/*",
      ]
    }
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "atlaslake-app"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

# ------------------------------------------------------------------ tasks + services
resource "aws_ecs_task_definition" "services" {
  for_each                 = local.services
  family                   = "${var.name}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name        = each.key
    image       = local.app_image
    essential   = true
    command     = each.value.command
    environment = local.common_env
    secrets     = local.db_password_secret
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.services[each.key].name
        awslogs-region        = var.region
        awslogs-stream-prefix = each.key
      }
    }
    readonlyRootFilesystem = false
  }])

  tags = var.tags
}

resource "aws_ecs_service" "services" {
  for_each        = local.services
  name            = each.key
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.services[each.key].arn
  desired_count   = each.value.desired
  launch_type     = "FARGATE"

  # one writer per checkpoint: never run two copies of a streaming query side by side
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [var.ecs_security_group_id]
    assign_public_ip = false
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  tags = var.tags
}

# ------------------------------------------------------------------ optional MSK Serverless
resource "aws_msk_serverless_cluster" "this" {
  count        = var.enable_msk ? 1 : 0
  cluster_name = "${var.name}-msk"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [var.msk_security_group_id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = var.tags
}
