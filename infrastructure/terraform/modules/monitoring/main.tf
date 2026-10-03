# CloudWatch alarms -> SNS. Local stack uses Prometheus/Grafana/Alertmanager; this is the AWS side.
resource "aws_sns_topic" "alerts" {
  name              = "${var.name}-alerts"
  kms_master_key_id = var.kms_key_arn
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_emails)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

# ---------------------------------------------------------------- RDS
resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${var.name}-rds-cpu-high"
  alarm_description   = "Source Postgres CPU > ${var.rds_cpu_threshold}% for 15 min"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  dimensions          = { DBInstanceIdentifier = var.db_instance_id }
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = var.rds_cpu_threshold
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

resource "aws_cloudwatch_metric_alarm" "rds_storage" {
  alarm_name          = "${var.name}-rds-free-storage-low"
  alarm_description   = "Source Postgres free storage < ${var.rds_free_storage_gb} GB"
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  dimensions          = { DBInstanceIdentifier = var.db_instance_id }
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.rds_free_storage_gb * 1024 * 1024 * 1024
  comparison_operator = "LessThanThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# ---------------------------------------------------------------- ECS
# Fires only when a service is meant to run (desired > 0) but fewer tasks are running.
resource "aws_cloudwatch_metric_alarm" "ecs_running_tasks" {
  for_each            = { for k, v in var.ecs_services : k => v if v.desired_count > 0 }
  alarm_name          = "${var.name}-${each.key}-tasks-below-desired"
  alarm_description   = "ECS service ${each.key} running tasks < ${each.value.desired_count} (needs Container Insights)"
  namespace           = "ECS/ContainerInsights"
  metric_name         = "RunningTaskCount"
  dimensions          = { ClusterName = var.ecs_cluster_name, ServiceName = each.value.service_name }
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 5
  threshold           = each.value.desired_count
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# ---------------------------------------------------------------- application logs
resource "aws_cloudwatch_log_metric_filter" "stream_failed" {
  name           = "${var.name}-stream-query-failed"
  log_group_name = var.stream_log_group
  pattern        = "\"query terminated with error\""
  metric_transformation {
    name          = "StreamQueryFailures"
    namespace     = var.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "stream_failed" {
  alarm_name          = "${var.name}-stream-query-failed"
  alarm_description   = "The Spark streaming query terminated with an error"
  namespace           = var.metric_namespace
  metric_name         = aws_cloudwatch_log_metric_filter.stream_failed.metric_transformation[0].name
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

resource "aws_cloudwatch_log_metric_filter" "errors" {
  for_each       = toset(var.app_log_groups)
  name           = "${var.name}-${replace(each.value, "/", "-")}-errors"
  log_group_name = each.value
  pattern        = "\" ERROR \""
  metric_transformation {
    name          = "AppErrors"
    namespace     = var.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "app_errors" {
  alarm_name          = "${var.name}-app-errors"
  alarm_description   = "More than ${var.error_threshold} ERROR log lines in 5 minutes across relay/stream"
  namespace           = var.metric_namespace
  metric_name         = "AppErrors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.error_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# Consumer lag is computed by the streaming job (atlas_stream_consumer_lag). In AWS it must be
# published to CloudWatch namespace "AtlasLake" (CloudWatch agent / ADOT scraping the job's
# Prometheus endpoint, or PutMetricData). Missing data is not treated as breaching.
resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  alarm_name          = "${var.name}-stream-consumer-lag"
  alarm_description   = "Streaming consumer lag > ${var.consumer_lag_threshold} records for 10 min"
  namespace           = var.metric_namespace
  metric_name         = "atlas_stream_consumer_lag"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = var.consumer_lag_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}
