locals {
  common_tags = {
    Project     = var.project_prefix
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  medallion_bucket_arns = [for b in aws_s3_bucket.medallion : b.arn]
  medallion_object_arns = [for b in aws_s3_bucket.medallion : "${b.arn}/*"]
}

resource "aws_s3_bucket" "medallion" {
  for_each = var.medallion_zones

  bucket        = "${var.project_prefix}-${each.key}-${var.environment}"
  force_destroy = var.use_local_endpoint

  tags = merge(local.common_tags, {
    MedallionZone = each.key
  })
}

resource "aws_s3_bucket_public_access_block" "medallion" {
  for_each = aws_s3_bucket.medallion

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "medallion" {
  for_each = aws_s3_bucket.medallion

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "medallion" {
  for_each = aws_s3_bucket.medallion

  bucket = each.value.id
  rule {
    id     = "expire-noncurrent-and-aborted-uploads"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_iam_role" "lakehouse_runtime" {
  name = "${var.project_prefix}-runtime-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "ec2.amazonaws.com",
            "lambda.amazonaws.com",
            "glue.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_policy" "lakehouse_s3_rw" {
  name        = "${var.project_prefix}-s3-rw-policy-${var.environment}"
  description = "Least-privilege read/write access scoped exclusively to Medallion Lakehouse S3 buckets."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListMedallionBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = local.medallion_bucket_arns
      },
      {
        Sid    = "ReadWriteMedallionObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = local.medallion_object_arns
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lakehouse_s3_rw" {
  role       = aws_iam_role.lakehouse_runtime.name
  policy_arn = aws_iam_policy.lakehouse_s3_rw.arn
}

resource "aws_budgets_budget" "monthly_guard" {
  count = var.enable_budget_guard ? 1 : 0

  name         = "${var.project_prefix}-${var.environment}-monthly-cost-guard"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_limit_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}

# ==============================================================================
# Spec 005: AWS Glue Data Catalog (Deterministic Schema-as-Code over S3)
# ==============================================================================

resource "aws_glue_catalog_database" "settlement_lakehouse_db" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  name        = "settlement_lakehouse_db_${var.environment}"
  description = "Governed Hive-compatible Glue Data Catalog metastore for Silver and Gold Medallion Delta/Parquet tables."
}

resource "aws_glue_catalog_table" "silver_settlements" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  name          = "silver_settlements"
  database_name = aws_glue_catalog_database.settlement_lakehouse_db[0].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
    classification        = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.medallion["silver"].bucket}/silver_settlements/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "silver_settlements_parquet_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "transaction_id"
      type = "string"
    }
    columns {
      name = "batch_id"
      type = "string"
    }
    columns {
      name = "sender_bic"
      type = "string"
    }
    columns {
      name = "receiver_bic"
      type = "string"
    }
    columns {
      name = "settlement_amount"
      type = "double"
    }
    columns {
      name = "target_currency"
      type = "string"
    }
    columns {
      name = "booked_fx_rate"
      type = "double"
    }
    columns {
      name = "target_amount"
      type = "double"
    }
    columns {
      name = "status"
      type = "string"
    }
  }

  partition_keys {
    name = "settlement_currency"
    type = "string"
  }
  partition_keys {
    name = "settlement_date"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "fct_daily_settlements" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  name          = "fct_daily_settlements"
  database_name = aws_glue_catalog_database.settlement_lakehouse_db[0].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
    classification        = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.medallion["gold"].bucket}/gold_daily_reconciliation/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "fct_daily_settlements_parquet_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "corridor_key"
      type = "string"
    }
    columns {
      name = "transaction_count"
      type = "bigint"
    }
    columns {
      name = "total_settlement_amount"
      type = "double"
    }
    columns {
      name = "total_target_amount"
      type = "double"
    }
    columns {
      name = "weighted_avg_fx_rate"
      type = "double"
    }
  }

  partition_keys {
    name = "settlement_currency"
    type = "string"
  }
  partition_keys {
    name = "settlement_date"
    type = "string"
  }
}

# ==============================================================================
# Spec 005: Amazon Athena FinOps Workgroup & Serverless Named Queries
# ==============================================================================

resource "aws_athena_workgroup" "settlement_analytics" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  name        = "${var.project_prefix}-wg-${var.environment}"
  description = "Serverless Trino/Presto SQL workgroup with FinOps bytes-scanned cutoff guardrail."

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff_per_query

    result_configuration {
      output_location = "s3://${aws_s3_bucket.medallion["gold"].bucket}/athena-query-results/"
    }
  }

  tags = local.common_tags
}

resource "aws_athena_named_query" "daily_corridor_reconciliation" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  name        = "daily_corridor_reconciliation_${var.environment}"
  workgroup   = aws_athena_workgroup.settlement_analytics[0].id
  database    = aws_glue_catalog_database.settlement_lakehouse_db[0].name
  description = "Partition-pruned daily settlement reconciliation across currency corridors."

  query = <<-EOT
    SELECT
      settlement_date,
      settlement_currency,
      corridor_key,
      transaction_count,
      total_settlement_amount,
      total_target_amount,
      weighted_avg_fx_rate
    FROM fct_daily_settlements
    WHERE settlement_date = CURRENT_DATE - INTERVAL '1' DAY
      AND settlement_currency IN ('USD', 'EUR', 'GBP', 'CLP')
    ORDER BY total_settlement_amount DESC;
  EOT
}

# ==============================================================================
# Spec 005: Amazon CloudWatch Log Group & Quarantine DLQ Anomaly Metric Alarm
# ==============================================================================

resource "aws_cloudwatch_log_group" "lakehouse_pipeline" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  name              = "/aws/lakehouse/${var.project_prefix}-${var.environment}"
  retention_in_days = 30

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "dlq_quarantine_anomaly_alarm" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  alarm_name          = "${var.project_prefix}-${var.environment}-dlq-quarantine-alarm"
  alarm_description   = "Triggers when QuarantineRecordCount > 0 in SettlementLakehouse/DataQuality (ISO 4217 / SWIFT BIC contract rejection)."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "QuarantineRecordCount"
  namespace           = "SettlementLakehouse/DataQuality"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    Environment  = var.environment
    PipelineName = "settlement-medallion-lakehouse"
  }

  tags = local.common_tags
}

# ==============================================================================
# Spec 005: Amazon EventBridge S3 Bronze Rule + AWS Lambda Ingestion Trigger
# ==============================================================================

resource "aws_lambda_function" "bronze_ingestion_trigger" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  function_name = "${var.project_prefix}-bronze-trigger-${var.environment}"
  description   = "Event-driven ingestion trigger invoked by EventBridge on S3 Bronze Object Created events."
  role          = aws_iam_role.lakehouse_runtime.arn
  handler       = "src.lakehouse.aws_observability.lambda_bronze_event_handler"
  runtime       = "python3.12"
  filename      = "${path.module}/lambda_placeholder.zip"
  timeout       = 60
  memory_size   = 256

  environment {
    variables = {
      LAKEHOUSE_ENV              = var.environment
      LAKEHOUSE_S3_BRONZE_BUCKET = aws_s3_bucket.medallion["bronze"].bucket
      LAKEHOUSE_S3_SILVER_BUCKET = aws_s3_bucket.medallion["silver"].bucket
      LAKEHOUSE_S3_DLQ_BUCKET    = aws_s3_bucket.medallion["dlq"].bucket
      LAKEHOUSE_S3_GOLD_BUCKET   = aws_s3_bucket.medallion["gold"].bucket
      CLOUDWATCH_NAMESPACE       = "SettlementLakehouse/DataQuality"
    }
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_event_rule" "bronze_s3_object_created" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  name        = "${var.project_prefix}-bronze-object-created-${var.environment}"
  description = "Captures S3 Object Created events in the Bronze Medallion bucket to trigger Silver contract validation."

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created", "ObjectCreated"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.medallion["bronze"].bucket]
      }
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "invoke_bronze_lambda" {
  count = var.enable_aws_catalog_observability ? 1 : 0

  rule      = aws_cloudwatch_event_rule.bronze_s3_object_created[0].name
  target_id = "BronzeIngestionLambdaTarget"
  arn       = aws_lambda_function.bronze_ingestion_trigger[0].arn
}
