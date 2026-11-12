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
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
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
