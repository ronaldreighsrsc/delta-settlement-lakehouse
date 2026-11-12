output "bucket_names" {
  description = "Provisioned Medallion S3 bucket names keyed by zone."
  value       = { for zone, b in aws_s3_bucket.medallion : zone => b.bucket }
}

output "bucket_uris" {
  description = "Provisioned s3:// base URIs keyed by Medallion zone."
  value       = { for zone, b in aws_s3_bucket.medallion : zone => "s3://${b.bucket}" }
}

output "bucket_arns" {
  description = "Provisioned Medallion S3 bucket ARNs keyed by zone."
  value       = { for zone, b in aws_s3_bucket.medallion : zone => b.arn }
}

output "bronze_bucket_uri" {
  description = "S3 base URI for the Bronze ingestion bucket."
  value       = "s3://${aws_s3_bucket.medallion["bronze"].bucket}"
}

output "silver_bucket_uri" {
  description = "S3 base URI for the Silver validated Delta bucket."
  value       = "s3://${aws_s3_bucket.medallion["silver"].bucket}"
}

output "dlq_bucket_uri" {
  description = "S3 base URI for the Silver Dead Letter Queue Delta bucket."
  value       = "s3://${aws_s3_bucket.medallion["dlq"].bucket}"
}

output "gold_bucket_uri" {
  description = "S3 base URI for the Gold reconciled summary bucket."
  value       = "s3://${aws_s3_bucket.medallion["gold"].bucket}"
}

output "iam_role_arn" {
  description = "ARN of the scoped Medallion runtime IAM role."
  value       = aws_iam_role.lakehouse_runtime.arn
}

output "iam_policy_arn" {
  description = "ARN of the least-privilege Medallion S3 IAM policy."
  value       = aws_iam_policy.lakehouse_s3_rw.arn
}

