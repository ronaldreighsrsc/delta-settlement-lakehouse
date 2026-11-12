variable "aws_region" {
  description = "Target AWS region for Medallion buckets and IAM resources."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment label (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "project_prefix" {
  description = "Globally unique naming prefix for S3 buckets and IAM policies."
  type        = string
  default     = "settlement-lakehouse"
}

variable "medallion_zones" {
  description = "Medallion architecture layers provisioned as isolated S3 buckets."
  type        = set(string)
  default     = ["bronze", "silver", "dlq", "gold"]
}

variable "noncurrent_version_retention_days" {
  description = "Days before expired noncurrent Delta object versions are purged."
  type        = number
  default     = 30
}

variable "use_local_endpoint" {
  description = "Route AWS provider calls to a local Docker S3 emulator ($0) instead of live AWS."
  type        = bool
  default     = true
}

variable "local_s3_endpoint" {
  description = "HTTP endpoint of local Docker S3/IAM emulator (Moto or LocalStack)."
  type        = string
  default     = "http://s3-mock:5000"
}

variable "enable_budget_guard" {
  description = "Provision an AWS Budgets monthly cost guardrail when targeting real AWS."
  type        = bool
  default     = false
}

variable "monthly_budget_limit_usd" {
  description = "Monthly cost ceiling in USD for AWS Free Plan guardrail alerts."
  type        = string
  default     = "1.0"
}

variable "budget_alert_email" {
  description = "Email subscriber notified when forecasted AWS cost crosses 80% of budget limit."
  type        = string
  default     = "alerts@example.com"
}

