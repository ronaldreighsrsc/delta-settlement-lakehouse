# Safe $0 LocalStack alternative endpoint configuration
aws_region          = "us-east-1"
environment         = "dev"
project_prefix      = "settlement-lakehouse"
use_local_endpoint  = true
local_s3_endpoint   = "http://localstack:4566"
enable_budget_guard = false

