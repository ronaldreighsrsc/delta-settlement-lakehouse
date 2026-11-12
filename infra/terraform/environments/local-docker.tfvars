# Safe $0 local Docker S3 emulator settings (Moto S3 server inside Docker Compose `cloud` profile)
aws_region          = "us-east-1"
environment         = "dev"
project_prefix      = "settlement-lakehouse"
use_local_endpoint  = true
local_s3_endpoint   = "http://s3-mock:5000"
enable_budget_guard = false

