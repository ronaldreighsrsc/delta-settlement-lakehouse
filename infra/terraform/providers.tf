provider "aws" {
  region                      = var.aws_region
  access_key                  = var.use_local_endpoint ? "mock_access_key" : null
  secret_key                  = var.use_local_endpoint ? "mock_secret_key" : null
  s3_use_path_style           = var.use_local_endpoint
  skip_credentials_validation = var.use_local_endpoint
  skip_metadata_api_check     = var.use_local_endpoint
  skip_requesting_account_id  = var.use_local_endpoint

  endpoints {
    s3  = var.use_local_endpoint ? var.local_s3_endpoint : null
    iam = var.use_local_endpoint ? var.local_s3_endpoint : null
    sts = var.use_local_endpoint ? var.local_s3_endpoint : null
  }
}

