terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Production AWS Free Plan remote-state template:
  # backend "s3" {
  #   bucket = "settlement-lakehouse-tfstate-<account-id>"
  #   key    = "medallion/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

