terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state (recommended). Create the bucket + lock table once, then uncomment:
  # backend "s3" {
  #   bucket         = "atlaslake-tfstate-<account-id>"
  #   key            = "prod-like/terraform.tfstate"
  #   region         = "us-west-2"
  #   dynamodb_table = "atlaslake-tflock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = local.tags
  }
}
