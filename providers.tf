terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.profile_name

  default_tags {
    tags = {
      project_name         = var.project_name
      environment          = var.environment
      managed_by_terraform = "true"
      owner_name           = var.owner_name
    }
  }
}