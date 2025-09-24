# Estratégia Multi-Account
locals {
  environment_configs = {
    dev = {
      account_id = var.dev_account_id
      region     = var.aws_region
    }
    hom = {
      account_id = var.prod_account_id
      region     = var.aws_region
    }
    prod = {
      account_id = var.prod_account_id
      region     = var.aws_region
    }
  }
  
  # Tags padrão por ambiente
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = "DataEngineering"
  }
}

# Provider para conta DEV
provider "aws" {
  alias  = "dev"
  region = var.aws_region
  
  assume_role {
    role_arn = "arn:aws:iam::${var.dev_account_id}:role/TerraformRole"
  }
  
  default_tags {
    tags = merge(local.common_tags, {
      Environment = "dev"
    })
  }
}

# Provider para conta PROD (HOM + PROD)
provider "aws" {
  alias  = "prod"
  region = var.aws_region
  
  assume_role {
    role_arn = "arn:aws:iam::${var.prod_account_id}:role/TerraformRole"
  }
  
  default_tags {
    tags = merge(local.common_tags, {
      Environment = var.environment
    })
  }
}