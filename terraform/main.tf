terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Buckets para as camadas
resource "aws_s3_bucket" "bronze_bucket" {
  bucket = "${var.project_name}-${var.environment}-bronze-${random_id.bucket_suffix.hex}"
  
  tags = {
    Layer = "Bronze"
    DataType = "Raw"
  }
}

resource "aws_s3_bucket" "silver_bucket" {
  bucket = "${var.project_name}-${var.environment}-silver-${random_id.bucket_suffix.hex}"
  
  tags = {
    Layer = "Silver"
    DataType = "Clean"
  }
}

resource "aws_s3_bucket" "gold_bucket" {
  bucket = "${var.project_name}-${var.environment}-gold-${random_id.bucket_suffix.hex}"
  
  tags = {
    Layer = "Gold"
    DataType = "Business"
  }
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# EventBridge Rule para trigger das APIs - Estratégia D-1
resource "aws_cloudwatch_event_rule" "api_trigger" {
  name                = "${var.project_name}-${var.environment}-api-trigger"
  description         = "Trigger D-1 para consumo das APIs - ${var.environment}"
  schedule_expression = var.environment == "prod" ? "cron(0 2 * * ? *)" : "cron(0 8 * * ? *)"
  state               = "ENABLED"  # Habilitado para D-1
  
  tags = merge(local.common_tags, {
    Component = "EventBridge"
  })
}

resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.api_trigger.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.digit_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

# Upload dos scripts para S3
resource "aws_s3_object" "glue_scripts" {
  for_each = {
    "bronze_agilean.py" = "../scripts/bronze_agilean.py"
    "bronze_digit.py"   = "../scripts/bronze_digit.py"
    "silver_agilean.py" = "../scripts/silver_agilean.py"
    "silver_digit.py"   = "../scripts/silver_digit.py"
    "gold_agilean.py"   = "../scripts/gold_agilean.py"
    "gold_digit_iceberg.py" = "../scripts/gold_digit_iceberg.py"
  }
  
  bucket = aws_s3_bucket.bronze_bucket.bucket
  key    = "scripts/${each.key}"
  source = each.value
  etag   = filemd5(each.value)
  
  tags = merge(local.common_tags, {
    Component = "Scripts"
  })
}