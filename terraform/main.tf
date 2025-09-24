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

# EventBridge Rule para trigger das APIs
resource "aws_events_rule" "api_trigger" {
  name                = "${var.project_name}-${var.environment}-api-trigger"
  description         = "Trigger para consumo das APIs - ${var.environment}"
  schedule_expression = var.environment == "prod" ? "cron(0 2 * * ? *)" : "rate(1 hour)"
  
  tags = {
    Component = "EventBridge"
  }
}

resource "aws_events_target" "step_function_target" {
  rule      = aws_events_rule.api_trigger.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.medallion_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "medallion_pipeline" {
  name     = "${var.project_name}-${var.environment}-medallion-pipeline"
  role_arn = aws_iam_role.step_function_role.arn
  
  tags = {
    Component = "StepFunctions"
  }

  definition = jsonencode({
    Comment = "Medallion Architecture Pipeline"
    StartAt = "ProcessAPIs"
    States = {
      ProcessAPIs = {
        Type = "Parallel"
        Branches = [
          for i in range(4) : {
            StartAt = "BronzeLayer${i + 1}"
            States = {
              "BronzeLayer${i + 1}" = {
                Type     = "Task"
                Resource = aws_glue_job.bronze_job_incremental[i].arn
                Next     = "SilverLayer${i + 1}"
              }
              "SilverLayer${i + 1}" = {
                Type     = "Task"
                Resource = aws_glue_job.silver_job[i].arn
                Next     = "GoldLayer${i + 1}"
              }
              "GoldLayer${i + 1}" = {
                Type = "Task"
                Resource = aws_glue_job.gold_job[i].arn
                End  = true
              }
            }
          }
        ]
        End = true
      }
    }
  })
}