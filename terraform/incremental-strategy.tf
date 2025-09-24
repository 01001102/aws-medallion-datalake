# DynamoDB para watermark D-1
resource "aws_dynamodb_table" "watermark_control" {
  name         = "${var.project_name}-watermark"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "api_name"

  attribute {
    name = "api_name"
    type = "S"
  }
}

# Atualizar Glue jobs com estratégia incremental
resource "aws_glue_job" "bronze_job_incremental" {
  count    = 4
  name     = "${var.project_name}-bronze-incremental-${count.index + 1}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.bronze_bucket.bucket}/scripts/bronze_incremental_${count.index + 1}.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable"
    "--API_URL"            = var.api_urls[count.index]
    "--OUTPUT_BUCKET"      = aws_s3_bucket.bronze_bucket.bucket
    "--WATERMARK_TABLE"    = aws_dynamodb_table.watermark_control.name
    "--API_NAME"           = "api_${count.index + 1}"
  }

  glue_version = "4.0"
}