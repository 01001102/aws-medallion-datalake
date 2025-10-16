# DynamoDB Table para Watermark
resource "aws_dynamodb_table" "watermark_table" {
  name           = "${var.project_name}-${var.environment}-watermark"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "api_name"

  attribute {
    name = "api_name"
    type = "S"
  }

  tags = merge(local.common_tags, {
    Component = "DynamoDB"
  })
}