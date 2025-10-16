# S3 Bucket para resultados do Athena
resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results-${random_id.bucket_suffix.hex}"
}

# Athena Workgroup
resource "aws_athena_workgroup" "medallion_workgroup" {
  name = "${var.project_name}-medallion-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Athena Named Queries para camada Gold
resource "aws_athena_named_query" "gold_api_queries" {
  count       = 4
  name        = "gold_api_${count.index + 1}_analysis"
  workgroup   = aws_athena_workgroup.medallion_workgroup.name
  database    = aws_glue_catalog_database.medallion_db.name
  description = "Análise da API ${count.index + 1} na camada Gold"

  query = <<EOF
SELECT 
    *,
    DATE(created_at) as date_partition,
    HOUR(created_at) as hour_partition
FROM ${aws_glue_catalog_database.medallion_db.name}.gold_api_${count.index + 1}_iceberg
WHERE created_at >= current_date - interval '7' day
ORDER BY created_at DESC
LIMIT 1000;
EOF
}

# Data Source para Athena (Iceberg)
resource "aws_athena_data_catalog" "iceberg_catalog" {
  name        = "${var.project_name}-iceberg-catalog"
  description = "Iceberg catalog for medallion architecture"
  type        = "GLUE"

  parameters = {
    "catalog-id" = data.aws_caller_identity.current.account_id
  }
}

# Data source para obter account ID
data "aws_caller_identity" "current" {}