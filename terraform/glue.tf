# Glue Database
resource "aws_glue_catalog_database" "medallion_db" {
  name = "${var.project_name}_medallion_db"
}

# Bronze Layer Jobs (referenciados do incremental-strategy.tf)
# Os jobs Bronze são definidos no incremental-strategy.tf como bronze_job_incremental

# Silver Layer Jobs
resource "aws_glue_job" "silver_job" {
  count    = 4
  name     = "${var.project_name}-silver-api-${count.index + 1}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.silver_bucket.bucket}/scripts/silver_job_${count.index + 1}.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"           = "job-bookmark-enable"
    "--enable-metrics"               = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--INPUT_BUCKET"                 = aws_s3_bucket.bronze_bucket.bucket
    "--OUTPUT_BUCKET"                = aws_s3_bucket.silver_bucket.bucket
    "--DATABASE_NAME"                = aws_glue_catalog_database.medallion_db.name
  }

  glue_version = "4.0"
}

# Gold Layer Jobs
resource "aws_glue_job" "gold_job" {
  count    = 4
  name     = "${var.project_name}-gold-api-${count.index + 1}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.gold_bucket.bucket}/scripts/gold_job_${count.index + 1}.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"           = "job-bookmark-enable"
    "--enable-metrics"               = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--INPUT_BUCKET"                 = aws_s3_bucket.silver_bucket.bucket
    "--OUTPUT_BUCKET"                = aws_s3_bucket.gold_bucket.bucket
    "--DATABASE_NAME"                = aws_glue_catalog_database.medallion_db.name
    "--additional-python-modules"    = "pyiceberg"
  }

  glue_version = "4.0"
}

# Glue Tables para Iceberg
resource "aws_glue_catalog_table" "gold_iceberg_tables" {
  count         = 4
  name          = "gold_api_${count.index + 1}_iceberg"
  database_name = aws_glue_catalog_database.medallion_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type"   = "ICEBERG"
    "format"       = "iceberg"
    "location"     = "s3://${aws_s3_bucket.gold_bucket.bucket}/gold/api_${count.index + 1}/"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.gold_bucket.bucket}/gold/api_${count.index + 1}/"
    input_format  = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
    output_format = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
    }
  }
}