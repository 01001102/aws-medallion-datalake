# Jobs Glue para API Digit

# Job Bronze Digit
resource "aws_glue_job" "bronze_digit_job" {
  name     = "${var.project_name}-bronze-digit"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.bronze_bucket.bucket}/scripts/bronze_digit.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"           = "job-bookmark-enable"
    "--enable-metrics"               = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--BRONZE_BUCKET"                = aws_s3_bucket.bronze_bucket.bucket
    "--DIGIT_SECRET_NAME"            = aws_secretsmanager_secret.digit_credentials.name
    "--WATERMARK_TABLE"              = aws_dynamodb_table.watermark_table.name
    "--additional-python-modules"    = "requests"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
  max_retries       = 1

  tags = merge(local.common_tags, {
    Component = "Glue"
    Layer     = "Bronze"
    API       = "Digit"
  })
}

# Job Silver Digit
resource "aws_glue_job" "silver_digit_job" {
  name     = "${var.project_name}-silver-digit"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.silver_bucket.bucket}/scripts/silver_digit_clean.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"           = "job-bookmark-disable"
    "--enable-metrics"               = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--BRONZE_BUCKET"                = aws_s3_bucket.bronze_bucket.bucket
    "--SILVER_BUCKET"                = aws_s3_bucket.silver_bucket.bucket
    "--SILVER_DATABASE"              = local.digit_silver_db_name
    "--WATERMARK_TABLE"              = aws_dynamodb_table.watermark_table.name
    "--datalake-formats"             = "iceberg"
    "--conf"                         = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.silver_bucket.bucket}/warehouse/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  }

  glue_version         = "4.0"
  number_of_workers    = 2
  worker_type          = "G.1X"
  timeout              = 30
  max_retries          = 1

  tags = merge(local.common_tags, {
    Component = "Glue"
    Layer     = "Silver"
    API       = "Digit"
  })
}

# Job Gold Digit com Iceberg
resource "aws_glue_job" "gold_digit_job" {
  name     = "${var.project_name}-gold-digit"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.gold_bucket.bucket}/scripts/gold_digit_fixed.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"           = "job-bookmark-enable"
    "--enable-metrics"               = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--SILVER_BUCKET"                = aws_s3_bucket.silver_bucket.bucket
    "--GOLD_BUCKET"                  = aws_s3_bucket.gold_bucket.bucket
    "--SILVER_DATABASE"              = local.digit_silver_db_name
    "--GOLD_DATABASE"                = local.digit_gold_db_name
    "--WATERMARK_TABLE"              = aws_dynamodb_table.watermark_table.name
  }

  glue_version         = "4.0"
  number_of_workers    = 2
  worker_type          = "G.1X"
  timeout              = 30
  max_retries          = 1

  tags = merge(local.common_tags, {
    Component = "Glue"
    Layer     = "Gold"
    API       = "Digit"
  })
}

# Secret Digit definido em secrets.tf