# Usar databases existentes - definidos como locals
locals {
  digit_bronze_db_name = "digit_bronze_db"
  digit_silver_db_name = "digit_silver_db"
  digit_gold_db_name   = "digit_gold_db"
}

# Crawlers específicos para cada database
resource "aws_glue_crawler" "digit_bronze_crawler" {
  database_name = local.digit_bronze_db_name
  name          = "digit-bronze-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.bronze_bucket.bucket}/digit/"
  }

  schedule = "cron(0 3 * * ? *)"
  
  tags = merge(local.common_tags, {
    Layer = "Bronze"
    API = "Digit"
  })
}

resource "aws_glue_crawler" "digit_silver_crawler" {
  database_name = local.digit_silver_db_name
  name          = "digit-silver-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.silver_bucket.bucket}/digit/"
  }

  schedule = "cron(0 4 * * ? *)"
  
  tags = merge(local.common_tags, {
    Layer = "Silver"
    API = "Digit"
  })
}

resource "aws_glue_crawler" "digit_gold_crawler" {
  database_name = local.digit_gold_db_name
  name          = "digit-gold-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.gold_bucket.bucket}/digit/"
  }

  schedule = "cron(0 5 * * ? *)"
  
  tags = merge(local.common_tags, {
    Layer = "Gold"
    API = "Digit"
  })
}