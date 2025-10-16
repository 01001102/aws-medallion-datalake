# Databases específicos para Agilean
locals {
  agilean_bronze_db_name = "agilean_bronze_db"
  agilean_silver_db_name = "agilean_silver_db"
  agilean_gold_db_name   = "agilean_gold_db"
}

# Crawlers específicos para cada database Agilean
resource "aws_glue_crawler" "agilean_bronze_crawler" {
  database_name = local.agilean_bronze_db_name
  name          = "agilean-bronze-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.bronze_bucket.bucket}/api=agilean/"
  }

  schedule = "cron(0 3 * * ? *)"
  
  tags = merge(local.common_tags, {
    Layer = "Bronze"
    API = "Agilean"
  })
}

resource "aws_glue_crawler" "agilean_silver_crawler" {
  database_name = local.agilean_silver_db_name
  name          = "agilean-silver-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.silver_bucket.bucket}/agilean/tables/"
  }

  schedule = "cron(0 4 * * ? *)"
  
  tags = merge(local.common_tags, {
    Layer = "Silver"
    API = "Agilean"
  })
}

resource "aws_glue_crawler" "agilean_gold_crawler" {
  database_name = local.agilean_gold_db_name
  name          = "agilean-gold-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.gold_bucket.bucket}/api=agilean/"
  }

  schedule = "cron(0 5 * * ? *)"
  
  tags = merge(local.common_tags, {
    Layer = "Gold"
    API = "Agilean"
  })
}