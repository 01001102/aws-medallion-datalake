# Upload dos scripts Python para S3

# Upload script Bronze Agilean
resource "aws_s3_object" "bronze_agilean_script" {
  bucket = aws_s3_bucket.bronze_bucket.bucket
  key    = "scripts/bronze_agilean.py"
  source = "../scripts/bronze_agilean.py"
  etag   = filemd5("../scripts/bronze_agilean.py")

  tags = merge(local.common_tags, {
    Component = "Scripts"
    Layer     = "Bronze"
  })
}

# Upload script Silver Agilean
resource "aws_s3_object" "silver_agilean_script" {
  bucket = aws_s3_bucket.silver_bucket.bucket
  key    = "scripts/silver_agilean.py"
  source = "../scripts/silver_agilean.py"
  etag   = filemd5("../scripts/silver_agilean.py")

  tags = merge(local.common_tags, {
    Component = "Scripts"
    Layer     = "Silver"
  })
}

# Upload script Silver Digit
resource "aws_s3_object" "silver_digit_script" {
  bucket = aws_s3_bucket.silver_bucket.bucket
  key    = "scripts/silver_digit_clean.py"
  source = "../scripts/silver_digit_clean.py"
  etag   = filemd5("../scripts/silver_digit_clean.py")

  tags = merge(local.common_tags, {
    Component = "Scripts"
    Layer     = "Silver"
  })
}

# Upload script Gold Digit Star Schema
resource "aws_s3_object" "gold_digit_star_schema_script" {
  bucket = aws_s3_bucket.gold_bucket.bucket
  key    = "scripts/gold_digit_star_schema.py"
  source = "../scripts/gold_digit_fixed.py"
  etag   = filemd5("../scripts/gold_digit_fixed.py")

  tags = merge(local.common_tags, {
    Component = "Scripts"
    Layer     = "Gold"
  })
}