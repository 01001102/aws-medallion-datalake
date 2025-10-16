# Glue Databases
resource "aws_glue_catalog_database" "medallion_db" {
  name = "medallion_pipeline"
  description = "Database para arquitetura medallion"
}

resource "aws_glue_catalog_database" "digit_bronze_db" {
  name = "digit_bronze_db"
  description = "Digit Bronze Layer Database"
}

resource "aws_glue_catalog_database" "digit_silver_db" {
  name = "digit_silver_db"
  description = "Digit Silver Layer Database"
}

resource "aws_glue_catalog_database" "digit_gold_db" {
  name = "digit_gold_db"
  description = "Digit Gold Layer Database"
}