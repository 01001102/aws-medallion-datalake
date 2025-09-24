output "bronze_bucket_name" {
  description = "Nome do bucket Bronze"
  value       = aws_s3_bucket.bronze_bucket.bucket
}

output "silver_bucket_name" {
  description = "Nome do bucket Silver"
  value       = aws_s3_bucket.silver_bucket.bucket
}

output "gold_bucket_name" {
  description = "Nome do bucket Gold"
  value       = aws_s3_bucket.gold_bucket.bucket
}

output "athena_workgroup_name" {
  description = "Nome do workgroup do Athena"
  value       = aws_athena_workgroup.medallion_workgroup.name
}

output "glue_database_name" {
  description = "Nome do database do Glue"
  value       = aws_glue_catalog_database.medallion_db.name
}

output "step_function_arn" {
  description = "ARN da Step Function"
  value       = aws_sfn_state_machine.medallion_pipeline.arn
}