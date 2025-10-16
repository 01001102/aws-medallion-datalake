# IAM Role para Glue
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "${var.project_name}-glue-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.bronze_bucket.arn}/*",
          "${aws_s3_bucket.silver_bucket.arn}/*",
          "${aws_s3_bucket.gold_bucket.arn}/*",
          aws_s3_bucket.bronze_bucket.arn,
          aws_s3_bucket.silver_bucket.arn,
          aws_s3_bucket.gold_bucket.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*",
          "logs:*",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "secretsmanager:GetSecretValue",
          "athena:*",
          "lakeformation:GetDataAccess",
          "lakeformation:GrantPermissions",
          "lakeformation:BatchGrantPermissions",
          "lakeformation:RevokePermissions",
          "lakeformation:BatchRevokePermissions",
          "lakeformation:ListPermissions",
          "glue:CreateDatabase",
          "glue:GetDatabase",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions"
        ]
        Resource = [
          "*",
          aws_secretsmanager_secret.agilean_credentials.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# IAM Role para Step Functions
resource "aws_iam_role" "step_function_role" {
  name = "${var.project_name}-step-function-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_function_policy" {
  name = "${var.project_name}-step-function-policy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:BatchStopJobRun",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = "*"
      }
    ]
  })
}

# IAM Role para EventBridge
resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project_name}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${var.project_name}-eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.digit_pipeline.arn
      }
    ]
  })
}