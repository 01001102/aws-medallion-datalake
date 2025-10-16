# Lambda Error Handler
resource "aws_lambda_function" "digit_error_handler" {
  filename         = "../scripts/error_handler/lambda_error_handler.zip"
  function_name    = "digit-pipeline-error-handler"
  role            = aws_iam_role.lambda_error_role.arn
  handler         = "lambda_error_handler.lambda_handler"
  runtime         = "python3.9"
  timeout         = 60

  environment {
    variables = {
      RECIPIENT_EMAIL = "ivan.franca@stitcloud.com"
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_error_policy,
    aws_cloudwatch_log_group.lambda_error_logs,
  ]

  tags = {
    Name = "digit-pipeline-error-handler"
  }
}

# IAM Role para Lambda
resource "aws_iam_role" "lambda_error_role" {
  name = "lambda-error-handler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Policy para Lambda
resource "aws_iam_role_policy" "lambda_error_policy" {
  name = "lambda-error-handler-policy"
  role = aws_iam_role.lambda_error_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ses:SendEmail",
          "ses:SendRawEmail"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attachment da policy básica
resource "aws_iam_role_policy_attachment" "lambda_error_policy" {
  role       = aws_iam_role.lambda_error_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_error_logs" {
  name              = "/aws/lambda/digit-pipeline-error-handler"
  retention_in_days = 14
}