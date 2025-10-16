# Step Functions - Digit Pipeline
resource "aws_sfn_state_machine" "digit_pipeline" {
  name     = "digit-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Digit Pipeline - Bronze Silver Gold"
    StartAt = "Bronze"
    States = {
      Bronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "medallion-pipeline-dev-bronze-digit"
        }
        Next = "Silver"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "ErrorNotification"
          ResultPath  = "$.error"
        }]
      }
      Silver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "medallion-pipeline-dev-silver-digit"
        }
        Next = "Gold"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "ErrorNotification"
          ResultPath  = "$.error"
        }]
      }
      Gold = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "medallion-pipeline-dev-gold-digit"
        }
        Next = "Success"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "ErrorNotification"
          ResultPath  = "$.error"
        }]
      }
      Success = {
        Type   = "Pass"
        Result = "Pipeline executado com sucesso!"
        End    = true
      }
      ErrorNotification = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = "digit-pipeline-error-handler"
          Payload = {
            "Error.$"        = "$.error.Error"
            "Cause.$"        = "$.error.Cause"
            "StateName.$"    = "$$.State.Name"
            "ExecutionName.$" = "$$.Execution.Name"
            "Timestamp.$"    = "$$.State.EnteredTime"
          }
        }
        Next = "FailPipeline"
      }
      FailPipeline = {
        Type  = "Fail"
        Error = "PipelineError"
        Cause = "Pipeline falhou - notificação enviada"
      }
    }
  })

  tags = {
    Name = "digit-pipeline"
  }
}