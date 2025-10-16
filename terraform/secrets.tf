# Secrets Manager para credenciais da API Agilean
resource "aws_secretsmanager_secret" "agilean_credentials" {
  name        = "${var.project_name}-${var.environment}-agilean-credentials"
  description = "Credenciais para API Agilean - ${var.environment}"

  tags = merge(local.common_tags, {
    Component = "SecretsManager"
    API       = "Agilean"
  })
}

# IMPORTANTE: Valores devem ser definidos manualmente via AWS Console ou CLI
# Não incluir credenciais reais no código
resource "aws_secretsmanager_secret_version" "agilean_credentials" {
  secret_id = aws_secretsmanager_secret.agilean_credentials.id
  secret_string = jsonencode({
    userName = "PLACEHOLDER_USER"
    password = "PLACEHOLDER_PASSWORD"
  })
  
  lifecycle {
    ignore_changes = [secret_string]
  }
}

# Secrets Manager para credenciais da API Digit
resource "aws_secretsmanager_secret" "digit_credentials" {
  name        = "${var.project_name}-${var.environment}-digit-credentials"
  description = "Credenciais para API Digit - ${var.environment}"

  tags = merge(local.common_tags, {
    Component = "SecretsManager"
    API       = "Digit"
  })
}

resource "aws_secretsmanager_secret_version" "digit_credentials" {
  secret_id = aws_secretsmanager_secret.digit_credentials.id
  secret_string = jsonencode({
    cliente_nome = "planoeplano"
    chave_secreta = "PLACEHOLDER_KEY"
    api_url = "https://www.digitca.com.br/api/ServiceLayerPlano"
  })
  
  lifecycle {
    ignore_changes = [secret_string]
  }
}