# Deploy Guide - Pipeline Medalhão Digit

## Pré-requisitos

- AWS CLI configurado
- Terraform instalado
- Permissões AWS necessárias

## Deploy por Ambiente

### 1. Desenvolvimento (DEV)
```bash
cd terraform
terraform workspace new dev
terraform init
terraform plan -var-file="terraform.tfvars.dev"
terraform apply -var-file="terraform.tfvars.dev"
```

### 2. Homologação (HOM)
```bash
cd terraform
terraform workspace new hom
terraform plan -var-file="terraform.tfvars.hom"
terraform apply -var-file="terraform.tfvars.hom"
```

### 3. Produção (PROD)
```bash
cd terraform
terraform workspace new prod
terraform plan -var-file="terraform.tfvars.prod"
terraform apply -var-file="terraform.tfvars.prod"
```

## Configuração de Secrets

Criar secret no AWS Secrets Manager:
```bash
aws secretsmanager create-secret \
  --name "medallion-pipeline/digit-api" \
  --description "Credenciais API Digit" \
  --secret-string '{"cliente_nome":"<CLIENTE>","chave_secreta":"<CHAVE>"}'
```

## Execução Manual

```bash
# Executar pipeline completo
aws stepfunctions start-execution \
  --state-machine-arn <ARN_STEP_FUNCTIONS> \
  --name "manual-$(date +%s)"
```

## Monitoramento

- **CloudWatch Logs**: `/aws/glue/jobs`
- **Step Functions**: Console AWS
- **Emails de erro**: Configurados via Lambda

## Troubleshooting

1. **Erro de permissões**: Verificar IAM roles
2. **Erro de secrets**: Verificar AWS Secrets Manager
3. **Erro de dados**: Verificar logs do Glue
4. **Pipeline não para**: Verificar Step Functions definition