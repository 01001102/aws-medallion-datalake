# Arquitetura Medalhão AWS Multi-Account

Implementação completa de arquitetura medalhão para processamento de dados de 4 APIs externas usando AWS Glue, Step Functions, EventBridge e Apache Iceberg com estratégia multi-account.

## Visão Geral da Arquitetura

```
APIs → Camada Bronze → Camada Silver → Camada Gold → Analytics
       (Dados Brutos)  (Dados Limpos)  (Dados Negócio)  (Athena)
```

## Principais Características

- **Processamento Paralelo**: 4 APIs processadas simultaneamente
- **Estratégia Incremental**: Processamento D-1 com controle de watermark
- **Apache Iceberg**: Capacidades avançadas de analytics
- **Orquestração Automática**: EventBridge + Step Functions
- **Multi-Account**: DEV isolado, HOM/PROD na mesma conta
- **Otimizado para Custos**: Modelo pay-per-use

## Estratégia Multi-Account

### Estrutura de Contas
- **Conta DEV**: Ambiente isolado para desenvolvimento
- **Conta PROD**: Ambientes HOM e PROD separados por tags

### Deploy por Ambiente

```bash
# Ambiente DEV
terraform workspace new dev
terraform plan -var-file="terraform.tfvars.dev"
terraform apply -var-file="terraform.tfvars.dev"

# Ambiente HOM
terraform workspace new hom
terraform plan -var-file="terraform.tfvars.hom"
terraform apply -var-file="terraform.tfvars.hom"

# Ambiente PROD
terraform workspace new prod
terraform plan -var-file="terraform.tfvars.prod"
terraform apply -var-file="terraform.tfvars.prod"
```

## Documentação

- [Fluxo da Arquitetura](ARCHITECTURE_FLOW.md) - Documentação técnica completa
- [Diagrama](arquitetura-medallion.drawio) - Representação visual da arquitetura (abrir com extensão Draw.io)

![alt text](image.png)

## Componentes

- **13 Jobs Glue** (4 genéricos + 1 Agilean por camada)
- **3 Buckets S3** (Bronze/Silver/Gold)
- **Step Functions** para orquestração
- **EventBridge** para agendamento (diário + semanal Agilean)
- **DynamoDB** para controle de watermark
- **Athena** para analytics
- **Tabelas Iceberg** para queries otimizadas

## API Agilean

### Características Especiais
- **Autenticação Bearer Token**: Login obrigatório antes de acessar endpoints
- **Execução Semanal**: Domingos às 02:00 (fora do horário comercial)
- **Alto Custo de Processamento**: Uso otimizado conforme recomendação
- **Dados Hierárquicos**: Projetos → Baseline/Orçamento → Cenários

### Endpoints Implementados
1. `/api/v1/users/login` - Autenticação
2. `/api/v1/projects` - Lista de projetos
3. `/api/v1/portal/long/budget-baseline-link-info/{projectId}` - Informações de baseline
4. `/api/v1/portal/long/scenario-value-table/{projectId}` - Dados do longo prazo

### Teste Local
```bash
cd scripts
python test_agilean_api.py <usuario> <senha>
```

## Governança e Tags

### Tags Padrão
- **Project**: medallion-pipeline
- **Environment**: dev/hom/prod
- **ManagedBy**: Terraform
- **Owner**: DataEngineering
- **Layer**: Bronze/Silver/Gold (para S3)
- **Component**: EventBridge/StepFunctions/Glue

### Naming Convention
```
{project-name}-{environment}-{component}-{suffix}
Exemplo: medallion-pipeline-prod-bronze-a1b2c3
```


## Estrutura do Projeto

```
├── scripts/                 # Scripts Glue (Bronze, Silver, Gold)
│   ├── error_handler/      # Lambda de tratamento de erro
│   ├── bronze_digit_daily.py
│   ├── silver_digit_clean.py
│   └── gold_digit_fixed.py
├── terraform/              # Infraestrutura como código
│   ├── *.tf               # Recursos AWS
│   └── terraform.tfvars.*  # Variáveis por ambiente
├── sql/                    # Queries Athena
├── step-functions/         # Definições Step Functions
└── docs/                   # Documentação técnica
```

## Deploy

Ver [DEPLOY.md](DEPLOY.md) para instruções detalhadas.

## Segurança

- Account IDs são exemplos genéricos
- URLs de APIs são placeholders
- Credenciais via AWS Secrets Manager
- Configurações sensíveis em .gitignore

## Autor

Desenvolvido por STIT Cloud 