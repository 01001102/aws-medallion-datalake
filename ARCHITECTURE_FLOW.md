# Arquitetura Medalhão AWS - Fluxo de Dados

## Visão Geral

Implementação completa de arquitetura medalhão para processamento de dados de 4 APIs externas, utilizando AWS Glue, Step Functions, EventBridge e Apache Iceberg para análises avançadas.

## Componentes da Arquitetura

### Orquestração
- **EventBridge**: Triggers automáticos (hourly + daily D-1)
- **Step Functions**: Orquestração de jobs em paralelo
- **DynamoDB**: Controle de watermark para processamento incremental

### Processamento
- **AWS Glue**: 12 jobs distribuídos em 3 camadas
- **S3**: Storage segregado por camadas (Bronze/Silver/Gold)
- **Apache Iceberg**: Formato de tabela para camada Gold

### Analytics
- **Athena**: Queries SQL na camada Gold
- **Glue Catalog**: Metadados centralizados

## Fluxo de Dados

### 1. Camada Bronze (Raw Data)
```
APIs Externas → Glue Jobs Bronze → S3 Bronze
```
- Ingestão de dados brutos das 4 APIs
- Particionamento por data
- Controle incremental via watermark
- Formato: Parquet

### 2. Camada Silver (Clean Data)
```
S3 Bronze → Glue Jobs Silver → S3 Silver
```
- Limpeza e padronização
- Remoção de duplicatas
- Validação de qualidade
- Formato: Parquet

### 3. Camada Gold (Business Data)
```
S3 Silver → Glue Jobs Gold → S3 Gold (Iceberg)
```
- Agregações de negócio
- Métricas calculadas
- Tabelas Iceberg para analytics
- Formato: Iceberg

### 4. Analytics Layer
```
S3 Gold → Athena → Business Intelligence
```
- Queries SQL otimizadas
- Análises ad-hoc
- Dashboards e relatórios

## Estratégia Incremental D-1

### Controle de Watermark
- **DynamoDB Table**: Armazena última execução por API
- **Processamento Diário**: Execução às 02:00 UTC
- **Filtro Temporal**: Dados do dia anterior (D-1)

### Fluxo Incremental
1. EventBridge dispara pipeline diário
2. Jobs consultam watermark no DynamoDB
3. APIs filtradas por intervalo de tempo
4. Processamento apenas de dados novos
5. Atualização do watermark

## Estrutura de Arquivos

```
terraform/
├── main.tf                    # Infraestrutura principal
├── glue.tf                    # Jobs Silver/Gold e tabelas
├── iam.tf                     # Roles e políticas
├── athena.tf                  # Configuração Athena
├── incremental-strategy.tf    # Jobs Bronze + estratégia D-1
├── variables.tf               # Variáveis
├── outputs.tf                 # Outputs
└── terraform.tfvars.example   # Exemplo de configuração

scripts/
├── bronze_job_template.py           # Template Bronze
├── bronze_incremental_template.py   # Template Bronze D-1
├── silver_job_template.py           # Template Silver
└── gold_job_template.py             # Template Gold

ARCHITECTURE_FLOW.md           # Documentação técnica
arquitetura-medallion.drawio   # Diagrama da arquitetura
README.md                      # Visão geral
```

## Configuração e Deploy

### Pré-requisitos
- Terraform >= 1.0
- AWS CLI configurado
- Permissões IAM adequadas

### Passos de Deploy
```bash
# 1. Clonar repositório
git clone <repository-url>
cd medallion-architecture

# 2. Configurar variáveis
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
vim terraform/terraform.tfvars

# 3. Deploy da infraestrutura
cd terraform/
terraform init
terraform plan
terraform apply

# 4. Upload dos scripts
aws s3 sync ../scripts/ s3://bronze-bucket/scripts/
```

### Configuração das APIs
Editar `terraform.tfvars`:
```hcl
api_urls = [
  "https://api1.example.com/data",
  "https://api2.example.com/data",
  "https://api3.example.com/data",
  "https://api4.example.com/data",
  "https://api5.example.com/data"
]
```

## Monitoramento

### CloudWatch Logs
- Logs de execução dos jobs Glue
- Métricas de performance
- Alertas de falha

### Job Bookmarks
- Controle automático de reprocessamento
- Evita duplicação de dados
- Otimização de performance

## Benefícios da Arquitetura

### Escalabilidade
- Processamento paralelo de múltiplas APIs
- Auto-scaling dos recursos AWS
- Particionamento eficiente dos dados

### Confiabilidade
- Controle de watermark para consistência
- Job bookmarks para recuperação
- Retry automático em falhas

### Performance
- Formato Iceberg para queries otimizadas
- Particionamento por data
- Compressão e otimização automática

### Governança
- Segregação clara por camadas
- Metadados centralizados no Glue Catalog
- Controle de acesso granular via IAM

## Custos Estimados

### Componentes Principais
- **AWS Glue**: Pay-per-use (DPU-hour)
- **S3**: Storage + requests
- **Step Functions**: Execuções
- **EventBridge**: Rules + invocations
- **DynamoDB**: On-demand pricing
- **Athena**: Pay-per-query (TB scanned)

### Otimizações
- Particionamento reduz scan do Athena
- Iceberg otimiza queries analíticas
- Job bookmarks evitam reprocessamento
- Compressão Parquet reduz storage

## Próximos Passos

1. **Implementar Data Quality**: Validações automáticas
2. **Adicionar Alertas**: Monitoramento proativo
3. **Otimizar Particionamento**: Baseado em padrões de acesso
4. **Implementar CDC**: Change Data Capture para APIs
5. **Adicionar Testes**: Validação automatizada dos pipelines