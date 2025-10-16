# Scripts Pipeline Medalhão

## Scripts Principais

### Bronze Layer
- `bronze_digit_daily.py` - **PRINCIPAL**: Carga D-1 de todos os endpoints
- `bronze_digit.py` - Carga histórica completa
- `bronze_agilean.py` - Carga semanal API Agilean

### Silver Layer  
- `silver_digit_clean.py` - **PRINCIPAL**: Limpeza e validação dados Digit
- `silver_agilean.py` - Processamento dados Agilean

### Gold Layer
- `gold_digit_fixed.py` - **PRINCIPAL**: Star schema e dimensões
- `gold_agilean.py` - Processamento final Agilean

## Scripts de Apoio
- `watermark_helper.py` - Controle incremental DynamoDB
- `run_digit_pipeline.py` - Execução manual pipeline
- `create_silver_tables_now.py` - Criação tabelas Silver

## Error Handler
- `error_handler/lambda_error_handler.py` - Notificação de erros
- `error_handler/email_templates.py` - Templates de email

## SQL
- `athena_queries_digit.sql` - Queries de análise