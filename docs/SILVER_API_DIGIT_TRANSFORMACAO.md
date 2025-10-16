# Camada Silver - Regras de Limpeza e Tratamento

## Visão Geral
A camada Silver processa dados brutos do Bronze aplicando limpeza, padronização e validação para criar dados estruturados e confiáveis.

## Tabelas Processadas

### 1. OBRAS
**Origem:** `bronze/digit/obras/`  
**Destino:** `silver/digit/obras/`

**Transformações:**
- `CodigoCliente` → `codigo_cliente` (string)
- `CodigoObra` → `codigo_obra` (string)
- `DescricaoObra` → `nome_obra` (trim)
- `endereco` → `endereco` (coalesce com string vazia)
- `datainicio/datafim` → `data_inicio/data_fim` (0000-00-00 → null)
- Margens convertidas para int
- Campos adicionais: `area_prefeitura`, `tipologia`, `uso`, etc.

### 2. FUNCIONÁRIOS
**Origem:** `bronze/digit/funcionarios/`  
**Destino:** `silver/digit/funcionarios/`

**Transformações:**
- `CodigoFuncionario` → `codigo_funcionario` (string)
- `NomeCompleto` → `nome_completo` (remove tabs/quebras)
- `CPF/PIS` → `cpf/pis` (apenas números)
- `DataNascimento` → `data_nascimento` (0000-00-00 → null)
- `ativo` → `status_ativo` (S/N → true/false)
- `PrimeiroEmprego` → `primeiro_emprego` (S/N → true/false)
- Todos os campos de RG e carteira profissional preservados

### 3. CARGOS
**Origem:** `bronze/digit/cargos/`  
**Destino:** `silver/digit/cargos/`

**Transformações:**
- `CodigoCliente` → `codigo_cliente` (string)
- `CodigoCargo` → `codigo_cargo` (string)
- `DescricaoCargo` → `nome_cargo` (trim)
- `CodigoObra` → `codigo_obra_especifico` (string vazia → null)

### 4. HORAS TRABALHADAS
**Origem:** `bronze/digit/horas_funcao/`  
**Destino:** `silver/digit/horas_trabalhadas/`

**Transformações:**
- `obra_id` → `codigo_obra` (string)
- `CodigoCargo` → `codigo_cargo` (string)
- `DescricaoCargo` → `nome_cargo` (trim)
- `TotalHoras` → `total_horas` (double)
- Período fixo: 2025-05-01 a 2025-07-31
- `extraction_timestamp` → `timestamp_extracao` (unix → timestamp)

### 5. RESUMO PERÍODO
**Origem:** `bronze/digit/resumo_periodo/`  
**Destino:** `silver/digit/resumo_periodo/`

**Transformações:**
- `obra_id` → `codigo_obra` (string)
- `totalHoras` → `total_horas_periodo` (double)
- `totalFuncionarios` → `total_funcionarios` (int)
- `diasPeriodo` → `dias_periodo` (int)
- Período fixo: 2025-05-01 a 2025-07-31
- `extraction_timestamp` → `timestamp_extracao` (unix → timestamp)

### 6. FUNCIONÁRIOS HORAS (Detalhado)
**Origem:** `bronze/digit/relatorio_expandido/`  
**Destino:** `silver/digit/funcionarios_horas/`

**Transformações:**
- Expansão do JSON `Funcionarios` em registros individuais
- `obra_id` → `codigo_obra` (string)
- `CodigoCargo` → `codigo_cargo` (string)
- `TotalHoras` → `total_horas_cargo` (double)
- JSON parsing: `CodigoFuncionario`, `NomeCompleto`, `HorasTrabalhadas`
- Limpeza de nomes (remove tabs/quebras)
- `extraction_timestamp` → `timestamp_extracao` (unix → timestamp)

## Regras Gerais de Limpeza

### Strings
- **Trim**: Remove espaços em branco no início/fim
- **Regex**: Remove tabs, quebras de linha (\t\n\r)
- **Normalização**: Campos vazios tratados como null quando apropriado

### Números
- **CPF/PIS**: Apenas dígitos (remove pontuação)
- **Timestamps**: Unix timestamp convertido para datetime
- **Decimais**: Cast para double com precisão

### Datas
- **Datas inválidas**: 0000-00-00 convertido para null
- **Período padrão**: 2025-05-01 a 2025-07-31 (3 meses)

### Booleanos
- **S/N**: Convertido para true/false
- **Flags**: Campos ativo, primeiro_emprego padronizados

### JSON
- **Parsing**: JSON strings expandidas em colunas estruturadas
- **Schema**: Definição explícita de tipos para arrays/objetos

## Metadados Adicionados

Todas as tabelas recebem:
- `data_processamento`: Timestamp atual da transformação
- `fonte`: "digit_api" (identificação da origem)
- `data_extracao`: Data original da extração (preservada)
- `timestamp_extracao`: Timestamp original convertido

## Configurações Técnicas

- **Modo de escrita**: OVERWRITE (substitui dados anteriores)
- **Formato**: Parquet comprimido
- **Coalesce**: 1 arquivo por tabela
- **Filtros**: Remove registros com chaves primárias nulas
- **Glue Version**: 3.0
- **Workers**: 3 DPUs G.1X

## Validações Aplicadas

1. **Chaves primárias não nulas**: Filtro obrigatório
2. **Tipos de dados**: Cast explícito para tipos corretos
3. **Valores inválidos**: Tratamento de datas/strings vazias
4. **Consistência**: Padronização de nomenclatura
5. **Integridade**: Preservação de relacionamentos entre tabelas