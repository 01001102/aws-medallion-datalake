# Camada Gold - Star Schema para Power BI

## Visão Geral
A camada Gold implementa um **Star Schema** otimizado para relatórios no Power BI, com dimensões e fatos que facilitam análises de produtividade e gestão de obras.

## Arquitetura Star Schema

```
        DIM_TEMPO
            |
DIM_OBRAS ──┼── FATO_HORAS_TRABALHADAS ── DIM_FUNCIONARIOS
            |                    |
        DIM_CARGOS              |
            |                    |
            └── FATO_RESUMO_OBRAS
```

## Dimensões (Tabelas de Referência)

### 1. DIM_OBRAS
**Chave:** `obra_key`
- `codigo_obra` - Código único da obra
- `nome_obra` - Nome/descrição da obra
- `endereco` - Localização da obra
- `data_inicio/data_fim` - Período da obra
- `area_prefeitura`, `tipologia`, `uso` - Características da obra

### 2. DIM_FUNCIONARIOS
**Chave:** `funcionario_key`
- `codigo_funcionario` - Código único do funcionário
- `nome_completo` - Nome do funcionário
- `cpf`, `genero`, `data_nascimento` - Dados pessoais
- `data_admissao`, `status_ativo` - Dados trabalhistas
- `codigo_filial`, `descricao_filial` - Empresa/filial

### 3. DIM_CARGOS
**Chave:** `cargo_key`
- `codigo_cargo` - Código único do cargo
- `nome_cargo` - Descrição do cargo

### 4. DIM_TEMPO
**Chave:** `data_key` (YYYYMMDD)
- `data_completa` - Data no formato YYYY-MM-DD
- `ano`, `mes`, `dia` - Componentes da data
- `trimestre`, `semestre` - Agrupamentos temporais
- `nome_mes`, `nome_dia_semana` - Nomes por extenso
- `eh_fim_semana` - Flag para fins de semana

## Fatos (Tabelas de Métricas)

### 1. FATO_HORAS_TRABALHADAS (Granular)
**Chaves Estrangeiras:**
- `obra_key` → DIM_OBRAS
- `funcionario_key` → DIM_FUNCIONARIOS
- `cargo_key` → DIM_CARGOS
- `data_key` → DIM_TEMPO

**Métricas:**
- `horas_trabalhadas` - Horas trabalhadas pelo funcionário
- `total_horas_cargo` - Total de horas do cargo na obra

### 2. FATO_RESUMO_OBRAS (Agregado)
**Chaves Estrangeiras:**
- `obra_key` → DIM_OBRAS
- `data_key` → DIM_TEMPO

**Métricas:**
- `total_horas_periodo` - Total de horas na obra no período
- `total_funcionarios` - Quantidade de funcionários
- `dias_periodo` - Dias do período
- `media_horas_por_funcionario` - Produtividade média
- `media_horas_por_dia` - Intensidade diária

## Relacionamentos no Power BI

### Relacionamentos 1:N
```
DIM_OBRAS (1) ←→ (N) FATO_HORAS_TRABALHADAS
DIM_FUNCIONARIOS (1) ←→ (N) FATO_HORAS_TRABALHADAS
DIM_CARGOS (1) ←→ (N) FATO_HORAS_TRABALHADAS
DIM_TEMPO (1) ←→ (N) FATO_HORAS_TRABALHADAS

DIM_OBRAS (1) ←→ (N) FATO_RESUMO_OBRAS
DIM_TEMPO (1) ←→ (N) FATO_RESUMO_OBRAS
```

## Métricas DAX Sugeridas

### Produtividade
```dax
Total Horas = SUM(FATO_HORAS_TRABALHADAS[horas_trabalhadas])
Média Horas por Funcionário = AVERAGE(FATO_RESUMO_OBRAS[media_horas_por_funcionario])
Produtividade por Cargo = DIVIDE([Total Horas], DISTINCTCOUNT(DIM_FUNCIONARIOS[funcionario_key]))
```

### Análise Temporal
```dax
Horas Mês Atual = CALCULATE([Total Horas], DATESMTD(DIM_TEMPO[data_completa]))
Crescimento MoM = DIVIDE([Horas Mês Atual] - [Horas Mês Anterior], [Horas Mês Anterior])
```

### Análise por Obra
```dax
Top 5 Obras = TOPN(5, VALUES(DIM_OBRAS[nome_obra]), [Total Horas])
Eficiência Obra = DIVIDE([Total Horas], [Dias Período])
```

## Dashboards Sugeridos

### 1. Dashboard Executivo
- **KPIs Principais**: Total horas, funcionários ativos, obras em andamento
- **Gráficos**: Evolução mensal, top obras, distribuição por cargo
- **Filtros**: Período, obra, cargo

### 2. Dashboard Operacional
- **Detalhamento**: Horas por funcionário, produtividade por cargo
- **Análises**: Comparativo obras, eficiência temporal
- **Drill-down**: Obra → Cargo → Funcionário

### 3. Dashboard Gerencial
- **Métricas**: Custos estimados, alocação de recursos
- **Tendências**: Sazonalidade, picos de produção
- **Alertas**: Obras com baixa produtividade

## Benefícios do Star Schema

### Performance
- **Consultas rápidas** com joins simples
- **Agregações otimizadas** nas tabelas fato
- **Índices eficientes** nas chaves primárias

### Usabilidade
- **Modelo intuitivo** para usuários de negócio
- **Relacionamentos claros** entre entidades
- **Flexibilidade** para diferentes análises

### Escalabilidade
- **Fácil adição** de novas dimensões
- **Histórico preservado** na dimensão tempo
- **Particionamento** por data para performance

## Configurações Técnicas

- **Formato**: Parquet otimizado
- **Particionamento**: Por data de extração
- **Compressão**: Snappy para balance performance/tamanho
- **Modo escrita**: OVERWRITE para refresh completo
- **Localização**: `s3://gold-bucket/digit_star_schema/`