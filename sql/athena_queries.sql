-- QUERIES PARA TESTAR NO ATHENA

-- 1. Verificar tabelas disponíveis
SHOW TABLES IN medallion_pipeline;

-- 2. SILVER - Verificar dados das obras
SELECT 
    codigo_obra,
    nome_obra,
    tipologia,
    uso,
    area_prefeitura
FROM medallion_pipeline.silver_digit_obras
LIMIT 10;

-- 3. SILVER - Funcionários ativos por filial
SELECT 
    descricao_filial,
    COUNT(*) as total_funcionarios,
    COUNT(CASE WHEN status_ativo = true THEN 1 END) as funcionarios_ativos
FROM medallion_pipeline.silver_digit_funcionarios
GROUP BY descricao_filial
ORDER BY total_funcionarios DESC;

-- 4. SILVER - Horas trabalhadas por obra
SELECT 
    codigo_obra,
    SUM(horas_trabalhadas) as total_horas,
    COUNT(DISTINCT codigo_funcionario) as total_funcionarios,
    AVG(horas_trabalhadas) as media_horas_funcionario
FROM medallion_pipeline.silver_digit_funcionarios_horas
GROUP BY codigo_obra
ORDER BY total_horas DESC;

-- 5. GOLD - Análise dimensional de obras
SELECT 
    o.nome_obra,
    o.tipologia,
    o.uso,
    f.total_horas_periodo,
    f.total_funcionarios,
    f.media_horas_por_funcionario
FROM medallion_pipeline.gold_dim_obras o
JOIN medallion_pipeline.gold_fato_resumo_obras f
    ON o.obra_key = f.obra_key
ORDER BY f.total_horas_periodo DESC;

-- 6. GOLD - Top 10 funcionários por horas trabalhadas
SELECT 
    func.nome_completo,
    func.descricao_filial,
    SUM(fato.horas_trabalhadas) as total_horas,
    COUNT(DISTINCT fato.obra_key) as total_obras
FROM medallion_pipeline.gold_fato_horas_trabalhadas fato
JOIN medallion_pipeline.gold_dim_funcionarios func
    ON fato.funcionario_key = func.funcionario_key
GROUP BY func.nome_completo, func.descricao_filial
ORDER BY total_horas DESC
LIMIT 10;

-- 7. GOLD - Análise temporal de produtividade
SELECT 
    t.ano,
    t.mes,
    t.nome_mes,
    SUM(f.horas_trabalhadas) as total_horas_mes,
    COUNT(DISTINCT f.funcionario_key) as funcionarios_ativos,
    AVG(f.horas_trabalhadas) as media_horas_dia
FROM medallion_pipeline.gold_fato_horas_trabalhadas f
JOIN medallion_pipeline.gold_dim_tempo t
    ON f.data_key = t.data_key
GROUP BY t.ano, t.mes, t.nome_mes
ORDER BY t.ano, t.mes;

-- 8. GOLD - Dashboard executivo por obra
SELECT 
    o.nome_obra,
    o.tipologia,
    COUNT(DISTINCT f.funcionario_key) as total_funcionarios,
    SUM(f.horas_trabalhadas) as total_horas,
    AVG(f.horas_trabalhadas) as media_horas_funcionario,
    MIN(f.data_extracao) as primeiro_registro,
    MAX(f.data_extracao) as ultimo_registro
FROM medallion_pipeline.gold_dim_obras o
JOIN medallion_pipeline.gold_fato_horas_trabalhadas f
    ON o.obra_key = f.obra_key
GROUP BY o.nome_obra, o.tipologia
ORDER BY total_horas DESC;

-- 9. Verificar qualidade dos dados
SELECT 
    'silver_obras' as tabela,
    COUNT(*) as total_registros,
    COUNT(DISTINCT codigo_obra) as obras_unicas
FROM medallion_pipeline.silver_digit_obras

UNION ALL

SELECT 
    'silver_funcionarios' as tabela,
    COUNT(*) as total_registros,
    COUNT(DISTINCT codigo_funcionario) as funcionarios_unicos
FROM medallion_pipeline.silver_digit_funcionarios

UNION ALL

SELECT 
    'gold_fato_horas' as tabela,
    COUNT(*) as total_registros,
    COUNT(DISTINCT obra_key || funcionario_key || data_key) as combinacoes_unicas
FROM medallion_pipeline.gold_fato_horas_trabalhadas;