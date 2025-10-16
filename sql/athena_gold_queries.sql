-- QUERIES GOLD LAYER - ATHENA
-- Database: digit_gold_db

-- 1. Verificar tabelas Gold
SHOW TABLES IN digit_gold_db;

-- 2. Análise dimensional - Top obras por horas
SELECT 
    o.nome_obra,
    o.tipologia,
    SUM(f.horas_trabalhadas) as total_horas,
    COUNT(DISTINCT f.funcionario_key) as total_funcionarios,
    AVG(f.horas_trabalhadas) as media_horas
FROM digit_gold_db.dim_obras o
JOIN digit_gold_db.fato_horas_trabalhadas f ON o.obra_key = f.obra_key
GROUP BY o.nome_obra, o.tipologia
ORDER BY total_horas DESC
LIMIT 10;

-- 3. Produtividade por mês
SELECT 
    t.ano,
    t.mes,
    t.nome_mes,
    SUM(f.horas_trabalhadas) as total_horas_mes,
    COUNT(DISTINCT f.funcionario_key) as funcionarios_ativos,
    AVG(f.horas_trabalhadas) as media_horas_dia
FROM digit_gold_db.fato_horas_trabalhadas f
JOIN digit_gold_db.dim_tempo t ON f.data_key = t.data_key
GROUP BY t.ano, t.mes, t.nome_mes
ORDER BY t.ano, t.mes;

-- 4. Top funcionários por produtividade
SELECT 
    func.nome_completo,
    func.descricao_filial,
    SUM(fato.horas_trabalhadas) as total_horas,
    COUNT(DISTINCT fato.obra_key) as obras_trabalhadas,
    AVG(fato.horas_trabalhadas) as media_horas_dia
FROM digit_gold_db.fato_horas_trabalhadas fato
JOIN digit_gold_db.dim_funcionarios func ON fato.funcionario_key = func.funcionario_key
GROUP BY func.nome_completo, func.descricao_filial
ORDER BY total_horas DESC
LIMIT 15;

-- 5. Dashboard executivo por obra
SELECT 
    o.nome_obra,
    o.tipologia,
    o.uso,
    COUNT(DISTINCT f.funcionario_key) as total_funcionarios,
    SUM(f.horas_trabalhadas) as total_horas,
    AVG(f.horas_trabalhadas) as media_horas_funcionario,
    MIN(f.data_extracao) as primeiro_registro,
    MAX(f.data_extracao) as ultimo_registro
FROM digit_gold_db.dim_obras o
JOIN digit_gold_db.fato_horas_trabalhadas f ON o.obra_key = f.obra_key
GROUP BY o.nome_obra, o.tipologia, o.uso
ORDER BY total_horas DESC;

-- 6. Análise temporal - Fins de semana vs dias úteis
SELECT 
    CASE WHEN t.eh_fim_semana THEN 'Fim de Semana' ELSE 'Dia Útil' END as tipo_dia,
    COUNT(*) as total_registros,
    SUM(f.horas_trabalhadas) as total_horas,
    AVG(f.horas_trabalhadas) as media_horas
FROM digit_gold_db.fato_horas_trabalhadas f
JOIN digit_gold_db.dim_tempo t ON f.data_key = t.data_key
GROUP BY t.eh_fim_semana
ORDER BY total_horas DESC;

-- 7. Resumo agregado por obra (fato resumo)
SELECT 
    o.nome_obra,
    r.total_horas_periodo,
    r.total_funcionarios,
    r.media_horas_por_funcionario,
    r.media_horas_por_dia,
    r.periodo_inicio,
    r.periodo_fim
FROM digit_gold_db.dim_obras o
JOIN digit_gold_db.fato_resumo_obras r ON o.obra_key = r.obra_key
ORDER BY r.total_horas_periodo DESC;

-- 8. Análise por trimestre
SELECT 
    t.ano,
    t.trimestre,
    COUNT(DISTINCT f.obra_key) as obras_ativas,
    COUNT(DISTINCT f.funcionario_key) as funcionarios_ativos,
    SUM(f.horas_trabalhadas) as total_horas_trimestre
FROM digit_gold_db.fato_horas_trabalhadas f
JOIN digit_gold_db.dim_tempo t ON f.data_key = t.data_key
GROUP BY t.ano, t.trimestre
ORDER BY t.ano, t.trimestre;