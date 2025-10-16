-- Queries de Exemplo para Athena - Digit Silver
-- Database: digit_silver_db

-- 1. Verificar tabelas disponíveis
SHOW TABLES IN digit_silver_db;

-- 2. Contar registros por tabela
SELECT 'obras' as tabela, COUNT(*) as total FROM digit_silver_db.digit_obras
UNION ALL
SELECT 'funcionarios' as tabela, COUNT(*) as total FROM digit_silver_db.digit_funcionarios
UNION ALL
SELECT 'cargos' as tabela, COUNT(*) as total FROM digit_silver_db.digit_cargos
UNION ALL
SELECT 'resumo_horas' as tabela, COUNT(*) as total FROM digit_silver_db.digit_resumo_horas;

-- 3. Análise de obras
SELECT 
    codigoobra,
    descricaoobra,
    COUNT(*) as total_funcionarios
FROM digit_silver_db.digit_funcionarios
WHERE codigoobra IS NOT NULL AND codigoobra != '0'
GROUP BY codigoobra, descricaoobra
ORDER BY total_funcionarios DESC
LIMIT 10;

-- 4. Funcionários por cargo
SELECT 
    c.descricaocargo,
    COUNT(f.codigofuncionario) as total_funcionarios
FROM digit_silver_db.digit_funcionarios f
JOIN digit_silver_db.digit_cargos c ON f.codigocargo = c.codigocargo
GROUP BY c.descricaocargo
ORDER BY total_funcionarios DESC
LIMIT 15;

-- 5. Funcionários ativos por obra
SELECT 
    codigoobra,
    descricaoobra,
    SUM(CASE WHEN ativo = 'S' THEN 1 ELSE 0 END) as funcionarios_ativos,
    SUM(CASE WHEN ativo = 'N' THEN 1 ELSE 0 END) as funcionarios_inativos,
    COUNT(*) as total_funcionarios
FROM digit_silver_db.digit_funcionarios
WHERE codigoobra IS NOT NULL AND codigoobra != '0'
GROUP BY codigoobra, descricaoobra
ORDER BY funcionarios_ativos DESC;

-- 6. Análise temporal de dados
SELECT 
    extraction_date,
    COUNT(*) as registros_processados
FROM digit_silver_db.digit_funcionarios
GROUP BY extraction_date
ORDER BY extraction_date DESC;

-- 7. Funcionários por gênero
SELECT 
    genero,
    COUNT(*) as total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentual
FROM digit_silver_db.digit_funcionarios
WHERE genero IS NOT NULL AND genero != ''
GROUP BY genero;

-- 8. Funcionários por faixa etária
SELECT 
    CASE 
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) < 25 THEN 'Até 24 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) BETWEEN 25 AND 34 THEN '25-34 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) BETWEEN 35 AND 44 THEN '35-44 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) BETWEEN 45 AND 54 THEN '45-54 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) >= 55 THEN '55+ anos'
        ELSE 'Não informado'
    END as faixa_etaria,
    COUNT(*) as total_funcionarios
FROM digit_silver_db.digit_funcionarios
WHERE datanascimento IS NOT NULL 
    AND datanascimento != ''
    AND YEAR(CAST(datanascimento AS DATE)) > 1900
GROUP BY 
    CASE 
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) < 25 THEN 'Até 24 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) BETWEEN 25 AND 34 THEN '25-34 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) BETWEEN 35 AND 44 THEN '35-44 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) BETWEEN 45 AND 54 THEN '45-54 anos'
        WHEN YEAR(CURRENT_DATE) - YEAR(CAST(datanascimento AS DATE)) >= 55 THEN '55+ anos'
        ELSE 'Não informado'
    END
ORDER BY total_funcionarios DESC;

-- 9. Dados de horas (se disponível)
SELECT 
    extraction_date,
    codigo_obra,
    COUNT(*) as registros_horas
FROM digit_silver_db.digit_resumo_horas
WHERE codigo_obra IS NOT NULL AND codigo_obra != '0'
GROUP BY extraction_date, codigo_obra
ORDER BY extraction_date DESC, registros_horas DESC
LIMIT 20;

-- 10. Schema das tabelas
DESCRIBE digit_silver_db.digit_funcionarios;
DESCRIBE digit_silver_db.digit_obras;
DESCRIBE digit_silver_db.digit_cargos;