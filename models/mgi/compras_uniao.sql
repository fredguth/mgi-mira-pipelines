MODEL (name mgi.compras_uniao);

WITH normalized AS (
    SELECT
        n.* EXCLUDE(n.quantidade, n.valor_unitario, n.valor_total_item, n.unidade), -- Exclui campos de quantidade e unidade
        n.valor_total_item::DECIMAL(18,2) * c.multiplicador AS valor_total_item, -- Aplica multiplicador ao valor total
        WEEKOFYEAR(n.timestamp_emissao) AS semana_emissao, -- Adiciona a semana do ano (1-53)
        u.cnae,
        u.atividade_economica,
        CASE
            WHEN LEFT(u.cnae, 2) IN ('86', '87', '88', '21') THEN 'saude'
            WHEN LEFT(u.cnae, 2) = '85' THEN 'educacao'
            ELSE 'administracao'
        END AS tipo_atividade
    FROM itens_nota n
    JOIN cfops c ON n.cfop = c.cfop
    JOIN cnpjs_uniao u ON n.destinatario = u.cnpj
),
grouped AS (
    -- Agrega dados por dimensões temporais, geográficas e de classificação - apenas valores totais
    SELECT
        timestamp_emissao::DATE AS data,
        ano_emissao AS ano,
        MONTH(timestamp_emissao) AS mes,
        semana_emissao AS semana_no_ano,
        uf_emitente,
        uf_destinatario,
        tipo_atividade, 
        cfop,
        descricao_cfop AS cfop_descricao,
        ncm,
        descricao_ncm AS ncm_descricao,
        SUM(valor_total_item) AS valor -- Apenas valor total das compras
    FROM normalized
    WHERE valor_total_item != 0 -- Filtra apenas itens com valor total positivo ou negativo
    GROUP BY ALL
)
-- Resultado final com mapeamento para classificações SCN - apenas valores totais
SELECT
    ano,
    mes, 
    semana_no_ano,
    data,
    LEFT(p.SCN128_COD, 4) AS SCN68, -- Extrai código SCN68 (4 primeiros dígitos)
    d.SCN67_NOME AS atividade,
    p.SCN128_COD AS SCN128,
    d.SCN128_NOME AS produto,
    g.ncm,
    g.ncm_descricao AS ncm_nome,
    g.cfop,
    g.cfop_descricao AS cfop_nome,
    g.uf_emitente,
    g.uf_destinatario,
    g.tipo_atividade, -- Added this field that was missing
    g.valor -- Apenas o valor total das compras
FROM grouped g
JOIN de_ncm_para_scn128 p ON g.ncm = p.NCM_COD -- Mapeia NCM para SCN128
JOIN dicionario_scn d ON d.SCN128_COD = p.SCN128_COD -- Busca descrições SCN
WHERE g.ncm IS NOT NULL;
