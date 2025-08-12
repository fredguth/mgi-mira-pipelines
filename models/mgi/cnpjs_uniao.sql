MODEL (name mgi.cnpjs_uniao);

SELECT DISTINCT
    concat(cnpj_basico, cnpj_ordem, cnpj_dv) AS cnpj,
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    nome_fantasia,
    identificador_matriz_filial,
    situacao_cadastral,
    e.sigla_uf,
    e.cnae_fiscal_principal as cnae,
    c.atividade_economica,
    CASE
        WHEN left(cnae, 2) IN ('86', '87', '88', '21') THEN 'saude'
        WHEN left(cnae, 2) = '85' THEN 'educacao'
        ELSE 'admnistracao'
    END AS tipo_atividade

FROM
    rfb_cnpj.estabelecimentos e
    JOIN raw.cnpj__cnaes c ON e.cnae_fiscal_principal = c.cnae
WHERE
    concat(cnpj_basico, cnpj_ordem, cnpj_dv) IN (
        SELECT DISTINCT
            destinatario
        FROM
            mgi.itens_nota
    );