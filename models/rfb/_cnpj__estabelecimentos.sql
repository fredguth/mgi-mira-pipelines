MODEL (
  name rfb_cnpj.estabelecimentos,
  depends_on ['cgu.notas']
);


SELECT 
    x.cnpj_basico::bigint as cnpj_basico,
    CASE 
        WHEN x.identificador_matriz_filial = 1 THEN true 
        ELSE false 
    END AS indicador_matriz,
    x.cnae::uinteger as cnae_principal,
    (LPAD(REGEXP_REPLACE(TRIM(x.cep), '[^0-9]', ''), 8, '0')::uinteger) as cep,
    x.id_municipio::uinteger as id_municipio,
    x.sigla_uf::uf as sigla_uf,
    d.valor as situacao_cadastral,
    d2.valor as motivo_situacao_cadastral
FROM raw.cnpj__estabelecimentos x 
LEFT JOIN raw.cnpj__dicionario d 
    ON TRIM(x.situacao_cadastral)::int = d.chave
    AND d.nome_coluna = 'situacao_cadastral'
LEFT JOIN raw.cnpj__dicionario d2 
    ON TRIM(x.motivo_situacao_cadastral)::int = d2.chave
    AND d2.nome_coluna = 'motivo_situacao_cadastral';