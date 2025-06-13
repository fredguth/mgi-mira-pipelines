MODEL (
  name rfb_cnpj.empresas,
  depends_on ['bd.cep']
);


WITH MATRIZES AS (
    SELECT DISTINCT * FROM rfb_cnpj.estabelecimentos
    WHERE indicador_matriz = true
),
EMPRESAS AS (
    SELECT
        e.cnpj_basico::uinteger as cnpj_basico,
        e.razao_social,
        e.natureza_juridica::uinteger as codigo_natureza_juridica,
        n.descricao as natureza_juridica,
        trim(n.tipo::varchar) as categoria_institucional,
        n.esfera as esfera,
        COALESCE(d.valor::varchar, 'Não Informado') as porte,
        e.ente_federativo,
        e.data_consulta::date as data_consulta
    FROM raw.cnpj__empresas e
    LEFT JOIN raw.cnpj__natureza_juridica n
        ON n.codigo::int = e.natureza_juridica::int
    LEFT JOIN raw.cnpj__dicionario d
        ON d.chave::int = e.porte::int
        AND d.nome_coluna = 'porte'
)
SELECT DISTINCT ON (e.cnpj_basico)
    printf('%08d', e.cnpj_basico::bigint) as cnpj_basico,
    e.razao_social, 
    e.codigo_natureza_juridica,
    e.natureza_juridica,
    e.categoria_institucional,
    coalesce(e.esfera, 
        CASE 
            WHEN e.ente_federativo = 'UNIÃO' THEN 'federal'
            WHEN e.ente_federativo LIKE '%-%' THEN 'municipal' 
            ELSE 'desconhecida' 
        END) as esfera,
    e.ente_federativo, 
    m.cnae_principal, 
    m.cep, 
    COALESCE(m.id_municipio, c.id_municipio) as id_municipio, 
    m.sigla_uf::uf,
    CASE 
        WHEN COALESCE(trim(m.situacao_cadastral), 'Desconhecida') = 'Ativa' THEN 'ativa'
        ELSE 'inativa'
    END as situacao_cadastral,
    COALESCE(trim(m.motivo_situacao_cadastral), 'Sem Motivo') as motivo_situacao_cadastral 
FROM empresas e 
LEFT JOIN matrizes m ON e.cnpj_basico = m.cnpj_basico
LEFT JOIN bd.cep c ON left(m.cep::varchar, 5) = left(c.cep::varchar, 5);


