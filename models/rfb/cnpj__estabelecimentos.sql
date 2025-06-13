MODEL (
  name rfb_cnpj.estabelecimentos
);


SELECT 
cnpj_basico,
cnpj_ordem,
cnpj_dv,
identificador_matriz_filial,
nome_fantasia,
situacao_cadastral,
data_situacao_cadastral,
codigo_motivo,
nome_cidade_exterior,
id_pais,
data_inicio_atividade,
cnae_fiscal_principal,
cnae_fiscal_secundaria,
cep,
sigla_uf,
id_municipio_rf,
situacao_especial,
data_situacao_especial
FROM raw.cnpj__estabelecimentos;