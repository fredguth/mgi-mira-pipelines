-- estabelecimentos cnpj
MODEL (name raw.cnpj__estabelecimentos, kind FULL);

SELECT
  cnpj,
  cnpj_basico,
  identificador_matriz_filial,
  nome_fantasia,
  situacao_cadastral,
  data_situacao_cadastral,
  motivo_situacao_cadastral,
  data_inicio_atividade,
  cnae_fiscal_principal AS cnae,
  cep,
  id_municipio,
  sigla_uf,
  situacao_especial,
  data_situacao_especial,
  data AS data_consulta
FROM
  read_csv(
    'data/inputs/rfb_cnpj/estabelecimentos/**/*/*.csv',
    ignore_errors = TRUE
  );