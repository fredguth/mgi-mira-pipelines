MODEL (
  name rfb_cnpj.empresas,
);


select cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, 
REPLACE(capital_social, ',', '.')::decimal(18,2) as capital_social,
porte, ente_federativo from raw.cnpj__empresas;
