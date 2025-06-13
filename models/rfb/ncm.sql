MODEL (
  name raw.ncm
);

SELECT
    unnest."Codigo" AS codigo,
    unnest."Descricao" AS descricao,
    strptime(unnest."Data_Inicio", '%d/%m/%Y')::date AS data_inicio
  FROM
    read_json('https://portalunico.siscomex.gov.br/classif/api/publico/nomenclatura/download/json'),
    UNNEST(Nomenclaturas) AS unnest;