MODEL (
    name mgi.ncm_cfops
);

SELECT a.ano_emissao, 
         a.mes_emissao, 
         a.codigo_ncm, 
         a.categoria_ncm, 
         a.cfop, 
         c.cfop_descricao, 
         SUM(a.valor_total::decimal(38,2)) AS valor 
  FROM cgu.itens_nota a
  JOIN raw.cfop c ON a.cfop = c.cfop
  GROUP BY ALL;
/*  POST-STATEMENT */
 @IF(
   @runtime_stage = 'evaluating',
    COPY mgi.ncm_cfops to 'data/outputs/mgi/por_ncm_cfops.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
 );

@IF(
   @runtime_stage = 'evaluating',
     COPY mgi.ncm_cfops to 'data/outputs/mgi/por_ncm_cfops.xlsx'  WITH (FORMAT xlsx, HEADER true)
 );

