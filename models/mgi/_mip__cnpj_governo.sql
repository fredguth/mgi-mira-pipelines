MODEL (
  name mgi_mip.cnpj_governo
);

SELECT * from rfb_cnpj.empresas where categoria_institucional = 'Administração Pública' or natureza_juridica = 'Empresa Pública';


/*  POST-STATEMENT */
 @IF(
   @runtime_stage = 'evaluating',
    COPY mgi_mip.cnpj_governo to 'data/outputs/mgi_mip/cnpj_governo.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
 );
