MODEL (
  name raw.cnpj__cfop
);

select * from st_read('data/inputs/rfb_cnpj/dicionario/160314_Tabela_CFOP.xlsx');
