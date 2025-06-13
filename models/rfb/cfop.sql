MODEL (
  name raw.cfop
);

select 
CFOP::int  as cfop,
"Descrição Resumida" as cfop_descricao,
"Categoria" as cfop_categoria

 from read_xlsx('data/inputs/rfb_cnpj/dicionario/160314_Tabela_CFOP.xlsx');
