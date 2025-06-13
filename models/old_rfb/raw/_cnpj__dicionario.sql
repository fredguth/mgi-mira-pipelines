MODEL (
  name raw.cnpj__dicionario
);

select id_tabela, nome_coluna, chave::int, valor from read_csv('data/inputs/rfb_cnpj/dicionario/dicionario.csv');
