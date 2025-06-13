MODEL (
  name bd.cep
);

SELECT 
    *
FROM read_csv('data/inputs/bd_cep/**/*.csv',  quote='"', escape='')
