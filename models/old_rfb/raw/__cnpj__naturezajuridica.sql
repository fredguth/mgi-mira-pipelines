MODEL (
  name raw.cnpj__natureza_juridica
);

select 
  codigo::int, 
  tipo, 
  descricao,
  case
  when descricao ilike '%federal' then 'federal'
  when descricao ilike '%estadual%' then 'estadual'
  when descricao ilike '%municipal%' then 'municipal'
   when descricao = 'União' then 'federal'
  when descricao = 'Estado ou Distrito Federal' then 'estadual'
  when descricao = 'Município' then 'municipal'
  end as esfera 
  from read_csv('data/inputs/rfb_cnpj/dicionario/naturezajuridica.csv', ignore_errors=true);

