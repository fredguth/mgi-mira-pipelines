MODEL (
  name raw.nfe__itens
);

SELECT DISTINCT * FROM READ_CSV('data/inputs/cgu_nfe/*Item.csv', delim=';', header=true, encoding='latin-1', escape='"', auto_type_candidates=['VARCHAR']) ;