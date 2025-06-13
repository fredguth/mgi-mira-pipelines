MODEL (
  name raw.nfe__notas
);

-- https://portaldatransparencia.gov.br/download-de-dados/notas-fiscais
SELECT DISTINCT * FROM READ_CSV('data/inputs/cgu_nfe/*Fiscal.csv', delim=';', header=true, encoding='latin-1', escape='"', auto_type_candidates=['VARCHAR']) 