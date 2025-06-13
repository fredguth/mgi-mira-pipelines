MODEL (
  name raw.nfe__eventos
);

SELECT DISTINCT * FROM READ_CSV('data/inputs/cgu_nfe/*Evento.csv', delim=';', header=true, encoding='latin-1', escape='"', auto_type_candidates=['VARCHAR']);