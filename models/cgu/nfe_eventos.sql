-- histórico de eventos das notas fiscais eletrônicas. status da nfe.
MODEL (
  name cgu.eventos,
  depends_on (cgu.notas)
);


SELECT DISTINCT
    "CHAVE DE ACESSO" AS chave_acesso, -- identificador da nota fiscal em que este evento foi registrado. juntamente com timestamp_evento, forma a chave primária
    "EVENTO"::tipo_evento AS evento, -- tipo de evento registrado (autorização de uso, cancelamento, etc)
    strptime("DATA/HORA EVENTO", '%d/%m/%Y %H:%M:%S') AS timestamp_evento, -- data e hora do evento
    "DESCRIÇÃO EVENTO" AS descricao_evento, -- descrição do evento (Protocolo)
    "MOTIVO EVENTO" AS motivo_evento, -- motivo do evento. campo de preenchimento livre.
FROM raw.nfe__eventos order by timestamp_evento; 
   

/*  POST-STATEMENT */
 @IF(
   @runtime_stage = 'evaluating',
    COPY cgu.eventos to 'data/outputs/cgu/nfe/eventos.parquet' (FORMAT PARQUET,  OVERWRITE_OR_IGNORE)
 );