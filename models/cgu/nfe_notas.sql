-- informacoes sobre notas fiscais eletrônicas emitidas por órgãos públicos
MODEL (
  name cgu.notas
);

-- PRE-STATEMENT
DROP TYPE IF EXISTS tipo_modelo;
CREATE TYPE tipo_modelo AS ENUM ('55 - NF-E EMITIDA EM SUBSTITUIÇÃO AO MODELO 1 OU 1A');

DROP TYPE IF EXISTS tipo_presenca;
CREATE TYPE tipo_presenca AS ENUM (
    '0 - NÃO SE APLICA',
    '1 - OPERAÇÃO PRESENCIAL',
    '2 - OPERAÇÃO NÃO PRESENCIAL, PELA INTERNET',
    '3 - OPERAÇÃO NÃO PRESENCIAL, TELEATENDIMENTO',
    '5 - NÃO INFORMADO',
    '9 - OPERAÇÃO NÃO PRESENCIAL, OUTROS'
);

DROP TYPE IF EXISTS uf;
CREATE TYPE uf AS ENUM (
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
    'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
    'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
);

DROP TYPE IF EXISTS indicador_ie;
CREATE TYPE indicador_ie AS ENUM (
    'CONTRIBUINTE ICMS',
    'CONTRIBUINTE ISENTO', 
    'NÃO CONTRIBUINTE'
);

DROP TYPE IF EXISTS tipo_consumidor;
CREATE TYPE tipo_consumidor AS ENUM (
    '0 - NORMAL',
    '1 - CONSUMIDOR FINAL'
);

DROP TYPE IF EXISTS tipo_destino;
CREATE TYPE tipo_destino AS ENUM (
    '1 - OPERAÇÃO INTERNA',
    '2 - OPERAÇÃO INTERESTADUAL',
    '3 - OPERAÇÃO COM EXTERIOR'
);

DROP TYPE IF EXISTS tipo_evento;
CREATE TYPE tipo_evento AS ENUM (
    'Autorização de Uso',
    'Sem informação',
    'Manifestação do destinatário - Operação não realizada',
    'Manifestação do destinatário - Ciência da operação',
    'Carta de correção',
    'Manifestação do destinatário - Confirmação da operação',
    'Cancelamento da NF-e',
    'Manifestação do destinatário - Desconhecimento da operação'
);

-- MODEL CREATION

SELECT DISTINCT
  "CHAVE DE ACESSO" AS chave_acesso, -- identificador único da nota fiscal eletrônica
  "MODELO"::tipo_modelo AS modelo, -- código do modelo de documento fiscao (55 - NFE)
  "SÉRIE"::USMALLINT AS serie, -- série da nota fiscal. juntamente com o número identifica unicamente a nota fiscal
  "NÚMERO"::BIGINT AS numero, -- número da nota fiscal. juntamente com a série identifica unicamente a nota fiscal
  "NATUREZA DA OPERAÇÃO" AS natureza_operacao, -- descrição da natureza da operação. campo de preenchimento livre.
  strptime("DATA EMISSÃO", '%d/%m/%Y %H:%M:%S') AS timestamp_emissao, -- data e hora de emissão da nota fiscal
  timestamp_emissao::date AS data_emissao, -- data de emissão da nota fiscal
  month(data_emissao)::UTINYINT AS mes_emissao, -- mês de emissão da nota fiscal
  year(data_emissao)::USMALLINT AS ano_emissao, -- ano de emissão da nota fiscal
  "EVENTO MAIS RECENTE"::tipo_evento AS ultimo_evento, -- evento mais recente associado a nota fiscal (indica status atual da nota)
  strptime("DATA/HORA EVENTO MAIS RECENTE", '%d/%m/%Y %H:%M:%S') AS timestamp_ultimo_evento, -- data e hora da última atualização do status da nota fiscal
  "CPF/CNPJ Emitente" AS emitente, -- cpf ou cnpj emissor da nota fiscal. fornecedor.
  "RAZÃO SOCIAL EMITENTE" AS nome_emitente, -- razão social do emissor da nota fiscal
  "INSCRIÇÃO ESTADUAL EMITENTE" AS inscricao_estadual_emitente, -- inscrição estadual do emissor da nota fiscal
  "UF EMITENTE"::uf AS uf_emitente, -- sigla unidade federativa do emissor da nota fiscal
  "MUNICÍPIO EMITENTE" AS municipio_emitente, -- nome município do emissor da nota fiscal. 
  "CNPJ DESTINATÁRIO" AS destinatario, -- cnpj do destinatário da nota fiscal. cliente.
  "NOME DESTINATÁRIO" AS nome_destinatario, -- nome do destinatário da nota fiscal
  "UF DESTINATÁRIO"::uf AS uf_destinatario, -- sigla da unidade federativa do destinatário da nota fiscal
  "INDICADOR IE DESTINATÁRIO"::indicador_ie AS indicador_ie_destinatario, -- indicador de inscrição estadual do destinatário da nota fiscal
  "DESTINO DA OPERAÇÃO"::tipo_destino AS destino_operacao, -- destino da operação da nota fiscal (interna, interestadual, exterior)
  "CONSUMIDOR FINAL"::tipo_consumidor AS indicador_consumidor_final, -- indica se o destinatário é consumidor final (0 - normal, 1 - consumidor final)
  "PRESENÇA DO COMPRADOR"::tipo_presenca AS indicador_presenca_comprador, -- indica se o comprador estava presente na operação (0 - não se aplica, 1 - presencial, 2 - não presencial, 3 - teleatendimento, 5 - não informado, 9 - outros)  
  REPLACE("VALOR NOTA FISCAL", ',', '.')::decimal(18,2) AS valor_nota_fiscal -- valor total da nota fiscal
FROM raw.nfe__notas order by timestamp_emissao; 


/*  POST-STATEMENT */
 @IF(
   @runtime_stage = 'evaluating',
    COPY cgu.notas
     to 'data/outputs/cgu/nfe/notas.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
 );