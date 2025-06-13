MODEL (
    name mgi.itens_nota
);

WITH chaves_validas as (
    select 
        chave_acesso
    from cgu.notas 
    where ultimo_evento not in (
        'Cancelamento da NF-e', 
        'Manifestação do destinatário - Operação não realizada', 
        'Manifestação do destinatário - Desconhecimento da operação')
) 
select 
  "CHAVE DE ACESSO" AS chave_acesso, -- identificador da nota fiscal em que este item foi registrado.
  "SÉRIE"::USMALLINT AS serie, -- série da nota fiscal. juntamente com o número identifica unicamente a nota fiscal
  "NÚMERO"::BIGINT AS numero, -- número da nota fiscal. juntamente com a série identifica unicamente a nota fiscal
  "NATUREZA DA OPERAÇÃO" AS natureza_operacao, -- descrição da natureza da operação. campo de preenchimento livre.
  strptime("DATA EMISSÃO", '%d/%m/%Y %H:%M:%S') AS timestamp_emissao, -- data e hora de emissão da nota fiscal
--   timestamp_emissao::date AS data_emissao, -- data de emissão da nota fiscal
--   month(data_emissao)::UTINYINT AS mes_emissao, -- mês de emissão da nota fiscal
  "CPF/CNPJ Emitente" AS emitente, -- cpf ou cnpj emissor da nota fiscal. fornecedor.
  "RAZÃO SOCIAL EMITENTE" AS nome_emitente, -- razão social do emissor da nota fiscal
-- "INSCRIÇÃO ESTADUAL EMITENTE" AS inscricao_estadual_emitente, -- inscrição estadual do emissor da nota fiscal
  "UF EMITENTE"::uf AS uf_emitente, -- sigla unidade federativa do emissor da nota fiscal
-- "MUNICÍPIO EMITENTE" AS municipio_emitente, -- nome município do emissor da nota fiscal. 
  "CNPJ DESTINATÁRIO" AS destinatario, -- cnpj do destinatário da nota fiscal. cliente.
  "NOME DESTINATÁRIO" AS nome_destinatario, -- nome do destinatário da nota fiscal
  "UF DESTINATÁRIO"::uf AS uf_destinatario, -- sigla da unidade federativa do destinatário da nota fiscal
--   "INDICADOR IE DESTINATÁRIO"::indicador_ie AS indicador_ie_destinatario, -- indicador de inscrição estadual do destinatário da nota fiscal
  "DESTINO DA OPERAÇÃO"::tipo_destino AS destino_operacao, -- destino da operação da nota fiscal (interna, interestadual, exterior)
  "CONSUMIDOR FINAL"::tipo_consumidor AS indicador_consumidor_final, -- indica se o destinatário é consumidor final (0 - normal, 1 - consumidor final)
  "PRESENÇA DO COMPRADOR"::tipo_presenca AS indicador_presenca_comprador, -- indica se o comprador estava presente na operação (0 - não se aplica, 1 - presencial, 2 - não presencial, 3 - teleatendimento, 5 - não informado, 9 - outros)  
  "NÚMERO PRODUTO"::USMALLINT AS numero_produto, -- número sequencial do item na nota fiscal. juntamente com a chave de acesso, forma a chave primária
  "DESCRIÇÃO DO PRODUTO/SERVIÇO" AS descricao_produto, -- descrição do produto ou serviço. campo de preenchimento livre.
  "CÓDIGO NCM/SH" AS ncm, -- código da nomenclatura comum do mercosul (NCM) do item (-1 é usado para serviços)
  "NCM/SH (TIPO DE PRODUTO)" AS descricao_ncm, -- descrição do NCM do item
  nfe__itens."CFOP"::USMALLINT AS cfop, -- código fiscal de operações e prestações. identifica a natureza fiscal da operação
  c.cfop_descricao as descricao_cfop, -- descrição do CFOP,
  c.cfop_categoria as categoria_cfop, -- categoria do CFOP
  REPLACE("QUANTIDADE", ',', '.')::double AS quantidade, -- quantidade do item
  "UNIDADE" AS unidade, -- unidades de medida do item
  REPLACE("VALOR UNITÁRIO", ',', '.')::double AS valor_unitario, -- valor unitário do item
  REPLACE("VALOR TOTAL", ',', '.')::double AS valor_total_item, -- valor total do item (quantidade * valor unitário)
  year(timestamp_emissao::date)::USMALLINT AS ano_emissao -- ano de emissão da nota fiscal
FROM raw.nfe__itens join raw.cfop c on raw.nfe__itens."CFOP" = c.CFOP 
where chave_acesso in (select chave_acesso from chaves_validas)
order by timestamp_emissao;

/*  POST-STATEMENT */
@IF(
  @runtime_stage = 'evaluating',
  COPY mgi.itens_nota TO 'data/outputs/mgi/notas_fiscais' (FORMAT PARQUET, PARTITION_BY (ano_emissao), OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'itens_{i}'
  )
);