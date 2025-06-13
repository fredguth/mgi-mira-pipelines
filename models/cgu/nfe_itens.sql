-- itens das notas fiscais eletrônicas. informações detalhadas dos produtos e serviços.
MODEL (
  name cgu.itens_nota,
  depends_on (cgu.notas)
);
SELECT DISTINCT
  "CHAVE DE ACESSO" AS chave_acesso, -- identificador da nota fiscal em que este item foi registrado. juntamente com número do item, forma a chave primária
  "NÚMERO PRODUTO"::USMALLINT AS numero_produto, -- número sequencial do item na nota fiscal. juntamente com a chave de acesso, forma a chave primária
  "DESCRIÇÃO DO PRODUTO/SERVIÇO" AS descricao_produto, -- descrição do produto ou serviço. campo de preenchimento livre.
  "CÓDIGO NCM/SH" AS ncm, -- código da nomenclatura comum do mercosul (NCM) do item (-1 é usado para serviços)
  "NCM/SH (TIPO DE PRODUTO)" AS descricao_ncm, -- descrição do NCM do item
  nfe__itens."CFOP"::USMALLINT AS cfop, -- código fiscal de operações e prestações. identifica a natureza fiscal da operação
  c.cfop_descricao as descricao_cfop, -- descrição do CFOP
  REPLACE("QUANTIDADE", ',', '.')::decimal(18,2) AS quantidade, -- quantidade do item
  "UNIDADE" AS unidade, -- unidades de medida do item
  REPLACE("VALOR UNITÁRIO", ',', '.')::decimal(18,2) AS valor_unitario, -- valor unitário do item
  REPLACE("VALOR TOTAL", ',', '.')::decimal(18,2) AS valor_total_item -- valor total do item (quantidade * valor unitário)
FROM raw.nfe__itens join raw.cfop c on raw.nfe__itens."CFOP" = c.CFOP order by raw.nfe__itens."DATA EMISSÃO";

   

/*  POST-STATEMENT */
 @IF(
   @runtime_stage = 'evaluating',
    COPY cgu.itens_nota to 'data/outputs/cgu/nfe/itens.parquet' (FORMAT PARQUET,  OVERWRITE_OR_IGNORE)
 );