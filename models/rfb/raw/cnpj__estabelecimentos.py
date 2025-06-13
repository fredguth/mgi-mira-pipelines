from datetime import datetime
import duckdb as db
from pathlib import Path
from lib.utils import download_unzip_convert, write_completion_marker, get_zip_urls  
import pandas as pd
import typing as t
from sqlmesh import model, ExecutionContext
import aiohttp
import asyncio


@model(
    "raw.cnpj__estabelecimentos",
    columns={
        "cnpj_basico": "varchar",
        "cnpj_ordem": "varchar",
        "cnpj_dv": "varchar",
        "identificador_matriz_filial": "varchar",
        "nome_fantasia": "varchar",
        "situacao_cadastral": "varchar",
        "data_situacao_cadastral": "varchar",
        "codigo_motivo": "varchar",
        "nome_cidade_exterior": "varchar",
        "id_pais": "varchar",
        "data_inicio_atividade": "varchar",
        "cnae_fiscal_principal": "varchar",
        "cnae_fiscal_secundaria": "varchar",
        "tipo_logradouro": "varchar",
        "logradouro": "varchar",
        "numero": "varchar",
        "complemento": "varchar",
        "bairro": "varchar",
        "cep": "varchar",
        "sigla_uf": "varchar",
        "id_municipio_rf": "varchar",
        "ddd_1": "varchar",
        "telefone_1": "varchar",
        "ddd_2": "varchar",
        "telefone_2": "varchar",
        "ddd_fax": "varchar",
        "fax": "varchar",
        "email": "varchar",
        "situacao_especial": "varchar",
        "data_situacao_especial": "varchar",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    
    # year_month = execution_time.strftime("%Y-%m")
    year_month = '2025-02'
    base_dir = Path("data/inputs/rfb_cnpj")
    download_path = base_dir / year_month
        
    if not download_path.exists() or not (download_path / "Estabelecimentos.md").exists():
        zip_urls = get_zip_urls(year_month, 'Estabelecimentos')
        asyncio.run(download_unzip_convert(download_path, 'Estabelecimentos', zip_urls))
        write_completion_marker(download_path, 'Estabelecimentos', zip_urls)
        
    
    
    if not (base_dir / year_month / "estabelecimentos/cnpj_dv=01/data_0.parquet").exists():
        conn = db.connect()
        query = f"""
            COPY
            (SELECT
                *
            FROM read_csv('{download_path}/*.ESTABELE', 
                strict_mode=FALSE,  
                quote='"',
                escape='"',
                encoding='utf-8',
                header=FALSE,
                sep=';',
                parallel=FALSE,
                columns={{
                    'cnpj_basico': 'varchar',
                    'cnpj_ordem': 'varchar',
                    'cnpj_dv': 'varchar',
                    'identificador_matriz_filial': 'varchar',
                    'nome_fantasia': 'varchar',
                    'situacao_cadastral': 'varchar',
                    'data_situacao_cadastral': 'varchar',
                    'codigo_motivo': 'varchar',
                    'nome_cidade_exterior': 'varchar',
                    'id_pais': 'varchar',
                    'data_inicio_atividade': 'varchar',
                    'cnae_fiscal_principal': 'varchar',
                    'cnae_fiscal_secundaria': 'varchar',
                    'tipo_logradouro': 'varchar',
                    'logradouro': 'varchar',
                    'numero': 'varchar',
                    'complemento': 'varchar',
                    'bairro': 'varchar',
                    'cep': 'varchar',
                    'sigla_uf': 'varchar',
                    'id_municipio_rf': 'varchar',
                    'ddd_1': 'varchar',
                    'telefone_1': 'varchar',
                    'ddd_2': 'varchar',
                    'telefone_2': 'varchar',
                    'ddd_fax': 'varchar',
                    'fax': 'varchar',
                    'email': 'varchar',
                    'situacao_especial': 'varchar',
                    'data_situacao_especial': 'varchar'
                }}
            )) TO '{download_path}/estabelecimentos' (FORMAT PARQUET, PARTITION_BY (cnpj_dv) )
        """
        conn.execute(query)
        conn.close()
    for cnpj_dv in range(0, 100):
        query = f"SELECT * FROM read_parquet('{download_path}/estabelecimentos/cnpj_dv={cnpj_dv:02}/*.parquet')"
        yield context.fetchdf(query)
    
    