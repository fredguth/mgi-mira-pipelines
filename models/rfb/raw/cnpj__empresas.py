from datetime import datetime
import duckdb as db
from pathlib import Path
from lib.utils import download_unzip_convert, write_completion_marker, get_zip_urls  
import pandas as pd
import typing as t  # Added missing import
from sqlmesh import model, ExecutionContext  # Added missing import
import aiohttp
import asyncio


@model(
    "raw.cnpj__empresas",
    columns={
        "cnpj_basico": "bigint",
        "razao_social": "varchar",
        "natureza_juridica": "bigint",
        "qualificacao_responsavel": "varchar",
        "capital_social": "varchar",
        "porte": "varchar",
        "ente_federativo": "varchar",
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
    out_dir = Path("data/outputs/rfb_cnpj")
    download_path = base_dir / year_month
        
    if not download_path.exists() or not (download_path / "Empresas.md").exists():
        zip_urls = get_zip_urls(year_month, 'Empresas')
        asyncio.run(download_unzip_convert(download_path, 'Empresas',zip_urls))
        write_completion_marker(download_path, 'Empresas', zip_urls)
    
    
    
    if not (base_dir / year_month / "empresas.parquet").exists():
        conn = db.connect()
        query = f"""
            COPY 
            (SELECT
                *
            FROM read_csv('{download_path}/*.EMPRECSV', 
                strict_mode=FALSE,  
                quote='"',
                escape='"',
                encoding='utf-8',
                header=FALSE,
                sep=';',
                columns={{
                    "cnpj_basico": "varchar",
                    "razao_social": "varchar",
                    "natureza_juridica": "varchar", 
                    "qualificacao_responsavel": "varchar",
                    "capital_social": "varchar",
                    "porte": "varchar",
                    "ente_federativo": "varchar"
                }}
            )) TO '{download_path}/empresas.parquet' (FORMAT PARQUET)
        """
        conn.execute(query)
        conn.close()
    query = f"SELECT * FROM read_parquet('{download_path}/empresas.parquet')"
    return context.fetchdf(query)
    