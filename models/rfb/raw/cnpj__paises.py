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
    "raw.cnpj__paises",
    columns={
        "id_pais": "varchar",
        "pais": "varchar"
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
        
    if not download_path.exists() or not (download_path / "paises.md").exists():
        zip_urls = [f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_month}/Paises.zip"]
        asyncio.run(download_unzip_convert(download_path, 'paises',zip_urls))
        write_completion_marker(download_path, 'paises', zip_urls)
    
    
    
    if not (base_dir / year_month / "paises.parquet").exists():
        conn = db.connect()
        query = f"""
            COPY 
            (SELECT
                *
            FROM read_csv('{download_path}/*.PAISCSV', 
                strict_mode=FALSE,  
                quote='"',
                escape='"',
                encoding='utf-8',
                header=FALSE,
                sep=';',
                columns={{
                    "id_pais": "varchar",
                    "pais": "varchar"
                }}
            )) TO '{download_path}/paises.parquet' (FORMAT PARQUET)
        """
        conn.execute(query)
        conn.close()
    query = f"SELECT * FROM read_parquet('{download_path}/paises.parquet')"
    return context.fetchdf(query)
    