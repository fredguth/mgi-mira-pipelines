# utils.py
import aiohttp
import asyncio
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import subprocess
import chardet
import random
import hashlib
from rich.progress import Progress, TaskID, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.console import Console
import nest_asyncio

nest_asyncio.apply()

# Progress Setup
def get_console() -> Console:
    return Console()

def create_progress(**kwargs) -> Progress:
    return Progress(
        TextColumn("[bold blue]{task.description}[/bold blue]"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
        console=get_console(),
        expand=True,
        **kwargs
    )

def get_zip_urls(year_month: str, name:str) -> list[str]:
    """Generate URLs for CNPJ zip files for a given year-month."""
    base_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_month}/"
    return [f"{base_url}{name}{i}.zip" for i in range(10)]

# Download Functions
async def download_file(
    session,
    url,
    progress,
    task_id,
    path,
    max_retries=5,
    initial_delay=2,
    chunk_size=4*1024*1024  # 4MB chunks
):
    temp_path = path.with_suffix('.tmp')
    attempt = 0
    last_error = None
    
    while attempt < max_retries:
        try:
            # Cleanup previous attempts
            if temp_path.exists():
                temp_path.unlink()

            async with session.get(url) as response:
                response.raise_for_status()
                content_length = int(response.headers.get('content-length', 0))
                
                # Update progress with actual size if available
                progress.update(
                    task_id,
                    total=content_length if content_length > 0 else None,
                    description=f"Downloading {path.name} (Attempt {attempt+1})"
                )

                downloaded = 0
                with open(temp_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress.update(task_id, completed=downloaded)

                # Validate size if content-length was provided
                if content_length > 0 and temp_path.stat().st_size != content_length:
                    raise aiohttp.ClientPayloadError(
                        f"Size mismatch: Expected {content_length}, got {temp_path.stat().st_size}"
                    )

                # Atomic rename on success
                temp_path.rename(path)
                return True

        except (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError,
                ConnectionResetError, aiohttp.ClientOSError, asyncio.TimeoutError) as e:
            last_error = e
            if temp_path.exists():
                temp_path.unlink()
            
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt) + random.uniform(0, 2)
                progress.console.log(f"Retrying {path.name} in {delay:.1f}s: {str(e)}")
                await asyncio.sleep(delay)
            
            attempt += 1

    # All retries failed
    progress.console.log(f"[red]Failed after {max_retries} attempts for {path.name}[/red]")
    raise last_error

# Extraction Functions
async def unzip_worker_sync(zip_path: Path, extract_path: Path, progress: Progress) -> List[str]:
    task_id = progress.add_task(f"Extracting {zip_path.name}", total=100)
    extracted_files = []

    try:
        def sync_unzip():
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.file_size > 1 * 1024**3:  # 1GB threshold
                        # Stream large files
                        with zip_ref.open(file_info) as source, \
                             open(extract_path / file_info.filename, 'wb') as target:
                            while True:
                                chunk = source.read(4 * 1024**2)  # 4MB chunks
                                if not chunk:
                                    break
                                target.write(chunk)
                    else:
                        with zip_ref.open(file_info) as source, \
                             open(extract_path / file_info.filename, 'wb') as target:
                            target.write(source.read())
                    extracted_files.append(file_info.filename)

        await asyncio.to_thread(sync_unzip)
        progress.update(task_id, completed=100)
        return extracted_files

    except Exception as e:
        progress.console.print(f"[red]Extraction failed for {zip_path}: {str(e)}[/red]")
        # Cleanup partial extracts
        for f in extracted_files:
            (extract_path / f).unlink(missing_ok=True)
        raise

# Conversion Functions
async def convert_worker_sync(file_path: Path, progress: Progress) -> None:
    task_id = progress.add_task(f"Converting {file_path.name}", total=100)
    temp_file = file_path.with_suffix('.tmp')
    backup_file = file_path.with_suffix('.bak')
    chunk_size = 4 * 1024 * 1024  # 4MB chunks

    try:
        # Create backup
        file_path.rename(backup_file)
        
        with open(backup_file, 'rb') as src, open(temp_file, 'w', encoding='utf-8') as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                try:
                    decoded = chunk.decode('iso-8859-1')
                except UnicodeDecodeError:
                    # Fallback to chardet for problematic chunks
                    encoding = chardet.detect(chunk)['encoding'] or 'iso-8859-1'
                    decoded = chunk.decode(encoding, errors='replace')
                dst.write(decoded)
                progress.update(task_id, advance=len(chunk))

        # Atomic replacement
        temp_file.replace(file_path)
        backup_file.unlink()

        # Verify conversion
        with open(file_path, 'rb') as f:
            sample = f.read(1024)
            detected = chardet.detect(sample)
            if detected['encoding'] not in ['utf-8', 'ascii']:
                raise ValueError(f"Unexpected encoding after conversion: {detected['encoding']}")

        progress.update(task_id, completed=100)

    except Exception as e:
        # Cleanup on failure
        if temp_file.exists():
            temp_file.unlink()
        if backup_file.exists():
            backup_file.replace(file_path)
        progress.console.print(f"[red]Conversion failed for {file_path}: {str(e)}[/red]")
        raise

# User Agents and Session Management
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
]

def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def download_unzip_convert(
    download_path: Path,
    csvname: str,
    zip_urls: List[str],
    workers: int = 4  # Reduced from 8 for stability
) -> None:
    download_path.mkdir(parents=True, exist_ok=True)
    
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }
    
    # Enhanced connection settings
    connector = aiohttp.TCPConnector(
        limit_per_host=2,
        enable_cleanup_closed=True,
        force_close=True,
        ssl=False
    )
    
    timeout = aiohttp.ClientTimeout(
        total=30 * 60,  # 30 minutes
        sock_connect=30,
        sock_read=300
    )

    async with aiohttp.ClientSession(
        headers=headers,
        connector=connector,
        timeout=timeout
    ) as session:
        with create_progress() as progress:
            tasks = []
            for url in zip_urls:
                zip_file = download_path / url.split('/')[-1]
                tasks.append(process_single_zip(session, progress, url, zip_file, download_path))

            # Process with progressive backoff
            delay_factors = [0.5, 1.0, 2.0, 4.0, 8.0]
            for idx, task in enumerate(asyncio.as_completed(tasks)):
                await task
                delay = random.uniform(0.5, 1.5) * delay_factors[min(idx, len(delay_factors)-1)]
                await asyncio.sleep(delay)

async def process_single_zip(
    session: aiohttp.ClientSession,
    progress: Progress,
    url: str,
    zip_file: Path,
    download_path: Path
) -> None:
    try:
        # 1. Download
        download_task = progress.add_task(f"Downloading {zip_file.name}", total=100)
        await download_file(session, url, progress, download_task, zip_file)

        # 2. Validate before extraction
        if not zip_file.is_file():
            raise FileNotFoundError(f"Downloaded file missing: {zip_file}")

        # 3. Unzip
        extracted_files = await unzip_worker_sync(zip_file, download_path, progress)

        # 4. Convert files with parallel processing
        convert_tasks = []
        for extracted in extracted_files:
            file_path = download_path / extracted
            if file_path.stat().st_size > 0:
                convert_tasks.append(convert_worker_sync(file_path, progress))
        
        await asyncio.gather(*convert_tasks)

        # 5. Cleanup
        zip_file.unlink()

    except Exception as e:
        progress.console.print(f"[red]Failed processing {zip_file.name}: {str(e)}[/red]")
        # Cleanup partial files
        if zip_file.exists():
            zip_file.unlink()
        raise

def write_completion_marker(download_path: Path, csvname: str, zip_urls: List[str]) -> None:
    marker = download_path / f"{csvname}.md"
    content = (
        f"Completed: {datetime.now().isoformat()}\n"
        f"Processed {len(zip_urls)} files\n"
        f"Encoding conversion: utf-8\n"
        f"Validated: true"
    )
    marker.write_text(content)