import asyncio
import aiohttp
from aiohttp import ClientSession
import time

async def download_file(session: ClientSession, url: str, retries=3):
    filename = url.split('/')[-1]
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                with open(filename, 'wb') as f:
                    async for chunk in response.content.iter_chunked(1024*1024):  # 1MB chunks
                        f.write(chunk)
                print(f"Successfully downloaded {filename}")
                return
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"Attempt {attempt + 1} failed for {filename}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"Failed to download {filename} after {retries} attempts. Error: {str(e)}")

async def main():
    urls = [
        'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-02/Empresas0.zip',
        # ... include all your URLs here ...
        'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-02/Empresas9.zip'
    ]
    
    connector = aiohttp.TCPConnector(limit=5)  # Limit concurrent connections
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [download_file(session, url) for url in urls]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    print(f"Total time: {time.time() - start:.2f} seconds")