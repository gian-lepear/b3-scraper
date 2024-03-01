import zipfile
from pathlib import Path

import httpx
import pandas as pd


def download_file(month: str) -> Path:
    headers = {"User-Agent": "Chrome/120 Linux x86_64 AppleWebKit/537.36"}
    url = f"https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M{month}.ZIP"
    filename = url.split("/")[-1].upper()

    response = httpx.get(url, headers=headers)
    response.raise_for_status()

    file_path = Path("files/stocks/zip")
    file_path.mkdir(parents=True, exist_ok=True)

    file_path /= filename
    file_path.write_bytes(response.content)

    return file_path


def unzip_file(zip_path: Path) -> None:
    extract_path = Path("files/stocks/extracted")
    extract_path.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(path=extract_path)


if __name__ == "__main__":
    month_range = pd.date_range("2024-02-01", "2024-02-29", freq="MS").strftime("%m%Y").tolist()
    for month in month_range:
        zip_path = download_file(month)
        unzip_file(zip_path)
