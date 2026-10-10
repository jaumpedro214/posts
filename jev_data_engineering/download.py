import requests
import os
import logging
import argparse
from typing import Dict, Any, List

# -----------------------
# CONFIG
# -----------------------
BASE_PATH = "./data/files"

anos = [2026]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------
# FILE LIST
# -----------------------
def build_file_list() -> List[Dict[str, str]]:
    files = []

    for ano in anos:
        files.append({
            "file": f"prestacao_contas/{ano}/prestacao_de_contas_eleitorais_candidatos_{ano}.zip",
            "url": f"https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}.zip"
        })

    return files


# -----------------------
# DOWNLOAD
# -----------------------
def download_file(url: str, dest_path: str):
    tmp_path = dest_path + ".part"

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()

        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    os.rename(tmp_path, dest_path)


def run():
    os.makedirs(BASE_PATH, exist_ok=True)

    files = build_file_list()
    logger.info(f"Files to download: {len(files)}")

    for item in files:
        file = item["file"]
        url = item["url"]
        dest_path = os.path.join(BASE_PATH, file)

        if os.path.exists(dest_path):
            logger.info(f"[SKIP] Already exists: {file}")
            continue

        logger.info(f"[START] {file}")

        try:
            download_file(url, dest_path)
            logger.info(f"[OK] {file}")
        except Exception as e:
            logger.error(f"[ERROR] {file} -> {e}")


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloader de prestação de contas do TSE")

    parser.parse_args()

    run()
