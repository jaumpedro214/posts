import os
import zipfile
import logging
import argparse
from typing import Optional

# -----------------------
# CONFIG
# -----------------------
SOURCE_PATH = "./data/files"
DEST_PATH = "./data/extracted"
SOURCE_ENCODING = "latin-1"
DEST_ENCODING = "utf-8"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------
# FILE LIST
# -----------------------
def list_zip_files() -> list[str]:
    zip_files = []

    for root, _, files in os.walk(SOURCE_PATH):
        for name in files:
            if name.lower().endswith(".zip"):
                zip_files.append(os.path.join(root, name))

    return sorted(zip_files)


# -----------------------
# EXTRACT
# -----------------------
def extract_file(zip_path: str, dest_path: str):
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_path)


# -----------------------
# ENCODING
# -----------------------
def convert_encoding(file_path: str, logger: Optional[logging.Logger] = None):
    with open(file_path, "r", encoding=SOURCE_ENCODING) as f:
        content = f.read()
    with open(file_path, "w", encoding=DEST_ENCODING) as f:
        f.write(content)


def convert_dir_encoding(directory: str, logger: Optional[logging.Logger] = None):
    for root, _, files in os.walk(directory):
        for name in files:
            if not name.lower().endswith(".csv"):
                continue

            file_path = os.path.join(root, name)
            try:
                convert_encoding(file_path)
            except UnicodeDecodeError as e:
                if logger:
                    logger.warning(f"[SKIP] Encoding: {file_path} -> {e}")


def run():
    if not os.path.exists(SOURCE_PATH):
        logger.error(f"Source path does not exist: {SOURCE_PATH}")
        return

    os.makedirs(DEST_PATH, exist_ok=True)

    zip_files = list_zip_files()
    logger.info(f"Zip files found: {len(zip_files)}")

    for zip_path in zip_files:
        relative = os.path.relpath(zip_path, SOURCE_PATH)
        dest_dir = os.path.join(DEST_PATH, os.path.splitext(relative)[0])

        if os.path.exists(dest_dir):
            logger.info(f"[SKIP] Already extracted: {relative}")
            continue

        logger.info(f"[START] {relative}")

        try:
            extract_file(zip_path, dest_dir)
            logger.info(f"[START] Encoding: {relative}")
            convert_dir_encoding(dest_dir, logger)
            logger.info(f"[OK] {relative}")
        except Exception as e:
            logger.error(f"[ERROR] {relative} -> {e}")


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extractor de arquivos do TSE")

    parser.parse_args()

    run()
