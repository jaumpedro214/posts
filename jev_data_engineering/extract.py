import os
import zipfile
import logging
import argparse

# -----------------------
# CONFIG
# -----------------------
SOURCE_PATH = "./data/files"
DEST_PATH = "./data/extracted"

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
