import httpx
import os
import csv
import asyncio
import logging
import time
import tempfile
from database import get_db_connection, DATABASE_NAME, run_sql_script
from classification.jev import CHOICES, ClassificationResult, jev_classification
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

def fetch_unclassified_records(db: duckdb.DuckDBPyConnection, batch_size: int = 10_000, max_records: int = None):
    limit_clause = f"LIMIT {max_records}" if max_records is not None else ""
    offset = 0

    while True:
        rows = db.execute(
            f"WITH unclassified AS ("
            f"  SELECT DS_DESPESA FROM {DATABASE_NAME}.expenses_to_classify_with_jev {limit_clause}"
            f") "
            f"SELECT DS_DESPESA FROM unclassified LIMIT {batch_size} OFFSET {offset}"
        ).fetchall()

        if not rows:
            break

        yield [row[0] for row in rows]

        offset += batch_size


def dump_records_as_csv_temp_file(records: list[ClassificationResult]) -> str:
    fd, tmp_path = tempfile.mkstemp(suffix=".csv")

    with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["text", "classification", "confidence"])
        writer.writerows(
            (r.text, r.classification, r.confidence) for r in records
        )

    return tmp_path


def insert_classified_records(
    db: duckdb.DuckDBPyConnection,
    records: list[ClassificationResult],
):
    if not records:
        return

    tmp_path = dump_records_as_csv_temp_file(records)

    try:
        db.execute(
            f"""
            INSERT INTO {DATABASE_NAME}.expenses_classified_by_jev (description, classification, confidence)
            SELECT text, classification, confidence
            FROM read_csv('{tmp_path}', header=true, delim=',')
            """
        )
    finally:
        os.remove(tmp_path)


async def classify_descriptions_in_batch(
        db: duckdb.DuckDBPyConnection, 
        batch_size: int = 500, 
        max_concurrency: int = 50, 
        max_records: int = None
    ):
    semaphore = asyncio.Semaphore(max_concurrency)

    limits = httpx.Limits(
        max_connections=max_concurrency,
        max_keepalive_connections=max_concurrency,
    )
    timeout = httpx.Timeout(60.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:

        async def classify_limited(description: str) -> ClassificationResult:
            async with semaphore:
                return await jev_classification(description, client, CHOICES)

        logger.info("Materializing descriptions to classify...")
        run_sql_script(db, "04_SELECT_expenses_to_classify_with_jev.sql")

        total = 0
        batch = 0

        for descriptions in fetch_unclassified_records(db, batch_size, max_records):

            if batch > 0:
                logger.info("Sleeping 5s before next batch...")
                await asyncio.sleep(5)

            batch += 1

            logger.info(f"[BATCH {batch}] Classifying {len(descriptions)} descriptions...")

            results = await asyncio.gather(
                *(classify_limited(description) for description in descriptions)
            )

            logger.info(f"[BATCH {batch}] Inserting {len(results)} rows...")

            insert_start = time.perf_counter()
            insert_classified_records(db, results)
            insert_elapsed = time.perf_counter() - insert_start

            total += len(results)
            logger.info(
                f"[BATCH {batch}] Inserted {len(results)} rows in {insert_elapsed:.2f}s "
                f"(total: {total})"
            )

        logger.info(f"Done. Total classified: {total}")


async def main(batch_size: int = 10_000, max_concurrency: int = 20, max_records: int = None):
    db = get_db_connection()
    start = time.perf_counter()

    try:
        await classify_descriptions_in_batch(db, batch_size=batch_size, max_concurrency=max_concurrency, max_records=max_records)
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        raise
    finally:
        db.close()
        elapsed = time.perf_counter() - start
        minutes, seconds = divmod(elapsed, 60)
        logger.info(f"Total time: {int(minutes)}m {seconds:.2f}s ({elapsed:.2f}s)")


if __name__ == "__main__":
    asyncio.run(main(batch_size=5_000,  max_concurrency = 30, max_records=100_000))