import httpx
import os
import asyncio
import logging
import time
from database import get_db_connection, ALIAS
from classification.jev import JevClassificationResult, classify_expense_with_jev
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def run_sql_file(db: duckdb.DuckDBPyConnection, filename: str):
    with open(os.path.join(SQL_PATH, filename)) as f:
        db.execute(f.read())


def fetch_unclassified_records(db: duckdb.DuckDBPyConnection, batch_size: int = 10_000):
    offset = 0

    while True:
        rows = db.execute(
            f"SELECT DS_DESPESA FROM {ALIAS}.descriptions_to_classify "
            f"LIMIT {batch_size} OFFSET {offset}"
        ).fetchall()

        if not rows:
            break

        yield [row[0] for row in rows]

        offset += batch_size


def insert_classified_records(
    db: duckdb.DuckDBPyConnection,
    records: list[JevClassificationResult],
):
    db.executemany(
        "INSERT INTO jev_predicted_tag (description, tag, confidence) VALUES (?, ?, ?)",
        [(r.description, r.tag, r.confidence) for r in records]
    )


async def classify_descriptions_in_batch(db: duckdb.DuckDBPyConnection, batch_size: int = 500, max_concurrency: int = 50):
    semaphore = asyncio.Semaphore(max_concurrency)

    limits = httpx.Limits(
        max_connections=max_concurrency,
        max_keepalive_connections=max_concurrency,
    )
    timeout = httpx.Timeout(60.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:

        async def classify_limited(description: str) -> JevClassificationResult:
            async with semaphore:
                return await classify_expense_with_jev(description, client)

        logger.info("Materializing descriptions to classify...")
        run_sql_file(db, "04_SELECT_descriptions_to_classify.sql")

        total = 0
        batch = 0

        for descriptions in fetch_unclassified_records(db, batch_size):

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


async def main(batch_size: int = 10_000, max_concurrency: int = 20):
    db = get_db_connection()

    try:
        await classify_descriptions_in_batch(db, batch_size=batch_size, max_concurrency=max_concurrency)
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())