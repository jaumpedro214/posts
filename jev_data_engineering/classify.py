import httpx
import os
import asyncio
import logging
from dataclasses import dataclass
from dotenv import load_dotenv
from database import get_db_connection, ALIAS
import duckdb

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def run_sql_file(db: duckdb.DuckDBPyConnection, filename: str):
    with open(os.path.join(SQL_PATH, filename)) as f:
        db.execute(f.read())


@dataclass
class JevClassificationResult:
    description: str
    tag: str
    confidence: str

async def classify_expense_with_jev(
    description: str,
    client: httpx.AsyncClient,
) -> JevClassificationResult:

    api_key = os.environ.get("TYPESAFE_API_KEY")

    if not api_key:
        raise ValueError('Error importing ENV Variables, TYPESAFE_API_KEY not found.')

    logger.debug(f"[REQUEST] {description}")

    base_url = "https://api.typesafe.ai/v1/systemone"
    choices = {
        "Transport": "Transport-related expenses such as gasoline and car/bus rental",
        "Tariffs and taxes": "Tariffs and taxes related to the purchase of products or services such as water, gas and electricity",
        "Online Advertising": "Online advertising campaigns, social-media boosting and other services",
        "Printed Material": "Newspapers, folders, banners, flags, stickers, 'santinhos', 'cola de votação', shirts, caps, among others",
        "Hiring": "Expenses related to hiring staff such as drivers, secretaries, accountants, directors, cleaners, etc.",
        "Food": "Food and drinks",
        "Physical Space": "Physical space rental or allocation",
        "Rallies and Actions": "Public actions, rallies and electoral parades. Hiring of 'cabo eleitoral'",
        "Consumer Electronics": "Computers, microwaves, air conditioning, TVs, monitors, freezers",
        "Others": "None of the above categories",
    }

    payload = {
        "model": 'jev-latest',
        "state": description,
        "questions": {
            "tag": {
                "type": "choice",
                "instructions": "Which is the most adequate class to this expense description?",
                "criteria": choices
            }
        }
    }

    response = await client.post(
        base_url,
        json=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    response.raise_for_status()
    data = response.json()

    result = JevClassificationResult(
        description=description,
        tag=data['answers']['tag']['choice'],
        confidence=data['answers']['tag']['confidence']
    )

    logger.debug(f"[RESPONSE] {description} -> {result.tag} ({result.confidence})")

    return result


def fetch_unclassified_records(db: duckdb.DuckDBPyConnection, batch_size: int = 10_000):
    # The pending descriptions are materialized into the
    # `descriptions_to_classify` table by the SQL script. We paginate with
    # LIMIT/OFFSET (re-running the query for each batch) instead of keeping a
    # cursor open, because an active scan blocks the DuckLake commits of the
    # inserts and raises "database is locked".
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


async def classify(batch_size: int = 500, max_concurrency: int = 50):
    db = get_db_connection()
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

        try:
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

                insert_classified_records(db, results)

                total += len(results)
                logger.info(f"[BATCH {batch}] Inserted {len(results)} rows (total: {total})")

            logger.info(f"Done. Total classified: {total}")
        except Exception as e:
            logger.error(f"[ERROR] {e}")
            raise
        finally:
            db.close()


if __name__ == "__main__":
    asyncio.run(classify(max_concurrency=10))