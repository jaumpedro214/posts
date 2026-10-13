import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from database import get_db_connection, DATABASE_NAME

TABLE = "expenses_to_classify_with_jev"
SAMPLE_SIZE = 1000
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample.json")


def sample_records_from_table(
    table: str,
    records: int,
    output_path: str = OUTPUT_PATH,
) -> int:
    con = get_db_connection()

    try:
        con.execute(
            f"""
            COPY (
                SELECT DS_DESPESA AS text
                FROM {DATABASE_NAME}.{table}
                USING SAMPLE {records} ROWS (reservoir)
            )
            TO '{output_path}' (FORMAT json, ARRAY true)
            """
        )

        total = con.execute(f"SELECT count(*) FROM '{output_path}'").fetchone()[0]

        return total
    finally:
        con.close()


def main():
    total = sample_records_from_table(TABLE, SAMPLE_SIZE)
    print(f"Sampled {total} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
