import os
import logging
import argparse
import duckdb

from database import get_db_connection

# -----------------------
# CONFIG
# -----------------------
SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------
# SQL
# -----------------------
def execute_sql_file(con: duckdb.DuckDBPyConnection, sql_file: str):
    with open(sql_file, "r") as f:
        sql = f.read()

    con.execute(sql)


def run(sql_files: list[str]):
    con = get_db_connection()

    try:
        for sql_file in sql_files:
            if not os.path.isabs(sql_file):
                sql_file = os.path.join(SQL_PATH, sql_file)

            name = os.path.relpath(sql_file, SQL_PATH)
            logger.info(f"[START] {name}")

            try:
                execute_sql_file(con, sql_file)
                logger.info(f"[OK] {name}")
            except Exception as e:
                logger.error(f"[ERROR] {name} -> {e}")
    finally:
        con.close()


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executor de scripts SQL no DuckDB")

    parser.add_argument("sql_files", nargs="*", help="Arquivos SQL a executar (relativos a sql/)")

    args = parser.parse_args()

    if args.sql_files:
        run(args.sql_files)
    else:
        run(sorted(
            os.path.join(SQL_PATH, name)
            for name in os.listdir(SQL_PATH)
            if name.endswith(".sql")
        ))
