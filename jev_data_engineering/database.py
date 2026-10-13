import os
import duckdb

from pathlib import Path

# -----------------------
# CONFIG
# -----------------------
DATA_PATH     = Path("./data/duckdb")
DATABASE_PATH = Path("./data/duckdb/database.duckdb")
METADATA_PATH = Path("./data/duckdb/metadata.sqlite")
DATABASE_NAME = "expenses"

# -----------------------
# CONNECTION
# -----------------------
def get_db_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DATABASE_PATH)

    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL sqlite")
    con.execute("LOAD sqlite")

    os.makedirs(DATA_PATH, exist_ok=True)

    con.execute(
        f"""
        ATTACH 'ducklake:sqlite:{METADATA_PATH}' AS {DATABASE_NAME}
            (DATA_PATH '{DATA_PATH}')
        """
    )

    con.execute(f"USE {DATABASE_NAME}")

    return con


# -----------------------
# SQL FILES
# -----------------------
SQL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def run_sql_script(con: duckdb.DuckDBPyConnection, sql_file: str) -> None:
    """Execute a SQL script from disk.

    `sql_file` may be an absolute path or a name relative to the sql/ directory.
    """
    if not os.path.isabs(sql_file):
        sql_file = os.path.join(SQL_PATH, sql_file)

    with open(sql_file, "r", encoding="utf-8") as f:
        con.execute(f.read())
