import os
import duckdb

from pathlib import Path

# -----------------------
# CONFIG
# -----------------------
DATA_PATH     = Path("./data/duckdb")
DATABASE_PATH = Path("./data/duckdb/database.duckdb")
METADATA_PATH = Path("./data/duckdb/metadata.sqlite")
ALIAS = "expenses"

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
        ATTACH 'ducklake:sqlite:{METADATA_PATH}' AS {ALIAS}
            (DATA_PATH '{DATA_PATH}')
        """
    )

    con.execute(f"USE {ALIAS}")

    return con
