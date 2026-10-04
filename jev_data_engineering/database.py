import os
import duckdb

from pathlib import Path

# -----------------------
# CONFIG
# -----------------------
DB_PATH = Path('./data/expenses.duckdb').resolve()

# -----------------------
# CONNECTION
# -----------------------
def get_db_connection() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)
