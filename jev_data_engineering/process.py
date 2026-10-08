import logging

from database import get_db_connection, run_sql_script

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# CONFIG
# -----------------------
SCRIPTS = [
    "01_CREATE_TABLE_paid_expense.sql",
    "02_CREATE_TABLE_expense_description.sql",
    "03_CREATE_TABLE_expenses_classified_by_jev.sql",
    "04_CREATE_TABLE_expense_classified_by_rules.sql",

    "04_CREATE_TABLE_expenses_to_classify_with_jev.sql",
    "05_CREATE_VIEW_expense_classified_by_jev_accepted.sql",

    "06_CREATE_TABLE_paid_expense_classified.sql",
    "07_CREATE_TABLE_expenses_report.sql"
]


# -----------------------
# SQL
# -----------------------
def run(sql_files: list[str]):
    con = get_db_connection()

    try:
        for sql_file in sql_files:
            logger.info(f"[START] {sql_file}")
            try:
                run_sql_script(con, sql_file)
                logger.info(f"[OK] {sql_file}")
            except Exception as e:
                logger.error(f"[ERROR] {sql_file} -> {e}")
                raise SystemExit(1)
    finally:
        con.close()


if __name__ == "__main__":
    run(SCRIPTS)
