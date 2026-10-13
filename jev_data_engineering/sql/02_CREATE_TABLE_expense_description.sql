CREATE TABLE IF NOT EXISTS expense_description AS (
    SELECT DISTINCT
        DS_DESPESA
    FROM paid_expense
)
