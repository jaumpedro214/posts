CREATE OR REPLACE TABLE expenses_report AS (

    WITH expenses AS (
        SELECT
            REGEXP_REPLACE(VR_PAGTO_DESPESA, ',', '.')::NUMERIC(10,2) AS  VR_PAGTO_DESPESA,
            AA_ELEICAO,
            classification
        FROM paid_expense_classified
    )
    SELECT
        AA_ELEICAO,
        classification,
        SUM(VR_PAGTO_DESPESA) AS total_expense
    FROM expenses
    GROUP BY AA_ELEICAO, classification
    ORDER BY AA_ELEICAO, classification

)