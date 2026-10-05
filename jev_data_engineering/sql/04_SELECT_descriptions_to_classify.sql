CREATE OR REPLACE TABLE descriptions_to_classify AS (
    SELECT DS_DESPESA
    FROM expense_description
    WHERE DS_DESPESA IS NOT NULL
      AND trim(DS_DESPESA) <> ''
      AND DS_DESPESA NOT IN (
        SELECT description FROM jev_predicted_tag
    )
)
