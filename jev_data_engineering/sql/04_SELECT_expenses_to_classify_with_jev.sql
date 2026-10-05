CREATE OR REPLACE TABLE expenses_to_classify_with_jev AS (
    SELECT DS_DESPESA
    FROM expense_description
    WHERE DS_DESPESA IS NOT NULL
    AND trim(DS_DESPESA) <> ''
    AND DS_DESPESA NOT IN (
      SELECT DS_DESPESA FROM expense_classified_by_rules
    )
    AND DS_DESPESA NOT IN (
        SELECT description FROM jev_predicted_tag
    )
)
