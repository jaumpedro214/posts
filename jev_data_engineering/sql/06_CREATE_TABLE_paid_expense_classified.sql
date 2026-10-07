CREATE OR REPLACE TABLE paid_expense_classified AS (

    SELECT
        paid_expense.*,
        coalesce(rule.classification, jev.classification, 'UNCLASSIFIED') AS classification
    FROM paid_expense
    
    LEFT JOIN expense_classified_by_jev_accepted AS jev
    ON paid_expense.DS_DESPESA = jev."description"

    LEFT JOIN expense_classified_by_rules AS rule
    ON paid_expense.DS_DESPESA = rule.DS_DESPESA

);