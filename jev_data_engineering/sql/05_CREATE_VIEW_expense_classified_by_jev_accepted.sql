CREATE VIEW IF NOT EXISTS expense_classified_by_jev_accepted AS
SELECT *
FROM expenses_classified_by_jev
WHERE
classification != 'Others'
AND (
    (
        classification = 'Electronics Furniture Office'
        AND confidence > 0.6
    )
    OR
    (
        classification = 'Food'
    )
    OR
    (
        classification = 'Hiring and Services'
    )
    OR
    (
        classification = 'Online Advertising'
    )
    OR
    (
        classification = 'Physical Space'
    )
    OR
    (
        classification = 'Press, Radio and TV'
        AND confidence > 0.95
    )
    OR
    (
        classification = 'Printed Material'
    )
    OR
    (
        classification = 'Rallies and Actions'
    )
    OR
    (
        classification = 'Tariffs and taxes'
    )
    OR
    (
        classification = 'Transport'
    )

);