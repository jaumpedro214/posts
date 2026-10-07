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
        AND confidence > 0.7
    )
    OR
    (
        classification = 'Hiring and Services'
        AND confidence > 0.4
    )
    OR
    (
        classification = 'Online Advertising'
        AND confidence > 0.7
    )
    OR
    (
        classification = 'Physical Space'
        AND confidence > 0.0
    )
    OR
    (
        classification = 'Press, Radio and TV'
        AND confidence > 0.0
    )
    OR
    (
        classification = 'Printed Material'
        AND confidence > 0.6
    )
    OR
    (
        classification = 'Rallies and Actions'
        AND confidence > 0.5
    )
    OR
    (
        classification = 'Tariffs and taxes'
        AND confidence > 0.0
    )
    OR
    (
        classification = 'Transport'
        AND confidence > 0.0
    )

);