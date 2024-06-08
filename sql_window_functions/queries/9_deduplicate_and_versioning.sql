CREATE TABLE _9_current_user_email AS
    SELECT
        user_id, email, created_at
    FROM
    (
        SELECT
            user_id, 
            email, 
            created_at,
            ROW_NUMBER() 
            OVER (PARTITION BY user_id ORDER BY created_at DESC) AS row_number
        FROM
            user_email_history
    ) AS subquery
    WHERE
        row_number = 1;