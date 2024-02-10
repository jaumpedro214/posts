CREATE TABLE _12_sum_deposits_withdraws AS
    SELECT
        account_id, 
        transaction_id, 
        transaction_date, 
        amount,
        
        -- Using CASE
        SUM( 
            CASE 
                WHEN amount > 0 THEN amount 
                ELSE 0 
            END 
        ) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sum_deposits,

        -- Using FILTER
        SUM(amount) FILTER (WHERE amount < 0) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sum_withdraws

    FROM
        bank_transactions