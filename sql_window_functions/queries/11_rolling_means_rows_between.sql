CREATE TABLE _11_rolling_means AS
    SELECT  
        day,
        cases,
        AVG(CASES) 
        OVER (
            ORDER BY day
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) 
        AS cases_roll_mean_3
    FROM covid_daily_cases;