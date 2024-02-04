CREATE TABLE _5_covid_rolling_means AS
    SELECT  
        day,
        cases,
        ((cases + LAG(cases, 1) OVER (ORDER BY day) + LAG(cases, 2) OVER (ORDER BY day))/3) AS cases_roll_mean_3
    FROM covid_daily_cases;