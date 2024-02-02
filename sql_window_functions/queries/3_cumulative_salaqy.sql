CREATE TABLE _3_cumulative_salary AS
SELECT
    date,
    SUM(salary) OVER (ORDER BY date) as cumulative_salary
FROM salaries;
