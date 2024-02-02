CREATE TABLE _4_cumulative_salary_worker AS
    SELECT
        date,
        name,
        salary,
        SUM(salary) OVER (PARTITION BY person_id ORDER BY date)
        AS cumulative_salary
    FROM salary_by_worker;
