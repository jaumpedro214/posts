CREATE TABLE _2_population_diff_by_country AS
    SELECT
        country,
        year,
        population,
        population - LAG(population, 1) OVER (PARTITION BY country ORDER BY year) AS population_diff
    FROM
        country_population;
