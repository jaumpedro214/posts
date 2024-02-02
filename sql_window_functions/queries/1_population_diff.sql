CREATE TABLE _1_population_diff AS
  SELECT
    year,
    population,
    population - LAG(population, 1) OVER (ORDER BY year) AS population_diff
  FROM
    world_population;

