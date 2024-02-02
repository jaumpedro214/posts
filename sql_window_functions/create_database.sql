CREATE TABLE IF NOT EXISTS salaries (
    date DATE,
    salary FLOAT
);

INSERT INTO salaries (date, salary)
VALUES
('2023-01-01', 100),
('2023-02-01', 100),
('2023-03-01', 100),
('2023-04-01', 100),
('2023-05-01', 100),
('2023-06-01', 100),
('2023-07-01', 100),
('2023-08-01', 100),
('2023-09-01', 100),
('2023-10-01', 100),
('2023-11-01', 100),
('2023-12-01', 150);


CREATE TABLE IF NOT EXISTS world_population (
    year INT,
    population BIGINT
);

INSERT INTO world_population (year, population)
VALUES
(2020, 7794798739),
(2021, 7837008946),
(2022, 7879258361),
(2023, 7921549600),
(2024, 7963882643),
(2025, 8006257500),
(2026, 8048674175),
(2027, 8091132666),
(2028, 8133632970),
(2029, 8176175085),
(2030, 8218759000);

CREATE TABLE country_population (
    population BIGINT,
    country VARCHAR(255),
    year INT
);

INSERT INTO country_population (population, country, year)
VALUES
    (210.2, 'Brazil', 2010),
    (45.80, 'Argentina', 2010),
    (20.10, 'Chile', 2010),
    (215.2, 'Brazil', 2011),
    (50.30, 'Argentina', 2011),
    (21.50, 'Chile', 2011),
    (220.3, 'Brazil', 2012),
    (54.20, 'Argentina', 2012),
    (22.90, 'Chile', 2012),
    (225.5, 'Brazil', 2013),
    (58.10, 'Argentina', 2013),
    (24.80, 'Chile', 2013),
    (230.8, 'Brazil', 2014),
    (61.90, 'Argentina', 2014),
    (26.70, 'Chile', 2014),
    (236.3, 'Brazil', 2015),
    (65.80, 'Argentina', 2015),
    (28.60, 'Chile', 2015),
    (241.9, 'Brazil', 2016),
    (69.70, 'Argentina', 2016),
    (30.50, 'Chile', 2016),
    (247.6, 'Brazil', 2017),
    (73.60, 'Argentina', 2017),
    (32.40, 'Chile', 2017),
    (253.5, 'Brazil', 2018),
    (77.50, 'Argentina', 2018),
    (34.30, 'Chile', 2018),
    (259.6, 'Brazil', 2019),
    (81.40, 'Argentina', 2019),
    (36.20, 'Chile', 2019),
    (265.9, 'Brazil', 2020),
    (85.30, 'Argentina', 2020),
    (38.10, 'Chile', 2020);
    