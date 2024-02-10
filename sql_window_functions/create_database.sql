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

CREATE TABLE IF NOT EXISTS salary_by_worker (
    person_id VARCHAR(255),
    name VARCHAR(255),
    date DATE,
    salary FLOAT
);

INSERT INTO salary_by_worker (person_id, name, date, salary)
VALUES
('01', 'João', '2023-01-01', 100),
('01', 'João', '2023-02-01', 100),
('01', 'João', '2023-03-01', 100),
('01', 'João', '2023-04-01', 100),
('01', 'João', '2023-05-01', 100),
('01', 'João', '2023-06-01', 100),
('01', 'João', '2023-07-01', 100),
('01', 'João', '2023-08-01', 100),
('01', 'João', '2023-09-01', 100),
('01', 'João', '2023-10-01', 100),
('01', 'João', '2023-11-01', 100),
('01', 'João', '2023-12-01', 150),
('02', 'Maria', '2023-01-01', 150),
('02', 'Maria', '2023-02-01', 150),
('02', 'Maria', '2023-03-01', 175),
('02', 'Maria', '2023-04-01', 175),
('02', 'Maria', '2023-05-01', 175),
('02', 'Maria', '2023-06-01', 175),
('02', 'Maria', '2023-07-01', 175),
('02', 'Maria', '2023-08-01', 175),
('02', 'Maria', '2023-09-01', 175),
('02', 'Maria', '2023-10-01', 175),
('02', 'Maria', '2023-11-01', 175),
('02', 'Maria', '2023-12-01', 225),
('03', 'Pedro', '2023-01-01', 110),
('03', 'Pedro', '2023-02-01', 110),
('03', 'Pedro', '2023-03-01', 110),
('03', 'Pedro', '2023-04-01', 110),
('03', 'Pedro', '2023-05-01', 110),
('03', 'Pedro', '2023-06-01', 110),
('03', 'Pedro', '2023-07-01', 110),
('03', 'Pedro', '2023-08-01', 110),
('03', 'Pedro', '2023-09-01', 110),
('03', 'Pedro', '2023-10-01', 110),
('03', 'Pedro', '2023-11-01', 110),
('03', 'Pedro', '2023-12-01', 190);


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
    

CREATE TABLE IF NOT EXISTS covid_daily_cases (
    day DATE,
    cases INT
);

INSERT INTO covid_daily_cases (day, cases)
VALUES
('2023-01-01', 10),
('2023-01-02', 12),
('2023-01-03', 15),
('2023-01-04', 20),
('2023-01-05', 25),
('2023-01-06', 30),
('2023-01-07', 35),
('2023-01-08', 40),
('2023-01-09', 45),
('2023-01-10', 50),
('2023-01-11', 55),
('2023-01-12', 52),
('2023-01-13', 49),
('2023-01-14', 46),
('2023-01-15', 43),
('2023-01-16', 40),
('2023-01-17', 37),
('2023-01-18', 34),
('2023-01-19', 31),
('2023-01-20', 28),
('2023-01-21', 25),
('2023-01-22', 22),
('2023-01-23', 19),
('2023-01-24', 16),
('2023-01-25', 13),
('2023-01-26', 10),
('2023-01-27', 7);

CREATE TABLE IF NOT EXISTS events (
    event_start_time TIMESTAMP,
    event_id VARCHAR(8)
);

INSERT INTO events (event_start_time, event_id)
VALUES
('2024-01-01 12:30:16', 'e8C3FG1H'),
('2024-01-01 12:31:55', 'DS4dsg3g'),
('2024-01-01 12:33:42', 'hG5fDg2S'),
('2024-01-01 12:35:21', 'jK9sHd4F'),
('2024-01-01 12:37:08', 'lM3gFh6J'),
('2024-01-01 12:38:47', 'nP7jKl8M'),
('2024-01-01 12:40:34', 'qR2mNp0Q'),
('2024-01-01 12:42:13', 'sT6qRr2U'),
('2024-01-01 12:43:59', 'uX0vSs4W'),
('2024-01-01 12:45:38', 'wZ4yVv6Y');

CREATE TABLE IF NOT EXISTS events_missing_values (
    event_start_time TIMESTAMP,
    event_value INT
);

INSERT INTO events_missing_values (event_start_time, event_value)
VALUES
('2024-01-01 12:30:16', 50),
('2024-01-01 12:31:55', 50),
('2024-01-01 12:33:42', 70),
('2024-01-01 12:35:21', 80),
('2024-01-01 12:37:08', NULL),
('2024-01-01 12:38:47', 92),
('2024-01-01 12:40:34', 82),
('2024-01-01 12:42:13', 70),
('2024-01-01 12:43:59', NULL),
('2024-01-01 12:45:38', NULL),
('2024-01-01 12:55:10', 60),
('2024-01-01 12:56:50', NULL);

CREATE TABLE IF NOT EXISTS user_email_history (
    user_id VARCHAR(255),
    email VARCHAR(255),
    created_at DATE
);

INSERT INTO user_email_history (user_id, email, created_at)
VALUES
('01abc', 'joao@email.com',      '2023-01-01 12:30:16'),
('01abc', 'joaopedro@email.com', '2023-01-01 12:31:55'),
('01xyz', 'jhon@zmail.com',      '2023-01-01 12:33:42'),
('01xyz', 'jhon2@zmail.com',     '2023-12-04 16:35:21'),
('01xyz', 'jhon3@zmail.com',     '2024-01-10 09:37:08'),
('e3r2d', 'maria@box.edu.br',    '2012-01-06 12:38:47'),
('e3r2d', 'maria@zmail.com',     '2024-01-01 12:40:34');