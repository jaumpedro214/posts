CREATE TABLE _10_frequency_encoder AS
    SELECT
        *,
        CAST( COUNT(*) OVER (PARTITION BY product_class) AS FLOAT ) / 
        CAST( COUNT(*) OVER() AS FLOAT ) AS class_frequency
    FROM
        products;