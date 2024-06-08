CREATE TABLE _8_fill_null_values_avg AS
    SELECT
        *,

        CASE 
            WHEN event_value IS NULL THEN
                AVG(event_value)
                OVER (
                    ORDER BY event_start_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ELSE
                event_value
        END AS event_value_filled

    FROM events_missing_values