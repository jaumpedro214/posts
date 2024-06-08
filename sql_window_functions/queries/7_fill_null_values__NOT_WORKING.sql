CREATE TABLE _7_fill_null_values AS
    SELECT
        *,
        -- Last value ignore nulls
        -- UNFORTUNATELY, THIS FUNCTION IS NOT SUPPORTED BY SQLite
        LAST_VALUE(event_value) IGNORE NULLS OVER (ORDER BY event_start_time) AS last_value_ignore_nulls
    FROM events_missing_values