CREATE TABLE _6_event_time AS
    SELECT
        event_id,
        event_start_time,
        (   
            -- Julianday is a requirement of SQLite to calculate the difference between two dates
            JULIANDAY(LEAD(event_start_time, 1) OVER (ORDER BY event_start_time)) 
            - JULIANDAY(event_start_time)
        ) * 86400.0 -- To convert from days to seconds 
        AS duration
    FROM events;