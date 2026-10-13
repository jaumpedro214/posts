CREATE TABLE IF NOT EXISTS expenses_classified_by_jev (
    description VARCHAR NOT NULL,
    classification VARCHAR,
    confidence DOUBLE,
    created_at TIMESTAMP DEFAULT current_timestamp,
    updated_at TIMESTAMP DEFAULT current_timestamp
)