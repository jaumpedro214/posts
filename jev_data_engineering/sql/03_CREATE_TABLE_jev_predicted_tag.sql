CREATE TABLE jev_predicted_tag (
    description VARCHAR NOT NULL,
    tag VARCHAR,
    confidence DOUBLE,
    created_at TIMESTAMP DEFAULT current_timestamp,
    updated_at TIMESTAMP DEFAULT current_timestamp
)
