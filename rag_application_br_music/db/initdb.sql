
CREATE EXTENSION vector;
CREATE TABLE songs (
    id bigserial PRIMARY KEY, 
    embedding vector(1536),
    author TEXT,
    lyrics TEXT,
    "name" TEXT,
    model  TEXT
);
