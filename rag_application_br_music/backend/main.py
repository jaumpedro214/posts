from fastapi import FastAPI
import psycopg
import os

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Backend FastAPI rodando com sucesso!"}

@app.get("/db-check")
def check_db():
    
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")

    try:
        with psycopg.connect(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
