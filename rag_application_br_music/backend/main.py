from fastapi import FastAPI
import psycopg2
import os

from rag import generate_responses

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Backend FastAPI rodando com sucesso!"}

@app.get("/generate-response")
def generate_response(
    question: str
):
    return generate_responses(question)
