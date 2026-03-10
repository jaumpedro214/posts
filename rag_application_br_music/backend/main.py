from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import psycopg2
import os

from rag import generate_responses

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite qualquer origem
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos (GET, POST, etc)
    allow_headers=["*"],  # Permite todos os headers
)

@app.get("/")
def read_root():
    return {"message": "Backend FastAPI rodando com sucesso!"}

@app.get("/generate-response")
def generate_response(
    question: str
):
    return generate_responses(question)
