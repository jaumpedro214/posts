import os
import json
import psycopg2
from openai import OpenAI

from embedding import get_embedding_api_client, connect_to_postgresql, EMBEDDING_MODEL


def embed_question(question: str):
    embedding_client = get_embedding_api_client()

    response = embedding_client.embeddings.create(
        input=question,
        model=EMBEDDING_MODEL
    )

    embedding_vector = response.data[0].embedding
    return embedding_vector

def generate_response(question: str):
    client = get_embedding_api_client()

    response = client.responses.create(
        model="o4-mini",
            input=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": question,
                        },
                    ]
                }
            ]
    )

    print(response.output_text)
    return response.output_text

def generate_rag_response(question: str):
    return question

def retrieve_similar_documents_from_database(question: str):
    pass

def generate_responses(question: str):
    normal_response = generate_response(question)
    rag_response = generate_rag_response(question)

    return {
        'normal_response': normal_response,
        'rag_response': rag_response
    }