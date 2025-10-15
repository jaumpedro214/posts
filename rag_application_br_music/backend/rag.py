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

    similar_documents = retrieve_similar_documents_from_database(question)

    return {
        'docs': similar_documents,
        'content': question
    }

def retrieve_similar_documents_from_database(question: str, threshold=0.5, num_docs=2):
    
    question_vector = embed_question(question)
    conn = connect_to_postgresql()
    cursor = conn.cursor()


    sql_documents = f"""
        SELECT name, lyrics, author, (embedding <=> \'{question_vector}\') AS distance
        FROM songs
        WHERE (embedding <=> \'{question_vector}\') > {threshold}
        ORDER BY embedding <=> \'{question_vector}\'
        LIMIT {num_docs}
    """

    try:
        cursor.execute(sql_documents)
        rows = cursor.fetchall()

        # Filtra por threshold (distância menor = mais similar)
        results = []
        for row in rows:
            name, lyrics, author, distance  = row
            results.append({
                "name": name,
                "author": author,
                "lyrics": lyrics,
                "distance": distance
            })

        return results

    except Exception as e:
        print(f"Erro ao recuperar documentos: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def generate_responses(question: str):
    normal_response = generate_response(question)
    rag_response = generate_rag_response(question)

    return {
        'normal_response': {
            'content': normal_response
        },
        'rag_response': rag_response
    }