import json
from openai import OpenAI
import os
from pprint import pprint
import psycopg2

EMBEDDING_MODEL = "text-embedding-3-small"

def get_embedding_api_client():
    if not os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") == "":
        raise ValueError("The OPENAI_API_KEY is not specified. Create an API key in the OpenAI and copy it to the .env file.")

    client = OpenAI()
    return client

def read_song_lyrics():
    with open('br-song-lyrics.json') as f:
        lyrics = json.load(f)

    return lyrics

def embed_lyrics(songs_lyrics, embedding_client):

    for i, lyrics in enumerate(songs_lyrics):

        title = lyrics['title']
        lyric_text = lyrics['lyrics']
        author = lyrics['artist'].replace('-', ' ').capitalize()

        print(f'{i+1}/{len(songs_lyrics)} {author} - {title}')

        lyric_full = f"""{author} - {title}\n\n{lyric_text}"""

        try:
            response = embedding_client.embeddings.create(
                input=lyric_full,
                model=EMBEDDING_MODEL
            )

            embedding_vector = response.data[0].embedding

            lyrics['vector'] = embedding_vector
            lyrics['model']  = EMBEDDING_MODEL
        except Exception as e:
            print(f"Error embedding song - {str(e)}")

    return songs_lyrics

def save_embeddings_in_file(lyrics_with_embeddings):
    
    with open('br-songs-lyrics-embedded.json', 'w') as f:
        json.dump(lyrics_with_embeddings, f)

def connect_to_postgresql():

    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_DB = os.getenv('POSTGRES_DB')

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port="5432",
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = True
    return conn

def save_embeddings_in_postgresql(lyrics_with_embeddings):

    if not lyrics_with_embeddings:
        raise ValueError("No songs to save in the argument 'lyrics_with_embeddings'")

    conn = connect_to_postgresql()
    cur = conn.cursor()

    insert_query = """
        INSERT INTO songs (embedding, author, lyrics, "name", model)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        for item in lyrics_with_embeddings:
            embedding_str = str(item["vector"])  # converte o vetor para string
            cur.execute(
                insert_query,
                (
                    embedding_str,           # embedding
                    item["artist"],          # author
                    item["lyrics"],          # lyrics
                    item["title"],           # name
                    item["model"],           # model
                )
            )
        conn.commit()
        print(f"{len(lyrics_with_embeddings)} registros inseridos em 'songs'")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao inserir dados: {e}")
    finally:
        cur.close()
        conn.close()

def text_embedding_flow():
    embedding_client = get_embedding_api_client()
    print("Sucessfully connect to the embedding client.")

    lyrics = read_song_lyrics()
    print("Successfully loaded song lyrics")

    lyrics_with_embeddings = embed_lyrics(lyrics, embedding_client)
    print("Successfully embedded lyrics for all songs")

    save_embeddings_in_file(lyrics_with_embeddings)
    print("Successfully saved lyrics with their embeddings in the JSON file")

    save_embeddings_in_postgresql(lyrics_with_embeddings)
    print("Successfully saved lyrics with their embeddings in the database")


if __name__ == "__main__":
    text_embedding_flow()