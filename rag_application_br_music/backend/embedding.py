import json
from openai import OpenAI
import os
from pprint import pprint

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

    EMBEDDING_MODEL = "text-embedding-3-small"

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

    return lyrics

def save_embeddings_in_file(lyrics_with_embeddings):
    
    with open('br-songs-lyrics-embedded.json', 'w') as f:
        json.dump(lyrics_with_embeddings, f)

def save_embeddings_in_postgresql():
    pass

def connect_to_postgresql():
    pass

def text_embedding_flow():
    embedding_client = get_embedding_api_client()
    print("Sucessfully connect to the embedding client.")

    lyrics = read_song_lyrics()[:4]
    print("Successfully loaded song lyrics")

    lyrics_with_embeddings = embed_lyrics(lyrics, embedding_client)
    print("Successfully embedded lyrics for all songs")

    save_embeddings_in_file(lyrics_with_embeddings)
    print("Successfully saved lyrics with their embeddings in the JSON file")

    

text_embedding_flow()