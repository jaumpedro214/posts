import json
from pydantic import BaseModel
from typing import Literal, Text
import random
from pprint import pprint

from rag import get_embedding_api_client, retrieve_similar_documents_from_database


BASIC_PROMPT = "The following song verses are from a Brazilian song, answer me the singer of the song. Answer me just the name of the singer, nothing more. Verses:\n {verses}."
RAG_PROMPT = "The following song verses are from a Brazilian song, answer me the singer of the song. Answer me just the name of the singer, nothing more. Verses:\n {verses}. Use these song lyrics: {song_lyrics}"


# List of every artist in the database
class Artist(BaseModel):
    name: Literal[
        'Reginaldo Rossi',
        'Roberto Carlos',
        'Falcao',
        'Fagner',
        'Zezo',
        'Ivete Sangalo',
        'Joao Gomes',
        'Flavio Jose',
        'Alceu Valenca',
        'Jose Augusto',
        'Marisa Monte',
        'Pinduca',
        'Joelma',
        'Chitaozinho e Xororo',
        'Bruno e Marrone',
        'Erasmo Carlos'
    ]

class DummyClassifier:

    def __init__(self) -> None:
        self.artists = [
            'reginaldo-rossi', 'roberto-carlos',
            'falcao','fagner','zezo',
            'ivete-sangalo','joao-gomes','flavio-jose',
            'alceu-valenca','jose-augusto','marisa-monte',
            'pinduca','joelma','chitaozinho-e-xororo',
            'bruno-e-marrone','erasmo-carlos'
        ]
    
    def classify(self, verses: Text):
        return random.choice(self.artists)

class GPTClassifier:
    def __init__(self) -> None:
        self.client = get_embedding_api_client()
    
    def classify(self, verses: Text):
        prompt = BASIC_PROMPT.format(verses=verses)
        response = self.client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "user", "content": prompt},
            ]
        )
        
        return response.output_text

class RAGClassifier:
    def __init__(self) -> None:
        self.client = get_embedding_api_client()
    
    def classify(self, verses: Text):
        documents = retrieve_similar_documents_from_database(verses, num_docs=3)
        
        song_lyrics = '\n\n'.join(
            [
                f"{doc['author'].replace('-', ' ').capitalize()}-{doc['name']}\n{doc['lyrics']}" 
                for doc in documents
            ]
        )
        
        prompt = RAG_PROMPT.format(verses=verses, song_lyrics=song_lyrics)
        response = self.client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "user", "content": prompt},
            ]
        )
        
        return response.output_text
    
class GPTStructuredClassifier:

    def __init__(self) -> None:
        self.client = get_embedding_api_client()

    def classify(self, verses: Text) -> str:
        prompt = BASIC_PROMPT.format(verses=verses)

        response = self.client.responses.parse(
            model="gpt-4o-mini",
            input=[
                {"role": "user", "content": prompt},
            ],
            text_format=Artist  # valida automaticamente com o modelo
        )

        artist: Artist = response.output_parsed  # já é uma instância Pydantic
        return artist.name


def load_lyrics():
    with open('br-song-lyrics.json', 'r') as f:
        lyrics = json.load(f)
    return lyrics

def create_classification_database(n_examples=100, n_verses=1):
    lyrics = load_lyrics()
    classification_dataset = []

    print(f"Loaded {len(lyrics)} lyrics")

    # sample without reposition n_examples songs
    random.seed(214) 
    random.shuffle(lyrics)

    lyrics_selected = lyrics[:n_examples]
    print(f"Selected {len(lyrics_selected)} lyrics")

    for song_lyrics in lyrics_selected:
        artist = song_lyrics['artist']
        lyrics = song_lyrics['lyrics']
        title = song_lyrics['title']

        # select a random verses in the song
        verses = lyrics.split('\n')
        random.shuffle(verses)
        selected_verses = '\n'.join(verses[:n_verses])

        classification_dataset.append(
            {
                'artist': artist,
                'verses': selected_verses,
                'title': title
            }
        )

    pprint(classification_dataset[:5])
    return classification_dataset

def generate_results(output_file='results.json', n_examples=15, n_verses=3):
    dataset = create_classification_database(n_examples=n_examples, n_verses=n_verses)

    classifiers = {
        'dummy': DummyClassifier(),
        'gpt': GPTClassifier(),
        'gpt-structured': GPTStructuredClassifier(),
        'rag': RAGClassifier()
    }

    results = []
    for i, sample in enumerate(dataset, 1):
        print(f"\n[{i}/{len(dataset)}] Processing '{sample['title']}' by {sample['artist']}")

        result_entry = {
            'title': sample['title'],
            'true_artist': sample['artist'],
            'verses': sample['verses'],
            'predictions': {}
        }

        for clf_name, clf in classifiers.items():
            try:
                pred = clf.classify(sample['verses'])
            except Exception as e:
                pred = f"error: {e}"
                print(f"error: {e}")

            result_entry['predictions'][clf_name] = pred
            
        print(result_entry)
        results.append(result_entry)

    # Salvar resultados
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved to {output_file}")
    return results


if __name__ == '__main__':
    generate_results(n_examples=200, n_verses=3)
    