import httpx
import os
import asyncio
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class JevClassificationResult:
    description: str
    tag: str
    confidence: str

async def classify_expense_with_jev(description: str) -> JevClassificationResult:

    api_key = os.environ.get("TYPESAFE_API_KEY")

    if not api_key:
        raise ValueError('Error importing ENV Variables, TYPESAFE_API_KEY not found.')

    base_url = "https://api.typesafe.ai/v1/systemone"
    choices = {
        "Transport": "Transport-related expenses such as gasoline and car/bus rental",
        "Tariffs and taxes": "Tariffs and taxes related to the purchase of products or services such as water, gas and electricity",
        "Online Advertising": "Online advertising campaigns, social-media boosting and other services",
        "Printed Material": "Newspapers, folders, banners, flags, stickers, 'santinhos', 'cola de votação', shirts, caps, among others",
        "Hiring": "Expenses related to hiring staff such as drivers, secretaries, accountants, directors, cleaners, etc.",
        "Food": "Food and drinks",
        "Physical Space": "Physical space rental or allocation",
        "Rallies and Actions": "Public actions, rallies and electoral parades. Hiring of 'cabo eleitoral'",
        "Consumer Electronics": "Computers, microwaves, air conditioning, TVs, monitors, freezers",
        "Others": "None of the above categories",
    }

    payload = {
        "model": 'jev-latest',
        "state": description,
        "questions": {
            "tag": {
                "type": "choice",
                "instructions": "Which is the most adequate class to this expense description?",
                "criteria": choices
            }
        }
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            base_url,
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        data = response.json()

    return JevClassificationResult(
        description=description,
        tag=data['answers']['tag']['choice'],
        confidence=data['answers']['tag']['confidence']
    )

if __name__ == "__main__":
    expense_class = asyncio.run(classify_expense_with_jev('33 LATAS DE GUARAMIX'))
    print(expense_class)