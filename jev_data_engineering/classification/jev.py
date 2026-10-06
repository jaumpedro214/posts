import asyncio
import httpx
import os
import logging
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

CHOICES = {
    "Transport": "Gasoline, car/bus rental",
    "Online Advertising": "Online ads, boosting, paid traffic; Facebook, Instagram, TikTok",
    "Tariffs and taxes": "Utility bills (water, gas, electricity, internet), TED, DOC, PIX, boleto, bank fees",
    "Printed Material": "Folders, banners, flags, preguinhas, adesivos, santinhos, colinhas/cola de votação, shirts, caps",
    "Hiring and Services": "Hiring staff: Coordinator, accountants, cleaners, lawyers, content/video/song/jingle production, marketing and social media services",
    "Food": "Food and drinks",
    "Physical Space": "Physical space rental",
    "Rallies and Actions": "Rallies, parades, public actions, militancia, cabo eleitoral",
    "Electronics Furniture Office": "Computers, microwaves, AC, TVs, freezers; chairs, desks, shelves, sofas; office supplies",
    "Others": "None of the other or ambiguous",
}


@dataclass
class ClassificationResult:
    text: str
    classification: str
    confidence: float | None = None


async def jev_classification(
    text: str,
    client: httpx.AsyncClient,
    choices: dict[str, str],
) -> ClassificationResult:

    base_url = "https://api.typesafe.ai/v1/systemone"
    model = "jev-latest"

    api_key = os.environ.get("TYPESAFE_API_KEY")

    if not api_key:
        raise ValueError('Error importing ENV Variables, TYPESAFE_API_KEY not found.')

    logger.debug(f"[REQUEST] {text}")

    payload = {
        "model": model,
        "state": text,
        "questions": {
            "tag": {
                "type": "choice",
                "instructions": "Which is the most adequate class to this expense description?",
                "criteria": choices
            }
        }
    }

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
    result = ClassificationResult(
        text=text,
        classification=data['answers']['tag']['choice'],
        confidence=data['answers']['tag'].get('confidence'),
    )

    logger.debug(f"[RESPONSE] {text} -> {result.classification} ({result.confidence})")

    return result


async def _integration_test() -> None:
    text = "Compra de gasolina para carro da campanha"
    async with httpx.AsyncClient(timeout=60.0) as client:
        result = await jev_classification(text, client, CHOICES)
    print(result)


if __name__ == "__main__":
    asyncio.run(_integration_test())
