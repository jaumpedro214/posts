import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

from classification.jev import CHOICES, jev_classification

HERE = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(HERE, "sample.json")
OUTPUT_PATH = os.path.join(HERE, "jev_classification.json")

MAX_CONCURRENCY = 20


async def main():
    with open(INPUT_PATH, encoding="utf-8") as f:
        samples = json.load(f)

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async with httpx.AsyncClient(timeout=60.0) as client:
        async def classify(text):
            async with semaphore:
                return await jev_classification(text, client, CHOICES)

        results = await asyncio.gather(*(classify(item["text"]) for item in samples))

    output = [
        {
            "text": r.text,
            "classification": r.classification,
            "confidence": r.confidence,
        }
        for r in results
    ]

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(output)} classifications -> {OUTPUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
