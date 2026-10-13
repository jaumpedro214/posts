import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

from classification.jev import CHOICES, jev_classification

HERE = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(HERE, "cost_estimate.json")

PRICE_PER_MILLION_USD = 0.042

TEXTS = [
    "Compra de gasolina para carro da campanha",
    "Impulsionamento de post no Instagram",
    "Conta de luz do comitê",
    "Impressão de 5000 santinhos",
    "Pagamento de aluguel de carro para o candidato",
    "Anúncio na capa do jornal local",
    "Inserção de propaganda na rádio",
    "Café e água para a equipe",
    "Aluguel de espaço para comício",
    "Compra de notebook para o escritório",
]


async def main() -> None:
    parser = argparse.ArgumentParser(description="Estimate JEV classification cost")
    parser.add_argument(
        "classifications",
        nargs="?",
        type=int,
        help="Total number of classifications to estimate the cost for",
    )

    args = parser.parse_args()

    async with httpx.AsyncClient(timeout=60.0) as client:
        results = [await jev_classification(text, client, CHOICES) for text in TEXTS]

    rows = [
        {
            "text": r.text,
            "classification": r.classification,
            "confidence": r.confidence,
            "input_tokens": r.input_tokens,
        }
        for r in results
    ]

    total_input_tokens = sum(r.input_tokens or 0 for r in results)
    avg_input_tokens = total_input_tokens / len(results)
    cost = total_input_tokens / 1_000_000 * PRICE_PER_MILLION_USD

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "price_per_million_usd": PRICE_PER_MILLION_USD,
                "texts": len(TEXTS),
                "total_input_tokens": total_input_tokens,
                "estimated_cost_usd": cost,
                "results": rows,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    for r in rows:
        print(f"{r['input_tokens']:>5} tokens  {r['classification']:<26} {r['text'][:50]}")

    print()
    print(f"texts:               {len(TEXTS)}")
    print(f"total input tokens:  {total_input_tokens}")
    print(f"price per 1M tokens: ${PRICE_PER_MILLION_USD}")
    print(f"estimated cost per classification:      ${cost:.8f} USD")

    if args.classifications is not None:
        total_cost = args.classifications * avg_input_tokens / 1_000_000 * PRICE_PER_MILLION_USD
        print()
        print(f"classifications requested: {args.classifications}")
        print(f"avg input tokens:          {avg_input_tokens:.1f}")
        print(f"estimated total cost:      ${total_cost:.4f} USD")

    print(f"saved -> {OUTPUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
