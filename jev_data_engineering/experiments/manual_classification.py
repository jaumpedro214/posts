import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from classification.jev import CHOICES

HERE = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(HERE, "sample.json")
OUTPUT_PATH = os.path.join(HERE, "manual_classification.json")


def load_samples(path: str = INPUT_PATH) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    return [item if isinstance(item, dict) else {"text": item} for item in data]


def load_results(path: str = OUTPUT_PATH) -> list[dict]:
    if not os.path.exists(path):
        return []

    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_results(results: list[dict], path: str = OUTPUT_PATH) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def prompt_choice(labels: list[str], current: str | None = None) -> str | None:
    """Ask the user to pick one of the labels.

    Returns the chosen label, or None to skip, or raises EOFError to quit.
    """
    print("\nClasses:")
    for i, label in enumerate(labels, start=1):
        marker = " (current)" if label == current else ""
        print(f"  [{i}] {label}{marker}")
    print("  [s] skip   [q] save and quit")

    while True:
        raw = input("> ").strip().lower()

        if raw == "q":
            raise EOFError

        if raw == "s":
            return None

        if raw.isdigit() and 1 <= int(raw) <= len(labels):
            return labels[int(raw) - 1]

        print("Invalid option, try again.")


def review_class(classname: str, results: list[dict]) -> None:
    """Reclassify every record currently labeled as `classname`."""
    targets = [(i, item) for i, item in enumerate(results) if item["classification"] == classname]

    if not targets:
        print(f"No records labeled '{classname}'.")
        return

    print(f"{len(targets)} records labeled '{classname}'. Reclassify or skip each one.")

    try:
        for index, (position, item) in enumerate(targets, start=1):
            print(f"\n[{index}/{len(targets)}] {item['text']}")
            print(f"current: {item['classification']}")

            classification = prompt_choice(list(CHOICES), current=item["classification"])

            if classification is None:
                continue

            results[position]["classification"] = classification
            save_results(results)
    except (EOFError, KeyboardInterrupt):
        print("\nInterrupted.")

    save_results(results)
    print(f"Saved -> {OUTPUT_PATH}")


def classify_pending(samples: list[dict], results: list[dict]) -> None:
    done = {item["text"] for item in results}
    pending = [item for item in samples if item["text"] not in done]

    print(f"{len(samples)} samples, {len(results)} already classified, {len(pending)} pending.")

    try:
        for index, item in enumerate(pending, start=1):
            text = item["text"]

            print(f"\n[{index}/{len(pending)}] {text}")

            classification = prompt_choice(list(CHOICES))

            if classification is None:
                continue

            results.append({"text": text, "classification": classification})
            save_results(results)
    except (EOFError, KeyboardInterrupt):
        print("\nInterrupted.")

    save_results(results)
    print(f"Saved {len(results)} classifications -> {OUTPUT_PATH}")


def main():
    parser = argparse.ArgumentParser(description="Manual classification of sampled records")
    parser.add_argument(
        "--review",
        metavar="CLASSNAME",
        help="Reclassify every record currently labeled as CLASSNAME",
    )

    args = parser.parse_args()

    results = load_results()

    if args.review:
        review_class(args.review, results)
    else:
        classify_pending(load_samples(), results)


if __name__ == "__main__":
    main()
