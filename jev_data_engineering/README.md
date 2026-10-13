# JEV Data Engineering

Pipeline that downloads the Brazilian electoral accounting data (TSE "prestação de contas"),
loads it into DuckDB (DuckLake), classifies each expense description with rules and with the
**JEV** model (Typesafe API), and produces an expenses report per category.

## Requirements

- [uv](https://docs.astral.sh/uv/) (Python 3.12 is managed automatically)
- A Typesafe API key (`TYPESAFE_API_KEY`) for the classification step

## Install

```bash
cd jev_data_engineering

# install dependencies and create the virtualenv
uv sync

# create your .env from the template and fill in the keys
cp .env.example .env
```

Edit `.env`:

```
TYPESAFE_API_KEY=your-key-here
```

## Run

Run each stage in order (all commands from `jev_data_engineering/`):

```bash
uv run download.py   # 1. download the TSE zip files
uv run extract.py    # 2. unzip and convert encoding to UTF-8
uv run process.py    # 3. build the DuckDB tables (SQL pipeline)
uv run classify.py   # 4. classify expense descriptions with JEV
uv run process.py    # 3. rebuild the DuckDB tables (SQL pipeline)
```

## What each script does

| Script | Summary |
| --- | --- |
| `download.py` | Downloads the TSE candidate accounting `.zip` files into `data/files/` (skips files already downloaded). |
| `extract.py` | Extracts the zip files into `data/extracted/` and converts the CSVs from `latin-1` to `utf-8`. |
| `process.py` | Runs the SQL scripts in `sql/` against DuckDB/DuckLake (DuckDB file in `data/duckdb/`) to build the `paid_expense`, `expense_description`, rule-based and JEV classification tables, and the final `expenses_report`. |
| `classify.py` | Reads the still-unclassified descriptions, sends them to the JEV model (Typesafe API) concurrently in batches, and stores `classification` + `confidence` in `expenses_classified_by_jev`. |

## Notes

- The DuckDB database lives in `data/duckdb/` and the raw/extracted data in `data/files/` and `data/extracted/`.
- `classify.py` makes real API calls and can be tuned at the bottom of the file
  (`batch_size`, `max_concurrency`, `max_records`).
- The notebooks/scripts in `experiments/` are for evaluating JEV quality (sampling, manual
  labeling, cost estimation and the performance analysis notebook).
