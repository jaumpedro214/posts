#!/usr/bin/env bash
#
# Abre o CLI do DuckDB com o catálogo DuckLake (expenses) já anexado.
#
# Uso:
#   ./database.sh                # abre o CLI interativo
#   ./database.sh -c "SHOW TABLES;"  # executa um comando e sai
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

DATABASE_PATH="./data/duckdb/database.duckdb"
METADATA_PATH="./data/duckdb/metadata.sqlite"
DATA_PATH="data/duckdb/"
ALIAS="expenses"

INIT_SQL="INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite; ATTACH 'ducklake:sqlite:${METADATA_PATH}' AS ${ALIAS} (DATA_PATH '${DATA_PATH}'); USE ${ALIAS};"

exec duckdb -init /dev/null -cmd "$INIT_SQL" "$DATABASE_PATH" "$@"
