#!/usr/bin/env bash

set -e

echo "Starting download..."
./download.sh

echo "Download completed."

echo "Starting extraction..."
./extract_all.sh

echo "Extraction completed."

echo "Pipeline finished successfully."