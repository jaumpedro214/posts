#!/usr/bin/env bash

set -e

echo "Starting download..."
./download.sh

echo "Download completed."

echo "Starting extraction..."
./extract.sh

echo "Extraction completed."

echo "Pipeline finished successfully."