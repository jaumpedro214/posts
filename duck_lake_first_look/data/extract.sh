#!/usr/bin/env bash

set -e

for year_dir in /data/*; do
    [ -d "$year_dir" ] || continue

    extract_root="$year_dir/extracted"
    mkdir -p "$extract_root"

    for zip_file in "$year_dir"/*.zip; do
        [ -e "$zip_file" ] || continue

        filename=$(basename "$zip_file" .zip)
        target_dir="$extract_root/$filename"

        # Skip if already extracted
        if [ -d "$target_dir" ]; then
            echo "Skipping $target_dir (already exists)"
            continue
        fi

        echo "Extracting $zip_file -> $target_dir"

        mkdir -p "$target_dir"
        unzip -q "$zip_file" -d "$target_dir"
    done
done

echo "Done extracting all files."