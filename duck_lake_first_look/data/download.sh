#!/usr/bin/env bash

set -e

years=()
for y in $(seq 2012 2 2024); do
    years+=("$y")
done
years+=("ATUAL")

states=(
AC AL AM AP BA CE ES GO MA MG MS MT
PA PB PE PI PR RJ RN RO RR RS SE SC SP TO
)

base_url="https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitor_secao/perfil_eleitor_secao_%s_%s.zip"

for year in "${years[@]}"; do
    mkdir -p "./$year"

    for state in "${states[@]}"; do
        file="./$year/perfil_eleitor_secao_${year}_${state}.zip"
        url=$(printf "$base_url" "$year" "$state")

        if [ -f "$file" ]; then
            echo "Skipping (already exists): $file"
            continue
        fi

        echo "Downloading year=$year state=$state"

        if curl -fL --connect-timeout 60 --retry 3 --retry-delay 5 -o "$file" "$url"; then
            echo "Saved: $file"
        else
            echo "Failed: $url"
            rm -f "$file"
        fi
    done
done