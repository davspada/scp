#!/bin/bash

# Controlla che sia stato passato il file in input
if [ $# -ne 1 ]; then
  echo "Usage: $0 <input_file>"
  exit 1
fi

input_file="$1"

# Ordina il file in base alla terza colonna (numero di co-acquisti) in ordine decrescente e mostra i primi 10
sort -t, -k3,3nr "$input_file" | head -n 10
