#!/bin/sh

# Nomi delle cartelle da eliminare
FOLDERS=("output" "target")

# Loop attraverso le cartelle
for FOLDER in "${FOLDERS[@]}"; do
    # Controlla se la cartella esiste
    if [ -d "$FOLDER" ]; then
        echo "Cleaning $FOLDER..."
        rm -rf "$FOLDER"
        echo "Clean."
    else
        echo "Folder $FOLDER doesn't exist."
    fi
done
