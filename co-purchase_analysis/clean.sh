#!/bin/sh

# Nome della cartella da eliminare
FOLDER="output"

# Controlla se la cartella esiste
if [ -d "$FOLDER" ]; then
    echo "Cleaning $FOLDER..."
    rm -rf "$FOLDER"
    echo "Clean."
else
    echo "Folder $FOLDER doesn't exist."
fi
