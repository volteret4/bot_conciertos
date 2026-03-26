#!/usr/bin/env python3
"""
Archivo de configuración de ejemplo para el bot de seguimiento de artistas
Copia este archivo como config.py y modifica los valores según tu configuración
"""

import os

# Token del bot de Telegram (obtenlo de @BotFather)
#TELEGRAM_TOKEN = "TU_TOKEN_DE_TELEGRAM_AQUI"

# Configuración de la base de datos
DB_PATH = "artist_tracker.db"

# Directorio para cache de MusicBrainz
CACHE_DIR = "./cache"

# Configuración de MusicBrainz User-Agent
USER_AGENT = {
    "app": "ArtistTrackerBot",
    "version": "1.0",
    "contact": "tu_email@ejemplo.com"  # Cambia por tu email real
}

# Variables de entorno (opcional - puedes usar estas en lugar de las constantes)
# export TELEGRAM_TOKEN="tu_token_aqui"
# export DB_PATH="artist_tracker.db"
# export CACHE_DIR="./cache"
