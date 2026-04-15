"""Configuration settings for the monitor project."""

from pathlib import Path

# Directorio base del proyecto.
BASE_DIR = Path(__file__).parent.parent.parent

# Configuración de base de datos.
DATABASE_PATH = BASE_DIR / "data" / "auctions.db"

# Directorio de datos adicional.
DATA_DIR = BASE_DIR / "data"

# Constantes generales.
PROJECT_NAME = "monitor-subastas-valencia"
VERSION = "0.1.0"

# Crear el directorio de datos si no existe.
DATA_DIR.mkdir(parents=True, exist_ok=True)
