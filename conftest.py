"""
conftest.py (raíz del proyecto)

Asegura que la raíz del proyecto esté en sys.path al ejecutar `pytest`,
para que los imports `from src....` funcionen sin necesidad de instalar
el proyecto como paquete (pip install -e .).
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
