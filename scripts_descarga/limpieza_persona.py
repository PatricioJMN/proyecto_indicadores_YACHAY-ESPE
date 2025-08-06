#!/usr/bin/env python3
# limpieza_persona.py
# Descomprime zips anidados y copia CSV según reglas por año sólo si NO existen en processed
# Basado en código original :contentReference[oaicite:2]{index=2}

import os
import re
import shutil
import zipfile
import tempfile
from pathlib import Path

# --- Configuración vía ENV ---
BASE_DIR = Path(os.getenv("ENEMDU_ROOT", "/data/raw/ANUAL"))
UNPROCESSED_DIR = Path(os.getenv("PERSONA_UNPROC", "/data/enemdu_persona/unprocessed"))
PROCESSED_DIR   = Path(os.getenv("PERSONA_PROCESSED", "/data/enemdu_persona/processed"))

# Asegura que existan ambos directorios
for d in (UNPROCESSED_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Precompilamos ambos patterns
regex_personas = re.compile(r'personas.*\.csv$', re.IGNORECASE)
regex_persona  = re.compile(r'persona(?!s).*\.csv$', re.IGNORECASE)

def match_csv(filename: str, year: int) -> bool:
    """
    True si el archivo debe copiarse según:
      - <=2018: sólo 'personas'
      - ==2019: 'personas' o 'persona'
      - >=2020: sólo 'persona' y no 'tics'
    """
    lower = filename.lower()
    if year <= 2018:
        return bool(regex_personas.search(lower))
    elif year == 2019:
        return bool(regex_personas.search(lower)) or bool(regex_persona.search(lower))
    else:
        return bool(regex_persona.search(lower)) and ('tics' not in lower)

def copiar_csv(src: Path, year: str, period: str):
    nombre_dst = f"{year}_{period.replace(' ', '_')}_{src.name}"
    dst = UNPROCESSED_DIR / nombre_dst
    try:
        shutil.copy(src, dst)
        print(f"   ✔ Copiado: {dst.name}")
    except Exception as e:
        print(f"   ❌ Error copiando {src.name}: {e}")

def extraer_zip_recursivo(zip_path: Path, temp_dir: Path):
    if not zipfile.is_zipfile(zip_path):
        print(f"⚠️  No es ZIP válido → {zip_path.name}")
        return
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(temp_dir)
    except Exception as e:
        print(f"⚠️  Falló extracción de {zip_path.name}: {e}")
        return
    for nested in temp_dir.rglob("*.zip"):
        subdir = nested.with_suffix('')
        subdir.mkdir(parents=True, exist_ok=True)
        extraer_zip_recursivo(nested, subdir)

# --- Lista de ya procesados ---
processed = {p.name for p in PROCESSED_DIR.glob("*.csv")}

# --- Procesamiento principal ---
for year_dir in sorted(BASE_DIR.iterdir()):
    if not year_dir.is_dir(): 
        continue
    year = int(year_dir.name)
    for period_dir in sorted(year_dir.iterdir()):
        if not period_dir.is_dir(): 
            continue
        period = period_dir.name
        print(f"\n📂 Revisando {year}/{period}")

        # 1) CSV sueltos
        for csv_file in period_dir.rglob("*.csv"):
            if csv_file.name in processed:
                print(f"⚠️  Ya procesado (skip): {csv_file.name}")
                continue
            if match_csv(csv_file.name, year):
                copiar_csv(csv_file, year_dir.name, period)

        # 2) Zips anidados
        for zip_file in period_dir.rglob("*.zip"):
            print(f"📦 Procesando ZIP → {zip_file.relative_to(period_dir)}")
            with tempfile.TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                extraer_zip_recursivo(zip_file, tmp_path)
                for csv_ex in tmp_path.rglob("*.csv"):
                    if csv_ex.name in processed:
                        continue
                    if match_csv(csv_ex.name, year):
                        copiar_csv(csv_ex, year_dir.name, period)

print("\n✅ Proceso completado. CSV nuevos en:", UNPROCESSED_DIR)
