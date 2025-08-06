#!/usr/bin/env python3
# limpieza_vivienda.py
# Descomprime zips anidados y copia CSV de vivienda sólo si NO existen en processed
# Basado en código original :contentReference[oaicite:3]{index=3}

import os
import re
import shutil
import zipfile
import tempfile
from pathlib import Path

# ─── CONFIGURACIÓN vía ENV ───
BASE_DIR        = Path(os.getenv("ENEMDU_ROOT", "/data/ANUAL"))
UNPROCESSED_DIR = Path(os.getenv("VIVIENDA_UNPROC", "/data/enemdu_vivienda/unprocessed"))
PROCESSED_DIR   = Path(os.getenv("VIVIENDA_PROCESSED", "/data/enemdu_vivienda/processed"))

# Asegura que existan ambos dirs
for d in (UNPROCESSED_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

vivienda_regex = re.compile(r'(vivienda|viv).*\.csv$', re.IGNORECASE)

def match_csv_vivienda(filename: str) -> bool:
    """
    True si es CSV de vivienda y NO contiene 'bdd' ni 'tics'
    """
    lower = filename.lower()
    if not vivienda_regex.search(lower):
        return False
    if 'bdd' in lower or 'tics' in lower:
        return False
    return True

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
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_dir)
    except Exception as e:
        print(f"⚠️  Falló extracción de {zip_path.name}: {e}")
        return
    for nested in temp_dir.rglob('*.zip'):
        subdir = nested.with_suffix('')
        subdir.mkdir(parents=True, exist_ok=True)
        extraer_zip_recursivo(nested, subdir)

# ─── Lista de ya procesados ───
processed = {p.name for p in PROCESSED_DIR.glob("*.csv")}

# ─── Procesamiento ───
for year_dir in sorted(BASE_DIR.iterdir()):
    if not year_dir.is_dir():
        continue
    year = year_dir.name
    for period_dir in sorted(year_dir.iterdir()):
        if not period_dir.is_dir():
            continue
        period = period_dir.name
        print(f"\n📂 Revisando {year}/{period}")

        # CSV sueltos
        for csv_file in period_dir.rglob('*.csv'):
            if csv_file.name in processed:
                print(f"⚠️  Ya procesado (skip): {csv_file.name}")
                continue
            if match_csv_vivienda(csv_file.name):
                copiar_csv(csv_file, year, period)

        # Zips y anidados
        for zip_file in period_dir.rglob('*.zip'):
            print(f"📦 Procesando ZIP → {zip_file.relative_to(period_dir)}")
            with tempfile.TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                extraer_zip_recursivo(zip_file, tmp_path)
                for csv_ex in tmp_path.rglob('*.csv'):
                    if csv_ex.name in processed:
                        continue
                    if match_csv_vivienda(csv_ex.name):
                        copiar_csv(csv_ex, year, period)

print("\n✅ Proceso completado. CSV nuevos de vivienda en:", UNPROCESSED_DIR)
