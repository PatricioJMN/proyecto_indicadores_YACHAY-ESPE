#!/usr/bin/env python3
# limpieza_vivienda.py
# Extrae y copia sólo CSV nuevos de vivienda a unprocessed, omitiendo los que ya existan en processed o unprocessed.

import os
import re
import shutil
import zipfile
import subprocess
import tempfile
import zlib
from pathlib import Path

# ─── CONFIGURACIÓN vía ENV ───
BASE_DIR = Path(os.getenv("ENEMDU_ROOT", "/data/raw/ANUAL"))
UNPROCESSED_DIR = Path(os.getenv("VIVIENDA_UNPROC", "/data/enemdu_vivienda/unprocessed"))
PROCESSED_DIR = Path(os.getenv("VIVIENDA_PROCESSED", "/data/enemdu_vivienda/processed"))

# Asegura existencia de carpetas
for d in (UNPROCESSED_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Patrón para CSV de vivienda
regex_viv = re.compile(r'(vivienda|viv).*\.csv$', re.IGNORECASE)

def match_csv_viv(name: str) -> bool:
    low = name.lower()
    return bool(regex_viv.search(low)) and 'bdd' not in low and 'tics' not in low

# Lista de raw procesados (nombres sin prefijo)
processed_raw = {p.name for p in PROCESSED_DIR.glob("*.csv")}

def extraer(zp: Path, out: Path):
    """Intenta con zipfile, si falla usa unzip; procesa zips anidados."""
    if not zipfile.is_zipfile(zp):
        print(f"⚠️  No es ZIP válido: {zp.name}")
        return
    try:
        with zipfile.ZipFile(zp, "r") as zf:
            zf.extractall(out)
    except (zipfile.BadZipFile, zlib.error) as e:
        print(f"⚠️  zipfile.extractall falló en {zp.name}: {e}. Usando unzip...")
        subprocess.run(
            ["unzip", "-o", str(zp), "-d", str(out)],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    for nested in out.rglob("*"):
        if nested.is_file() and nested.suffix.lower() == ".zip":
            deeper = nested.with_suffix("")
            deeper.mkdir(parents=True, exist_ok=True)
            extraer(nested, deeper)

# Recorre años/meses
for year_dir in sorted(BASE_DIR.iterdir()):
    if not year_dir.is_dir():
        continue
    year = year_dir.name
    for period_dir in sorted(year_dir.iterdir()):
        if not period_dir.is_dir():
            continue
        period = period_dir.name
        print(f"\n📂 Procesando {year}/{period}")

        # 1) CSV sueltos
        for candidate in period_dir.rglob("*.csv"):
            name = candidate.name
            if name in processed_raw:
                print(f"– Skip (ya en processed): {name}")
                continue
            dst_name = f"{year}_{period.replace(' ','_')}_{name}"
            dst_path = UNPROCESSED_DIR / dst_name
            if dst_path.exists():
                print(f"– Skip (ya en unprocessed): {dst_name}")
                continue
            if match_csv_viv(name):
                shutil.copy(candidate, dst_path)
                print(f"✔ Copiado: {dst_name}")

        # 2) Zips y su contenido
        for zip_file in period_dir.rglob("*.zip"):
            print(f"📦 ZIP → {zip_file.relative_to(period_dir)}")
            with tempfile.TemporaryDirectory() as tmpd:
                tmp_path = Path(tmpd)
                extraer(zip_file, tmp_path)
                for extracted in tmp_path.rglob("*.csv"):
                    name = extracted.name
                    if name in processed_raw:
                        continue
                    dst_name = f"{year}_{period.replace(' ','_')}_{name}"
                    dst_path = UNPROCESSED_DIR / dst_name
                    if dst_path.exists():
                        continue
                    if match_csv_viv(name):
                        shutil.copy(extracted, dst_path)
                        print(f"✔ Copiado desde ZIP: {dst_name}")

print("\n✅ limpieza_vivienda completada.")
