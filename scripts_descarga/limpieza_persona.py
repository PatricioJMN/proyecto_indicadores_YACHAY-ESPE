#!/usr/bin/env python3
# limpieza_persona.py
# Extrae y copia sólo CSV nuevos a unprocessed (omitiendo los que ya existan en processed).
# Ahora case-insensitive para .zip y .csv

import os
import re
import shutil
import zipfile
import subprocess
import tempfile
import zlib
from pathlib import Path

# ─── CONFIGURACIÓN vía ENV ───
BASE_DIR        = Path(os.getenv("ENEMDU_ROOT",      "/app/data/raw/ANUAL"))
UNPROCESSED_DIR = Path(os.getenv("PERSONA_UNPROC",   "/app/data/enemdu_persona/unprocessed"))
PROCESSED_DIR   = Path(os.getenv("PERSONA_PROCESSED","/app/data/enemdu_persona/processed"))

for d in (UNPROCESSED_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

regex_personas = re.compile(r'personas.*\.csv$', re.IGNORECASE)
regex_persona  = re.compile(r'persona(?!s).*\.csv$', re.IGNORECASE)

def match_csv(name: str, year: int) -> bool:
    low = name.lower()
    if year <= 2018:
        return bool(regex_personas.search(low))
    if year == 2019:
        return bool(regex_personas.search(low)) or bool(regex_persona.search(low))
    return bool(regex_persona.search(low)) and 'tics' not in low

# lee nombres de CSV ya procesados
processed_files = {p.name for p in PROCESSED_DIR.glob("*") if p.is_file() and p.suffix.lower() == ".csv"}

def extraer(zp: Path, out: Path):
    """Intenta con zipfile, si falla use unzip; luego busca zips anidados case-insensitive."""
    if not zipfile.is_zipfile(zp):
        print(f"⚠️  No es ZIP válido: {zp.name}")
        return
    try:
        with zipfile.ZipFile(zp, "r") as zf:
            zf.extractall(out)
    except (zipfile.BadZipFile, zlib.error) as e:
        print(f"⚠️  zipfile.extractall falló en {zp.name}: {e}. Usando unzip...")
        try:
            subprocess.run(
                ["unzip", "-o", str(zp), "-d", str(out)],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        except subprocess.CalledProcessError as e2:
            print(f"❌  unzip falló en {zp.name}: {e2}")
            return
    # zips anidados (.zip, .ZIP, etc)
    for nested in out.rglob("*"):
        if nested.is_file() and nested.suffix.lower() == ".zip":
            nested_out = nested.with_suffix("")
            nested_out.mkdir(parents=True, exist_ok=True)
            extraer(nested, nested_out)

# Recorre años/meses
for year_dir in sorted(BASE_DIR.iterdir()):
    if not year_dir.is_dir(): continue
    year = int(year_dir.name)
    for period_dir in sorted(year_dir.iterdir()):
        if not period_dir.is_dir(): continue
        period = period_dir.name
        print(f"\n📂 Procesando {year}/{period}")

        # 1) CSV sueltos (case-insensitive)
        for candidate in period_dir.rglob("*"):
            if not candidate.is_file() or candidate.suffix.lower() != ".csv":
                continue
            name = candidate.name
            if name in processed_files:
                print(f"- Skip (ya en processed): {name}")
                continue
            if match_csv(name, year):
                dst = UNPROCESSED_DIR / f"{year}_{period.replace(' ','_')}_{name}"
                shutil.copy(candidate, dst)
                print(f"✔ Copiado: {dst.name}")

        # 2) Zips (case-insensitive) y su contenido
        for candidate in period_dir.rglob("*"):
            if not candidate.is_file() or candidate.suffix.lower() != ".zip":
                continue
            print(f"📦 ZIP → {candidate.relative_to(period_dir)}")
            with tempfile.TemporaryDirectory() as tmpd:
                tmp_path = Path(tmpd)
                extraer(candidate, tmp_path)
                # CSV en extraídos
                for extracted in tmp_path.rglob("*"):
                    if not extracted.is_file() or extracted.suffix.lower() != ".csv":
                        continue
                    name = extracted.name
                    if name in processed_files:
                        continue
                    if match_csv(name, year):
                        dst = UNPROCESSED_DIR / f"{year}_{period.replace(' ','_')}_{name}"
                        shutil.copy(extracted, dst)
                        print(f"✔ Copiado desde ZIP: {dst.name}")

print("\n✅ limpieza_persona completada.")
