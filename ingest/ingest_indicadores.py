#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────────
# Inserta los indicadores desde los CSV generados a ClickHouse
# ──────────────────────────────────────────────────────────────
import os
import pandas as pd
from clickhouse_driver import Client
from pathlib import Path

# ---------- rutas ----------
OUT_DIR = Path("/resultados")
CSV_NACIONAL = OUT_DIR / "indicadores_enemdu_2007_2025.csv"
CSV_CIUDAD   = OUT_DIR / "indicadores_enemdu_por_ciudad_2007_2025.csv"

# ---------- conexión ClickHouse ----------
client = Client(
    host=os.getenv("CH_HOST", "clickhouse"),
    port=int(os.getenv("CH_PORT", 9000)),
    user=os.getenv("CH_USER", "admin"),              
    password=os.getenv("CH_PASSWORD", "secret_pw"),  
    database=os.getenv("CH_DATABASE", "indicadores") 
)

# ---------- mapeo de columnas ----------
col_map = {
    "Año": "anio",
    "Periodo": "periodo",
    "Mes": "mes",
    "Ciudad": "ciudad",
    "TPG (%)": "tpg",
    "TPB (%)": "tpb",
    "TD (%)": "td",
    "Empleo Total (%)": "empleo_total",
    "Formal (%)": "formal",
    "Informal (%)": "informal",
    "Adecuado (%)": "adecuado",
    "Subempleo (%)": "subempleo",
    "No Remun. (%)": "no_remunerado",
    "Otro No Pleno (%)": "otro_no_pleno",
    "Brecha Adecuado H-M (%)": "brecha_adecuado_hm",
    "Brecha Salarial H-M (%)": "brecha_salarial_hm",
    "NiNi (%)": "nini",
    "Desempleo Juvenil (%)": "desempleo_juvenil",
    "Trabajo Infantil (%)": "trabajo_infantil",
    "Manufactura / Empleo (%)": "manufactura_empleo"
}

# ---------- función de carga ----------
def cargar_csv(path: Path, tabla: str, tiene_ciudad=False):
    print(f"▶ Cargando {path.name} a la tabla {tabla}...")
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.rename(columns=col_map, inplace=True)
    if not tiene_ciudad:
        df = df[[c for c in col_map.values() if c != "ciudad"]]
    else:
        df = df[[c for c in col_map.values() if c in df.columns]]

    client.execute(
        f"INSERT INTO {tabla} ({', '.join(df.columns)}) VALUES",
        df.to_dict("records")
    )
    print(f"✅ Insertadas {len(df)} filas en {tabla}.")

# ---------- ejecutar carga ----------
if __name__ == "__main__":
    cargar_csv(CSV_NACIONAL, "indicadores_nacionales", tiene_ciudad=False)
    cargar_csv(CSV_CIUDAD, "indicadores_por_ciudad", tiene_ciudad=True)