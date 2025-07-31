# Script para carga de CSVs en ClickHouse: enemdu_persona
#!/usr/bin/env python3
import os
import time
import pandas as pd
from pathlib import Path
from clickhouse_driver import Client, errors
from datetime import datetime

# ========= Parámetros generales =========
MAX_RETRIES   = int(os.getenv('MAX_RETRIES', 12))
RETRY_DELAY   = int(os.getenv('RETRY_DELAY', 5))  # segundos
DATA_DIR      = os.getenv('DATA_DIR', '/data/enemdu_persona')
LOG_DIR       = os.getenv('LOG_DIR', '/logs')
ERR_DIR       = os.getenv('ERR_DIR', '/errors')
STOP_ON_ERROR = os.getenv('STOP_ON_ERROR', 'false').lower() in ('1', 'true', 'yes')
USE_SENTINELS = os.getenv('USE_SENTINELS', 'false').lower() in ('1', 'true', 'yes')

# ========= Conexión a ClickHouse =========
host     = os.getenv('CH_HOST', 'clickhouse')
port     = int(os.getenv('CH_PORT', 9000))
user     = os.getenv('CH_USER', 'admin')
password = os.getenv('CH_PASSWORD', 'secret_pw')
database = os.getenv('CH_DATABASE', 'indicadores')
table    = os.getenv('CH_TABLE', 'enemdu_persona')

# ========= Utilidades =========
def ensure_dirs():
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    Path(ERR_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts} UTC] {msg}", flush=True)

def _is_float(x: str) -> bool:
    try:
        float(x)
        return True
    except Exception:
        return False

# Mapas para casteo rápido
FLOAT_COLS  = {'fexp','ingrl','ingpc'}
INT_COLS    = {
    'condact','desempleo','empleo','secemp','estrato','nnivins','rama1',
    'vivienda','grupo1','hogar','id_hogar','id_persona','id_vivienda', 'upm',
    'p01','p02','p03','p04','p06','p07','p09','p10a','p10b','p15',
    'p20','p21','p22','p23','p24','p25','p26','p27','p28','p29',
    'p32','p33','p34','p35','p36','p37','p38','p39','p40','p41',
    'p42','p44f','p46','p47a','p47b','p49','p50','p51a','p51b',
    'p51c','p63','p64a','p64b','p65','p66','p67','p68a','p68b',
    'p69','p70a','p70b','p71a','p71b','p72a','p72b','p73a','p73b',
    'p74a','p74b','p75','p76'
}
STRING_COLS = {'area', 'ciudad', 'cod_inf', 'periodo', 'panelm'}

SENTINEL_INT    = -404
SENTINEL_FLOAT  = -404.0
SENTINEL_STRING = "-404"

def coerce_value(col: str, raw):
    s = None if raw is None or (isinstance(raw, float) and pd.isna(raw)) else str(raw)
    if col in FLOAT_COLS:
        if s is None:
            return SENTINEL_FLOAT if USE_SENTINELS else None
        s2 = s.replace(' ', '').replace(',', '.')
        return float(s2) if _is_float(s2) else (SENTINEL_FLOAT if USE_SENTINELS else None)
    if col in INT_COLS:
        if s is None:
            return SENTINEL_INT if USE_SENTINELS else None
        s2 = s.strip()
        if s2.lstrip('-').isdigit():
            try:
                return int(s2)
            except Exception:
                return SENTINEL_INT if USE_SENTINELS else None
        return SENTINEL_INT if USE_SENTINELS else None
    if col in STRING_COLS:
        if s is None or s.strip() == "":
            return SENTINEL_STRING if USE_SENTINELS else None
        if col == 'ciudad':
            # s ya es str, quita espacios y luego rellena
            s = s.strip().zfill(6)
        return s
        
    if s is None or s.strip() == "":
        return SENTINEL_STRING if USE_SENTINELS else None
    return s

def get_ch_client():
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            client = Client(host=host, port=port, user=user, password=password, database=database)
            client.execute('SELECT 1')
            log("[OK] Conectado a ClickHouse")
            return client
        except errors.NetworkError as e:
            last_err = e
            log(f"[WARN] Intento {i+1}/{MAX_RETRIES} fallido, reintentando en {RETRY_DELAY}s… ({e})")
            time.sleep(RETRY_DELAY)
    raise RuntimeError(f"No pude conectar a ClickHouse: {last_err}")

def ensure_db_and_table(client: Client):
    client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    client.execute(f"USE {database}")

def fetch_table_columns(client: Client, db: str, tbl: str):
    rows = client.execute(
        """
        SELECT name, type, position
        FROM system.columns
        WHERE database = %(db)s AND table = %(tbl)s
        ORDER BY position
        """,
        {'db': db, 'tbl': tbl}
    )
    cols = []
    for name, dtype, _ in rows:
        is_nullable = dtype.startswith('Nullable(')
        cols.append((name, dtype, is_nullable))
    return cols

def row_to_insert_values(row_dict, columns_meta):
    return [coerce_value(name, row_dict.get(name, None)) for name, _dtype, _ in columns_meta]

def write_failed_row(file_base: str, header, values):
    out_path = Path(ERR_DIR) / f"{file_base}_failed_rows.csv"
    write_header = not out_path.exists()
    import csv
    with out_path.open('a', newline='', encoding='utf-8') as f:
        w = csv.writer(f, delimiter=';')
        if write_header:
            w.writerow(header)
        w.writerow(values)

# forzar lectura de las columnas string como texto (para conservar ceros iniciales)
string_dtypes = { col: str for col in STRING_COLS }

def main():
    ensure_dirs()

    client = get_ch_client()
    ensure_db_and_table(client)

    columns_meta = fetch_table_columns(client, database, table)
    col_names = [c[0] for c in columns_meta]
    if 'extra' in col_names:
        raise RuntimeError("La tabla aún tiene la columna 'extra'. Actualiza/elimina esa columna del esquema.")
    log(f"Columnas en destino ({database}.{table}): {', '.join(col_names)}")

    for csv_path in Path(DATA_DIR).glob('*.csv'):
        log(f"\nProcesando {csv_path.name} …")
        try:
            df = pd.read_csv(csv_path, sep=';', encoding='utf-8', dtype=string_dtypes, low_memory=False, header=0)
        except Exception as e:
            log(f"[ERROR] No pude leer el CSV {csv_path.name}: {e}")
            continue

        df = df.where(pd.notnull(df), None)
        header_out = col_names

        # Preparamos el batch completo
        batch_values = []
        for _, row in df.iterrows():
            row_dict = {k: row[k] for k in df.columns}
            batch_values.append(tuple(row_to_insert_values(row_dict, columns_meta)))

        total = len(batch_values)
        try:
            # Inserción por archivo en un solo batch
            client.execute(
                f"INSERT INTO {database}.{table} ({', '.join(col_names)}) VALUES",
                batch_values
            )
            log(f"→ Insertadas {total} filas en bloque exitosamente.")
        except Exception as e:
            log(f"[FAIL- BATCH] {csv_path.name}: {e}")
            ok_cnt = 0
            fail_cnt = 0
            # Fallback a inserción por fila para aislar errores
            for idx, values in enumerate(batch_values, start=1):
                try:
                    client.execute(
                        f"INSERT INTO {database}.{table} ({', '.join(col_names)}) VALUES",
                        [values]
                    )
                    ok_cnt += 1
                except Exception as e_row:
                    fail_cnt += 1
                    log(f"[FAIL] {csv_path.name} fila {idx}: {e_row}")
                    try:
                        write_failed_row(csv_path.stem, header_out, list(values))
                    except Exception as e2:
                        log(f"[WARN] No pude escribir fila fallida: {e2}")
                    if STOP_ON_ERROR:
                        log("[STOP_ON_ERROR] Activado. Me detengo en el primer error.")
                        return
            log(f"→ Tras fallback, {ok_cnt} filas OK, {fail_cnt} filas fallidas.")

    log("Proceso completado.")

if __name__ == "__main__":
    main()
