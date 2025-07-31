# Script para carga de CSVs en ClickHouse: codigos_vivienda_inec
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
LOG_DIR       = os.getenv('LOG_DIR', '/logs')
ERR_DIR       = os.getenv('ERR_DIR', '/errors')
STOP_ON_ERROR = os.getenv('STOP_ON_ERROR', 'false').lower() in ('1', 'true', 'yes')

# Parámetros específicos para codigos_vivienda_inec
DATA_DIR      = os.getenv('CODIGOS_DIR', '/data/codigos_vivienda_inec')
DATABASE      = os.getenv('CH_DATABASE', 'indicadores')
TABLE         = os.getenv('CH_TABLE', 'codigos_vivienda_inec')

# Todas las columnas son strings en este caso
def ensure_dirs():
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    Path(ERR_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ts=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts} UTC] {msg}", flush=True)

def get_ch_client():
    last_err=None
    for i in range(MAX_RETRIES):
        try:
            client=Client(host=os.getenv('CH_HOST','clickhouse'), port=int(os.getenv('CH_PORT',9000)),
                          user=os.getenv('CH_USER','admin'), password=os.getenv('CH_PASSWORD','secret_pw'),
                          database=DATABASE)
            client.execute('SELECT 1')
            log("[OK] Conectado a ClickHouse")
            return client
        except errors.NetworkError as e:
            last_err=e; log(f"[WARN] Intento {i+1}/{MAX_RETRIES} fallido: {e}"); time.sleep(RETRY_DELAY)
    raise RuntimeError(f"No pude conectar: {last_err}")

def write_failed_row(base, header, values):
    out=Path(ERR_DIR)/f"{base}_failed.csv"
    write_header=not out.exists()
    import csv
    with out.open('a', newline='', encoding='utf-8') as f:
        w=csv.writer(f, delimiter=';')
        if write_header: w.writerow(header)
        w.writerow(values)

# Procesar CSVs
def main():
    ensure_dirs()
    client=get_ch_client()
    client.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    client.execute(f"USE {DATABASE}")
    # Obtener esquema destino
    cols_meta=client.execute("SELECT name FROM system.columns WHERE database=%(db)s AND table=%(tbl)s ORDER BY position", {'db':DATABASE,'tbl':TABLE})
    col_names=[r[0] for r in cols_meta]
    log(f"Columnas destino {TABLE}: {col_names}")

    for csvf in Path(DATA_DIR).glob('*.csv'):
        log(f"Procesando {csvf.name}...")
        try:
            df=pd.read_csv(csvf, sep=';', dtype={c:str for c in col_names}, low_memory=False)
        except Exception as e:
            log(f"[ERROR] Lectura {csvf.name}: {e}"); continue
        df=df.where(pd.notnull(df), None)
        batch=[]
        for _,row in df.iterrows():
            batch.append([row.get(c) for c in col_names])
        try:
            client.execute(f"INSERT INTO {DATABASE}.{TABLE} ({','.join(col_names)}) VALUES", batch)
            log(f"→ Insertadas {len(batch)} filas.")
        except Exception as e:
            log(f"[FAIL BATCH] {e}")
            ok=0; fail=0
            for i,vals in enumerate(batch,1):
                try:
                    client.execute(f"INSERT INTO {DATABASE}.{TABLE} ({','.join(col_names)}) VALUES", [vals]); ok+=1
                except Exception as ex:
                    fail+=1; log(f"[FAIL fila {i}] {ex}"); write_failed_row(f"{TABLE}_{csvf.stem}", col_names, vals)
                    if STOP_ON_ERROR: return
            log(f"→ {ok} OK, {fail} fallidas.")
    log("Proceso completado codigos_vivienda_inec.")

if __name__=='__main__': main()
