#!/usr/bin/env python3
# =========================================================
# Calcula indicadores ENEMDU y los sube automáticamente a
# ClickHouse (nacional + por ciudad)
# =========================================================
from __future__ import annotations
import os, re, csv, warnings, sys, subprocess, textwrap
from pathlib import Path
from typing import Dict, List
import pandas as pd
from clickhouse_driver import Client

# ---------- rutas ----------
ROOT    = Path("/data")
OUT_DIR = Path("/resultados")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- conexión ClickHouse ----------
client = Client(
    host=os.getenv("CH_HOST", "clickhouse"),
    port=int(os.getenv("CH_PORT", 9000)),
    user=os.getenv("CH_USER", "default"),
    password=os.getenv("CH_PASSWORD", ""),
    database=os.getenv("CH_DATABASE", "enemdu")
)

# ---------- diccionarios y columnas (idénticos a antes) ----------
MONTH = {f"{m:02}": n for m, n in enumerate(
    ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
     "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"], 1)}

AGE_C, SEXO_C, STAT_C = ["p03","edad"], ["p02","sexo"], ["condact","condact3"]
WEIG_C = ["fexp","fexp_r","facexp","factor_expansion","peso","peso_2020"]
SECEMP = ["secemp"]; SIZE_C, RUC_C, DOM_C = ["p47a"], ["p49"], ["p42"]
STUD_C = ["p07","p07a","p07b","p07_1","asiste","estudia","asis_esc"]
INGR_C = ["ingrl"]
HOR_C  = ["horas","p24","p24a","p24b"]
ACT1_C = ["rama1","ramacciu","rama_1","rama_1dig"]
CITY_C = ["ciudad","ciuc","ubica_ciu","codigo_ciudad"]

YEAR_RX   = re.compile(r"(\d{4})\D?(\d{2})")
_safe_pct = lambda n,d: round(100*n/d,2) if d and d>0 else float("nan")

# ---------- utilidades rápidas ----------
def _pick(df: pd.DataFrame, cands: List[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in cols: return cols[c.lower()]
    for pref in cands:
        for low, orig in cols.items():
            if low.startswith(pref.lower()): return orig
    raise KeyError

def _wclean(s: pd.Series) -> pd.Series:
    return (s.astype(str).str.replace(",",".",regex=False)
            .str.extract(r"([\d\.]+)")[0].astype(float))

def _secemp(row,size,ruc,dom):
    if row[dom]==10: return 3
    if row[size]==2: return 1
    if row[size]==1: return 1 if row[ruc]==1 else 2
    return 4

# ---------- cálculo de indicadores (mismo que antes) ----------
def indicadores(df: pd.DataFrame) -> Dict[str,float]:
    df.columns = df.columns.str.strip().str.lower()
    edad, sexo, stat, peso = map(lambda c:_pick(df,c),
                                 (AGE_C,SEXO_C,STAT_C,WEIG_C))
    ingreso = _pick(df, INGR_C)

    # opcionales
    try: estud = _pick(df, STUD_C); has_study=True
    except KeyError: has_study=False
    try: horas = _pick(df, HOR_C); has_hora=True
    except KeyError: has_hora=False
    try: act1  = _pick(df, ACT1_C); has_act=True
    except KeyError: has_act=False

    for c in (edad, sexo, stat):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[peso] = _wclean(df[peso])
    df[ingreso]=pd.to_numeric(df[ingreso], errors="coerce")
    if has_hora:
        df[horas]=pd.to_numeric(df[horas], errors="coerce")
    df.loc[df[ingreso] >= 999_999, ingreso] = pd.NA
    df.loc[df[ingreso] <= 0,        ingreso] = pd.NA

    if has_study:
        df["_estu_txt"]=df[estud].astype(str).str.strip().str.lower()
        df["_estu_cod"]=pd.to_numeric(df[estud], errors="coerce")
    else:
        df["_estu_txt"],df["_estu_cod"]=" ",float("nan")

    pet  = df[(df[edad]>=15)&(df[peso]>0)]
    pea  = pet[pet[stat].between(1,8)]
    ocup = pea[pea[stat].between(1,6)].copy()

    if any(c in df.columns.str.lower() for c in SECEMP):
        s=_pick(df,SECEMP)
        ocup["_s"]=pd.to_numeric(ocup[s],errors="coerce").fillna(4).astype(int)
    else:
        size,ruc,dom=map(lambda c:_pick(df,c),(SIZE_C,RUC_C,DOM_C))
        for c in (size,ruc,dom): ocup[c]=pd.to_numeric(ocup[c],errors="coerce")
        ocup["_s"]=ocup.apply(_secemp, axis=1, size=size, ruc=ruc, dom=dom)

    Wpop,Wpet,Wpea,Wocc = df[peso].sum(), pet[peso].sum(), pea[peso].sum(), ocup[peso].sum()
    formal_w   = ocup.loc[ocup["_s"]==1, peso].sum()
    informal_w = ocup.loc[ocup["_s"]==2, peso].sum()

    out = {
        "TPG (%)":            _safe_pct(Wpea,Wpet),
        "TPB (%)":            _safe_pct(Wpea,Wpop),
        "TD (%)":             _safe_pct(pea.loc[pea[stat].isin([7,8]), peso].sum(), Wpea),
        "Empleo Total (%)":   _safe_pct(Wocc,Wpea),
        "Formal (%)":         _safe_pct(formal_w,Wocc),
        "Informal (%)":       _safe_pct(informal_w,Wocc),
        "Adecuado (%)":       _safe_pct(ocup.loc[ocup[stat]==1, peso].sum(), Wpea),
        "Subempleo (%)":      _safe_pct(ocup.loc[ocup[stat].isin([2,3]), peso].sum(), Wpea),
        "No Remun. (%)":      _safe_pct(ocup.loc[ocup[stat]==5, peso].sum(), Wpea),
        "Otro No Pleno (%)":  _safe_pct(ocup.loc[ocup[stat]==4, peso].sum(), Wpea),
    }

    pea_h, pea_m = pea.loc[pea[sexo]==1,peso].sum(), pea.loc[pea[sexo]==2,peso].sum()
    ade_h = ocup.loc[(ocup[sexo]==1)&(ocup[stat]==1), peso].sum()
    ade_m = ocup.loc[(ocup[sexo]==2)&(ocup[stat]==1), peso].sum()
    out["Brecha Adecuado H-M (%)"] = _safe_pct((ade_h/pea_h)-(ade_m/pea_m), ade_h/pea_h) if pea_h else float("nan")

    occ_h=ocup.loc[(ocup[sexo]==1) & (ocup[ingreso].notna())]
    occ_m=ocup.loc[(ocup[sexo]==2) & (ocup[ingreso].notna())]
    if len(occ_h) and len(occ_m):
        mean_h=(occ_h[ingreso]*occ_h[peso]).sum()/occ_h[peso].sum()
        mean_m=(occ_m[ingreso]*occ_m[peso]).sum()/occ_m[peso].sum()
        out["Brecha Salarial H-M (%)"]=_safe_pct(mean_h-mean_m, mean_h)
    else:
        out["Brecha Salarial H-M (%)"]=float("nan")

    if has_study:
        juv=df[df[edad].between(15,24)]
        no_est=(juv["_estu_cod"]==2)|juv["_estu_txt"].isin({"no","0","n","ninguno",""})
        no_trab=juv[stat].isin([7,8,9])
        out["NiNi (%)"]=_safe_pct(juv.loc[no_est&no_trab,peso].sum(), juv[peso].sum())
    else:
        out["NiNi (%)"]=float("nan")

    juv_pea=pea[pea[edad].between(18,29)]
    juv_des=juv_pea.loc[juv_pea[stat].isin([7,8]), peso].sum()
    out["Desempleo Juvenil (%)"]=_safe_pct(juv_des, juv_pea[peso].sum())

    niños=df[df[edad].between(5,14)]
    ti_mask=niños[stat].between(1,6)
    if has_hora:
        ti_mask|=(niños[horas].fillna(0)>0)
    out["Trabajo Infantil (%)"]=_safe_pct(niños.loc[ti_mask,peso].sum(), niños[peso].sum())

    if has_act:
        manu_mask=ocup[act1].astype(str).str.strip().isin({"3"})
        out["Manufactura / Empleo (%)"]=_safe_pct(ocup.loc[manu_mask,peso].sum(), Wocc)
    else:
        out["Manufactura / Empleo (%)"]=float("nan")

    return out

# ───────────── 5. Recorre CSV y compila resultados ────────────
rows_nac: List[Dict[str,object]]=[]
rows_city:List[Dict[str,object]]=[]

for f in ROOT.rglob("*.csv"):
    if "persona" not in f.stem.lower(): continue
    m=YEAR_RX.search(f.stem)
    if not m: continue
    year, period = m.groups()

    with open(f,"r",encoding="latin1") as fh:
        sample = "".join(fh.readline() for _ in range(5))
        delim  = csv.Sniffer().sniff(sample, delimiters=";,").delimiter

    try:
        df = pd.read_csv(f, delimiter=delim, encoding="latin1", low_memory=False)
        rows_nac.append({
            "Año":int(year), "Periodo":int(period), "Mes":MONTH[period], **indicadores(df)
        })

        try:
            city_col=_pick(df,CITY_C)
            for city, sub in df.groupby(city_col):
                rows_city.append({
                    "Año":int(year), "Periodo":int(period), "Mes":MONTH[period],
                    "Ciudad":str(city).zfill(6), **indicadores(sub)
                })
        except KeyError:
            pass

    except Exception as e:
        warnings.warn(f"{f.name}: {e}")

# ---------- 6. Guarda CSV (por si quieres descargarlos) ----------
df_nac  = pd.DataFrame(rows_nac )
df_city = pd.DataFrame(rows_city)

file_nac  = OUT_DIR / "indicadores_enemdu_2007_2025.csv"
file_city = OUT_DIR / "indicadores_enemdu_por_ciudad_2007_2025.csv"
df_nac.to_csv(file_nac,  index=False, encoding="utf-8-sig")
df_city.to_csv(file_city, index=False, encoding="utf-8-sig")

print(f"✅ CSV nacional: {file_nac}  ({len(df_nac)} filas)")
print(f"✅ CSV ciudad  : {file_city}  ({len(df_city)} filas)")

