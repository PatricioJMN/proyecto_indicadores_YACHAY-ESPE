#!/usr/bin/env python3
# =========================================================
# Calcula indicadores ENEMDU y los sube automáticamente a
# ClickHouse (nacional + por ciudad)
# =========================================================
from pathlib import Path
import re, warnings, csv
import pandas as pd
import numpy as np

# ---------- rutas ----------
ROOT    = Path("/data/enemdu_persona/processed")
OUT_DIR = Path("/data/resultados")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- constantes ----------
MONTH = {f"{m:02}": n for m, n in enumerate(
    ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
     "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"], 1)}

YEAR_RX = re.compile(r"(\d{4})\D?(\d{2})")
_safe_pct = lambda n,d: (n*100/d).round(2) if d>0 else np.nan

# Configuración de lectura de CSV
CSV_READ_KW = dict(encoding="latin1", low_memory=False)

# ---------- utilidades ----------
def detect_delim(path):
    sample = path.read_text(encoding="latin1", errors="ignore").splitlines()[:3]
    return csv.Sniffer().sniff("\n".join(sample), delimiters=";,").delimiter

def pick(cols_lower, cands):
    for c in cands:
        if c in cols_lower: return cols_lower[c]
    for pref in cands:
        for low, orig in cols_lower.items():
            if low.startswith(pref): return orig
    raise KeyError(f"No pude hallar columna para {cands}")

# Vectorización de `_secemp`
def compute_sector(df, size_col, ruc_col, dom_col):
    s = pd.to_numeric(df[size_col], errors="coerce")
    r = pd.to_numeric(df[ruc_col], errors="coerce")
    d = pd.to_numeric(df[dom_col], errors="coerce")
    # Precrea serie con default=4
    sec = pd.Series(4, index=df.index)
    sec[d==10] = 3
    mask_size2 = (s==2)
    sec[mask_size2] = 1
    mask_size1 = (s==1) & (r==1)
    sec[mask_size1] = 1
    mask_size1_r0 = (s==1) & (r!=1)
    sec[mask_size1_r0] = 2
    return sec.astype(int)

def indicadores(df_orig):
    # Lowercase cols y detect cols clave
    df = df_orig.rename(columns=lambda c: c.strip().lower())
    cols = {c: c for c in df.columns}
    edad_col = pick(cols, ["p03","edad"])
    sexo_col = pick(cols, ["p02","sexo"])
    stat_col = pick(cols, ["condact","condact3"])
    peso_col = pick(cols, ["fexp","peso","factor_expansion","peso_2020"])
    ingreso_col = pick(cols, ["ingrl"])
    # opcionales
    estud_col = pick(cols, ["p07","asiste"],) if any(k in cols for k in ["p07","asiste"]) else None
    horas_col = pick(cols, ["horas","p24"])        if any(k in cols for k in ["horas","p24"]) else None
    act1_col  = pick(cols, ["rama1","ramacciu"])  if any(k in cols for k in ["rama1","ramacciu"]) else None
    city_col  = pick(cols, ["ciudad","ciuc"])     if any(k in cols for k in ["ciudad","ciuc"]) else None

    # Casting y limpieza
    df[edad_col] = pd.to_numeric(df[edad_col], errors="coerce")
    df[sexo_col] = pd.to_numeric(df[sexo_col], errors="coerce")
    df[stat_col] = pd.to_numeric(df[stat_col], errors="coerce")
    df[peso_col] = (df[peso_col].astype(str)
                        .str.replace(",",".",False)
                        .str.extract(r"([\d\.]+)")[0]
                        .astype(float))
    df[ingreso_col] = pd.to_numeric(df[ingreso_col], errors="coerce")
    df.loc[df[ingreso_col]>=999999, ingreso_col] = np.nan
    df.loc[df[ingreso_col]<=0,      ingreso_col] = np.nan
    if estud_col:
        df["_estu_cod"] = pd.to_numeric(df[estud_col], errors="coerce")
    if horas_col:
        df[horas_col] = pd.to_numeric(df[horas_col], errors="coerce")

    # Máscaras
    pet_m  = (df[edad_col]>=15) & (df[peso_col]>0)
    pea_m  = pet_m & df[stat_col].between(1,8)
    occ_m  = pea_m & df[stat_col].between(1,6)

    pet  = df[pet_m]
    pea  = df[pea_m]
    ocup = df[occ_m].copy()

    # Sector empleo
    if "secemp" in df.columns:
        sec_col = pick(cols, ["secemp"])
        ocup["_s"] = pd.to_numeric(ocup[sec_col], errors="coerce").fillna(4).astype(int)
    else:
        size_col = pick(cols, ["p47a"])
        ruc_col  = pick(cols, ["p49"])
        dom_col  = pick(cols, ["p42"])
        ocup["_s"] = compute_sector(ocup, size_col, ruc_col, dom_col)

    # Pesos totales
    Wpop = df[peso_col].sum()
    Wpet = pet[peso_col].sum()
    Wpea = pea[peso_col].sum()
    Wocc = ocup[peso_col].sum()

    # Agregados por máscara
    suma = lambda mask: (ocup.loc[mask, peso_col].sum())
    formal_w   = suma(ocup["_s"]==1)
    informal_w = suma(ocup["_s"]==2)

    # Diccionario de resultados
    out = {
        "TPG (%)":          _safe_pct(Wpea, Wpet),
        "TPB (%)":          _safe_pct(Wpea, Wpop),
        "TD (%)":           _safe_pct(pea.loc[pea[stat_col].isin([7,8]), peso_col].sum(), Wpea),
        "Empleo Total (%)": _safe_pct(Wocc, Wpea),
        "Formal (%)":       _safe_pct(formal_w, Wocc),
        "Informal (%)":     _safe_pct(informal_w, Wocc),
        "Adecuado (%)":     _safe_pct(suma(ocup[stat_col]==1), Wpea),
        "Subempleo (%)":    _safe_pct(suma(ocup[stat_col].isin([2,3])), Wpea),
        "No Remun. (%)":    _safe_pct(suma(ocup[stat_col]==5), Wpea),
        "Otro No Pleno (%)":_safe_pct(suma(ocup[stat_col]==4), Wpea),
    }

        # --- Brecha Adecuado H-M (%)
    pea_h = pea.loc[pea[sexo_col] == 1, peso_col].sum()
    pea_m = pea.loc[pea[sexo_col] == 2, peso_col].sum()
    ade_h = ocup.loc[(ocup[sexo_col] == 1) & (ocup[stat_col] == 1), peso_col].sum()
    ade_m = ocup.loc[(ocup[sexo_col] == 2) & (ocup[stat_col] == 1), peso_col].sum()
    if pea_h > 0 and pea_m > 0:
        brecha_ade = (ade_h / pea_h) - (ade_m / pea_m)
        out["Brecha Adecuado H-M (%)"] = _safe_pct(brecha_ade, ade_h / pea_h)
    else:
        out["Brecha Adecuado H-M (%)"] = np.nan

    # --- Brecha Salarial H-M (%)
    occ_h = ocup.loc[(ocup[sexo_col] == 1) & ocup[ingreso_col].notna()]
    occ_m = ocup.loc[(ocup[sexo_col] == 2) & ocup[ingreso_col].notna()]
    if not occ_h.empty and not occ_m.empty:
        mean_h = (occ_h[ingreso_col] * occ_h[peso_col]).sum() / occ_h[peso_col].sum()
        mean_m = (occ_m[ingreso_col] * occ_m[peso_col]).sum() / occ_m[peso_col].sum()
        out["Brecha Salarial H-M (%)"] = _safe_pct(mean_h - mean_m, mean_h)
    else:
        out["Brecha Salarial H-M (%)"] = np.nan

    # --- NiNi juvenil (%)
    if estud_col:
        juv = df[df[edad_col].between(15, 24)]
        no_est = (juv["_estu_cod"] == 2) | juv[estud_col].astype(str).str.lower().isin({"no","0","n","ninguno",""})
        no_trab = juv[stat_col].isin([7, 8, 9])
        out["NiNi (%)"] = _safe_pct(juv.loc[no_est & no_trab, peso_col].sum(), juv[peso_col].sum())
    else:
        out["NiNi (%)"] = np.nan

    # --- Desempleo Juvenil (%)
    juv_pea = pea[pea[edad_col].between(18, 29)]
    juv_des = juv_pea.loc[juv_pea[stat_col].isin([7, 8]), peso_col].sum()
    out["Desempleo Juvenil (%)"] = _safe_pct(juv_des, juv_pea[peso_col].sum())

    # --- Trabajo Infantil (%)
    niños = df[df[edad_col].between(5, 14)]
    ti_mask = niños[stat_col].between(1, 6)
    if horas_col:
        ti_mask |= (niños[horas_col].fillna(0) > 0)
    out["Trabajo Infantil (%)"] = _safe_pct(niños.loc[ti_mask, peso_col].sum(), niños[peso_col].sum())

    # --- Manufactura / Empleo (%)
    if act1_col:
        manu_mask = ocup[act1_col].astype(str).str.strip() == "3"
        out["Manufactura / Empleo (%)"] = _safe_pct(ocup.loc[manu_mask, peso_col].sum(), Wocc)
    else:
        out["Manufactura / Empleo (%)"] = np.nan

    return out

# ───────────── 5. Recorre CSV y compila resultados ────────────
rows_nac, rows_city = [], []

for f in ROOT.rglob("*.csv"):
    if "persona" not in f.stem.lower(): continue
    m = YEAR_RX.search(f.stem)
    if not m: continue
    year, period = m.groups()
    try:
        delim = detect_delim(f)
        df = pd.read_csv(f, sep=delim, **CSV_READ_KW)
        ind_nac = indicadores(df)
        rows_nac.append({"Año":int(year), "Periodo":int(period), "Mes":MONTH[period], **ind_nac})

        # Por ciudad
        try:
            city_col = pick({c:c for c in df.columns}, ["ciudad","ciuc"])
            for city, sub in df.groupby(city_col):
                rows_city.append({
                    "Año":int(year),"Periodo":int(period),"Mes":MONTH[period],
                    "Ciudad":str(city).zfill(6),
                    **indicadores(sub)
                })
        except KeyError:
            pass

    except Exception as e:
        warnings.warn(f"{f.name}: {e}")

# ───────────── Guarda CSV ─────────────
pd.DataFrame(rows_nac).to_csv(OUT_DIR/"indicadores_nac.csv", index=False)
pd.DataFrame(rows_city).to_csv(OUT_DIR/"indicadores_ciudad.csv", index=False)