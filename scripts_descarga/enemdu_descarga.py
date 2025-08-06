# enemdu_actualizador_mensual.py
# Descarga SOLO los períodos que aún no existen localmente.
# Estructura: <ROOT>/<año>/<periodo>/(modal_n)/archivos…

import re, time, os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

# ────────── CONFIG ──────────
ROOT = os.getenv("ENEMDU_ROOT", "/data/raw/ANUAL")
os.makedirs(ROOT, exist_ok=True)

opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_experimental_option("prefs", {
    "download.default_directory": str(ROOT),
    "profile.default_content_settings.popups": 0,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "profile.default_content_setting_values.automatic_downloads": 1,
})
drv  = webdriver.Chrome(options=opt)
wait = WebDriverWait(drv, 25, poll_frequency=0.10)
slug = lambda s: re.sub(r"\W+", "_", s).strip("_")
MASK = (By.CSS_SELECTOR, "div.ui-widget-overlay.ui-dialog-mask")

# ────────── UTILIDADES ──────────
def wait_mask_off():
    try:
        WebDriverWait(drv, 25, 0.10).until(EC.invisibility_of_element_located(MASK))
    except TimeoutException:
        pass

def set_dir(p: Path):
    os.makedirs(p, exist_ok=True)
    drv.execute_cdp_cmd("Page.setDownloadBehavior",
                        {"behavior": "allow", "downloadPath": str(p)})

def close_modal():
    try:
        btn = wait.until(EC.element_to_be_clickable(
              (By.XPATH, "//a[contains(@class,'ui-dialog-titlebar-close')]")))
        drv.execute_script("arguments[0].click();", btn)
        time.sleep(1)
    except Exception:
        pass

def dl_modal(dst: Path, idx: int):
    """Descarga TODOS los archivos de un modal paginado."""
    mdir = dst / f"modal_{idx}"
    set_dir(mdir)

    page = 1
    while True:
        modal = wait.until(EC.presence_of_element_located(
                (By.ID, "frmBi:lstArchivosDisp")))
        btns  = modal.find_elements(By.XPATH, ".//button[.//span[text()='Descargar']]")

        for pos in range(len(btns)):
            modal = drv.find_element(By.ID, "frmBi:lstArchivosDisp")
            fresh = modal.find_elements(By.XPATH, ".//button[.//span[text()='Descargar']]")
            if pos >= len(fresh):
                break
            print(f"      ↳ pág {page} • archivo {pos+1}/{len(btns)}")
            drv.execute_script("arguments[0].click();", fresh[pos])
            time.sleep(1.2)

        # siguiente página
        try:
            nxt = modal.find_element(
                By.XPATH, ".//a[contains(@class,'ui-paginator-next') "
                          "and not(contains(@class,'ui-state-disabled'))]")
            drv.execute_script("arguments[0].click();", nxt)
            wait_mask_off()
            page += 1
        except Exception:
            break
    close_modal()
    set_dir(dst)  # restaura carpeta del período

def iter_select(label_id: str, focus_id: str):
    """Itera por todos los valores del SelectOneMenu usando ARROW_DOWN."""
    wait_mask_off()
    wait.until(EC.element_to_be_clickable((By.ID, label_id))).click()
    focus = wait.until(EC.presence_of_element_located((By.ID, focus_id)))

    vistos, rep = set(), 0
    while rep < 2:
        anterior = drv.find_element(By.ID, label_id).text.strip()
        for _ in range(2):
            focus.send_keys(Keys.ARROW_DOWN)
            try:
                WebDriverWait(drv, 1, 0.10).until(
                    lambda d: d.find_element(By.ID, label_id).text.strip() != anterior)
                break
            except TimeoutException:
                continue

        txt = drv.find_element(By.ID, label_id).text.strip()
        if txt.lower().startswith("seleccione") or txt in vistos:
            rep += 1
            continue

        focus.send_keys(Keys.ENTER)
        wait_mask_off(); time.sleep(0.8)
        yield txt

        wait.until(EC.element_to_be_clickable((By.ID, label_id))).click()
        focus = wait.until(EC.presence_of_element_located((By.ID, focus_id)))
        vistos.add(txt); rep = 0

# ────────── INICIO ──────────
drv.get("https://aplicaciones3.ecuadorencifras.gob.ec/BIINEC-war/index.xhtml")
wait.until(EC.element_to_be_clickable(
    (By.XPATH, "//span[text()='Estadísticas Sociodemográficas y Sociales']"))).click()
wait.until(EC.element_to_be_clickable(
    (By.XPATH, "//td[span[text()='Trabajo']]"))).click()
print("✔ Menú Trabajo abierto")

ID_YEAR_L, ID_YEAR_F = "frmBi:lstOE:3:j_idt99_label", "frmBi:lstOE:3:j_idt99_focus"
ID_PER_L , ID_PER_F  = "frmBi:slPeriodos_label"   , "frmBi:slPeriodos_focus"
ROWS_CSS             = "#frmBi\\:lstArchDescarga_data > tr"

# ────────── ITERAR AÑOS ──────────
for anio in iter_select(ID_YEAR_L, ID_YEAR_F):
    year_dir = os.path.join(ROOT, slug(anio))
    for periodo in iter_select(ID_PER_L, ID_PER_F):
        per_dir = Path(os.path.join(year_dir, slug(periodo)))
        if os.path.exists(per_dir):
            print(f"⏭ {anio} - {periodo} ya descargado")
            continue

        print(f"⬇ Nuevo período: {anio} - {periodo}")
        set_dir(per_dir)

        fila, idx_modal = 0, 1
        while True:
            filas = drv.find_elements(By.CSS_SELECTOR, ROWS_CSS)
            if fila >= len(filas):
                break
            row    = filas[fila]
            nombre = row.find_element(By.CSS_SELECTOR, "td:nth-child(1) label").text.strip()
            imgsrc = row.find_element(By.CSS_SELECTOR, "td:nth-child(2) img").get_attribute("src")
            boton  = row.find_element(By.CSS_SELECTOR, "td:last-child button")

            if "varios.png" in imgsrc.lower():
                print(f"  • ({fila+1}) '{nombre}' → MODAL")
                drv.execute_script("arguments[0].click();", boton)
                dl_modal(per_dir, idx_modal); idx_modal += 1
            else:
                print(f"  • ({fila+1}) '{nombre}' → directa")
                drv.execute_script("arguments[0].click();", boton)
                time.sleep(1.0)  # espera descarga

            fila += 1

print("\n✅ Actualización mensual completada.")
drv.quit()