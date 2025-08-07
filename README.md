# Proyecto Indicadores YACHAY-ESPE

Sistema basado en Docker Compose para la obtención, procesamiento y visualización de indicadores de la ENEMDU (Encuesta Nacional de Empleo, Desempleo y Subempleo) en la plataforma YACHAY-ESPE.

---

## 📑 Descripción

Este proyecto orquesta cuatro componentes principales:

1. **Scraper de ENEMDU**  
   Descarga automática de los archivos de encuesta ENEMDU desde el portal oficial mediante web scraping.

2. **Limpieza y Normalización**  
   Procesa los CSV descargados, corrige formatos, llena valores faltantes y genera un dataset “limpio” en `data/clean/`.

3. **Base de Datos ClickHouse**  
   Servicio ClickHouse que ingiere los CSV limpios usando scripts en Python para carga masiva.

4. **Visualización con Apache Superset**  
   Interfaz web de Superset preconfigurada para conectarse automáticamente a ClickHouse y generar dashboards de indicadores laborales.

---

## 🚀 Características

- **Automatización completa**: un solo `docker-compose up -d` monta todos los servicios.  
- **Modularidad**: cada componente corre en su propio contenedor.  
- **Reproducible**: entornos idénticos en desarrollo y producción gracias a Docker.  
- **Dashboards interactivos**: gráficos y tablas configurables en Superset para análisis exploratorio.

---

## 📂 Estructura del Proyecto

```bash
└── patriciojmn-proyecto_indicadores_yachay-espe/
    ├── README.md
    ├── docker-compose.yml
    ├── data/
    │   ├── diccionario/
    │   │   ├── processed/
    │   │   │   └── *.csv
    │   │   └── unprocessed/
    │   │       └── *.csv
    │   ├── enemdu_persona/
    │   │   ├── processed/
    │   │   │   └── *.csv
    │   │   └── unprocessed/
    │   │       └── *.csv
    │   ├── enemdu_vivienda/
    │   │   ├── processed/
    │   │   │   └── *.csv
    │   │   └── unprocessed/
    │   │       └── *.csv
    │   ├── raw/
    │   │   └── ANUAL/
    │   │       ├── 2007/
    │   │       ├── 2008/
    │   │       ├── ... /
    │   │       └── 2025/
    ├── ingest/
    │   ├── calcular_indicadores.py
    │   ├── Dockerfile
    │   ├── ingest_codigos.py
    │   ├── ingest_indicadores.py
    │   ├── ingest_persona.py
    │   └── ingest_vivienda.py
    ├── init-scripts/
    │   ├── clickhouse/
    │   │   └── create_table.sql
    │   └── superset/
    │       └── init_superset_db.py
    └── scripts_descarga/
        ├── Dockerfile
        ├── enemdu_descarga.py
        ├── limpieza_persona.py
        ├── limpieza_vivienda.py
        └── requirements.txt
```

---

## ⚙️ Requisitos Previos

- [Docker](https://www.docker.com/) ≥ 20.10  
- [Docker Compose](https://docs.docker.com/compose/) ≥ 1.29  
- Acceso a internet para descargar los datos ENEMDU.

---

## 🛠️ Instalación y Arranque

1. **Clona el repositorio**
   ```bash
   git clone https://github.com/PatricioJMN/proyecto_indicadores_YACHAY-ESPE.git
   cd proyecto_indicadores_YACHAY-ESPE
   ```
   
2. **Configura variables de entorno (opcional)**
Crea un archivo .env en la raíz con parámetros como credenciales de Superset o ClickHouse (o simplemente edita el `docker-compose.yml`):
   ```bash
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=ContrasenaSegura
   SUPERSET_PW=MiSuperContrasenaSegura
   ```

3. **Levanta los servicios**
   ```bash
   docker-compose up -d
   ```
   
4. **Verifica el estado**
   ```bash
   docker-compose ps
   ```
   
5. **Accede a Superset**
   - URL: `http://localhost:8088`  
   - Usuario: `admin`  
   - Contraseña: la definida en `.env` (SUPERSET_PW)

---

## 🔄 Flujo de Trabajo Interno
1. **Scraper:**
   - 1.1. Ejecuta enemdu_descarga.py.
   - 1.2. Guarda archivos .csv en data/raw/ANUAL.

2. **Cleaner:**
   - 2.1. Ejecuta limpieza_persona.py y limpieza_vivienda.py.
   - 2.2. Lee data/raw/ANUAL/{AÑO}/*.zip, extrae los CSV comprimidos y vuelca a data/enemdu_persona/unprocessed/.

3. **Carga en ClickHouse:**
   - 3.1. Al iniciarse, crea esquema, tablas si no existen y las vistas materializadas con el cálculo automático de indicadores.
   - 3.2. `ingest_codigos.py`, `ingest_vivienda.py` e `ingest_persona.py` monitorean data/enemdu_{vivienda/persona}/unprocessed/ e inserta los nuevos CSVs a la base de datos.

4. **Superset:**
   - 4.1. Crea el usuario Administrador (configurado en el `docker-compose.yml`).
   - 4.2. Configurado para apuntar a la base ClickHouse.
   - 4.3. Excluye ejemplos de dashboards en superset/config.

---

## 💻 Uso

Forzar nueva descarga:
  ```bash
  docker-compose exec enemdu_descarga python enemdu_descarga.py --force
  ```

Procesar manualmente:
  ```bash
  docker-compose exec enemdu_descarga python limpieza_persona.py
  docker-compose exec enemdu_descarga python limpieza_vivienda.py
  ```

Resetear base de datos:
  ```bash
  docker-compose down --volumes
  docker-compose up --build
  ```

---

## 🤝 Contribuciones

- **Ing. Patricio Mendoza (ESPE)**  
  - Email: `tototue2000@gmail.com`

- **MsC. Francois Baquero (YachayTech)**  
  - Email: `jenner.baquero@yachaytech.edu.ec`
