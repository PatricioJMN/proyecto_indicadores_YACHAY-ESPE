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
proyecto_indicadores_YACHAY-ESPE/
├── docker-compose.yml
├── data/
│   ├── Dockerfile
│   ├── download_enemdu.py
│   └── diccionario/
│       └── Dockerfile
├── ingest/
│   ├── Dockerfile
│   └── clean_normalize.py
├── init-scripts/
│   ├── Dockerfile
│   └── load_to_clickhouse.py
├── scripts_descarga/
│   ├── Dockerfile
│   └── superset_config.py
└── README.md
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
Crea un archivo .env en la raíz con parámetros como credenciales de Superset o ClickHouse:
  ```bash
  CLICKHOUSE_USER=default
  CLICKHOUSE_PASSWORD=ContrasenaSegura
  SUPSER_PW=MiSuperContrasenaSegura
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
  URL: http://localhost:8088
  Usuario: admin
  Contraseña: la que definiste en .env (SUPSER_PW)

---

## 🔄 Flujo de Trabajo Interno
1. **Scraper:**
   1.1. Ejecuta download_enemdu.py.
   1.2. Guarda archivos .csv en data/raw/.

2. **Cleaner:**
   2.1. Ejecuta clean_normalize.py.
   2.2. Lee data/raw/*.csv, aplica transformaciones y vuelca a data/clean/.

3. **Carga en ClickHouse:**
   3.1. Al iniciarse, load_to_clickhouse.py monitorea data/clean/ e ingiere nuevos CSVs.
   3.2. Crea esquema y tablas si no existen.

4. **Superset:**
   4.1. Configurado para apuntar a la base ClickHouse.
   4.2. Excluye ejemplos de dashboards en superset/config.

---

## 💻 Uso

Forzar nueva descarga:
  ```bash
  docker-compose exec scraper python download_enemdu.py --force
  ```

Procesar manualmente:
  ```bash
  docker-compose exec cleaner python clean_normalize.py
  ```

Resetear base de datos:
  ```bash
  docker-compose down --volumes
  docker-compose up --build
  ```

---

## 🤝 Contribuciones

Ing. Patricio Mendoza (ESPE)
  email: tototue2000@gmail.com
MsC. Francois Baquero (YachayTech)
  email: jenner.baquero@yachaytech.edu.ec
