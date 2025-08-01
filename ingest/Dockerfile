FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias de Python necesarias
RUN pip install --no-cache-dir \
    clickhouse-driver \
    pandas \
    clickhouse-connect

# Copia de scripts y tus archivos de headers al build context
COPY ingest_persona.py ingest_vivienda.py ingest_codigos.py ./