FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1

# 1) Instala Chrome y utilidades
RUN apt-get update && apt-get install -y \
      wget gnupg unzip curl \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub \
         | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
         > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2) Instala dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3) Copia tus scripts
COPY *.py /app/

# 4) Variables por defecto (puedes sobreescribirlas en docker-compose)
ENV ENEMDU_ROOT=/data/raw/ANUAL \
    PERSONA_UNPROC=/data/enemdu_persona/unprocessed \
    PERSONA_PROCESSED=/data/enemdu_persona/processed \
    VIVIENDA_UNPROC=/data/enemdu_vivienda/unprocessed \
    VIVIENDA_PROCESSED=/data/enemdu_vivienda/processed

# 5) Crea los directorios de datos
RUN mkdir -p $ENEMDU_ROOT \
             $PERSONA_UNPROC $PERSONA_PROCESSED \
             $VIVIENDA_UNPROC $VIVIENDA_PROCESSED

# 6) Al arrancar, descargas y luego limpiezas
CMD ["sh", "-c", "python enemdu_descarga.py && python limpieza_persona.py && python limpieza_vivienda.py"]
