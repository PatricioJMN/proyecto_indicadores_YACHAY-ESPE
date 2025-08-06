FROM python:3.9-slim

# Instala Chrome y Chromedriver
RUN apt-get update && apt-get install -y \
      wget gnupg unzip \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
         > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y \
      google-chrome-stable \
      chromium-chromedriver \
    && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Copia e instala dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia los scripts
COPY *.py /app/

# Variables default (se pueden sobreescribir en docker-compose)
ENV ENEMDU_ROOT=/app/data/ANUAL \
    PERSONA_DEST=/app/data/limpio/persona \
    VIVIENDA_DEST=/app/data/limpio/vivienda

# Crea los directorios base
RUN mkdir -p $ENEMDU_ROOT $PERSONA_DEST $VIVIENDA_DEST

# Comando por defecto: descarga y luego limpieza
CMD ["sh", "-c", "python enemdu_descarga.py && python limpieza_persona.py && python limpieza_vivienda.py"]