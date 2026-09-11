FROM python:3.12

# 1) SO: ODBC (unixodbc) + repo MS + driver msodbcsql18
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl gnupg ca-certificates apt-transport-https \
      unixodbc unixodbc-dev \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
 && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
      -o /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

# 2) Python
WORKDIR /usr/local/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3) Código y estáticos
COPY main.py .
COPY Sidon_logo.png .
COPY static ./static
COPY DUMA_EXECUTIVE_PROMPT.txt .
COPY schema.md .
COPY duma_cookbook.txt .
COPY ["System prompt.txt", "."]
# El manual y su indexador. Sin esta linea la imagen se construye igual y el contenedor
# arranca sano, pero Duma declina todas las preguntas del manual sin decir por que: el
# primer despliegue salio asi y solo se vio revisando los logs del contenedor.
COPY indexar_manual.py .
COPY manuales ./manuales

EXPOSE 8000

# Sonda de salud: si /health deja de responder, docker marca el contenedor como
# unhealthy y queda visible en `docker ps` sin tener que revisar los logs.
# El margen de arranque cubre la construccion del indice del manual, que ocurre al
# iniciar cuando el indice falta -siempre en un despliegue nuevo, porque no se
# versiona- y tarda alrededor de un minuto entre trocear y vectorizar.
HEALTHCHECK --interval=60s --timeout=10s --start-period=180s --retries=3   CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=8).status==200 else 1)"

# 4) Uvicorn (le pasamos .env en docker run)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
