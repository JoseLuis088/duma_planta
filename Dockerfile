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

EXPOSE 8000

# 4) Uvicorn (le pasamos .env en docker run)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
