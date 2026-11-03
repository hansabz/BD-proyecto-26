# Imagen para publicar Hospital W 2.0 en Render (o cualquier servicio que acepte Docker).
# Trae lo que el programa necesita además de Python:
#   - Java 17 (PySpark arranca una JVM por dentro)
#   - ODBC Driver 18 de Microsoft (pyodbc lo usa para hablar con SQL Server / Azure SQL)
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    ACCEPT_EULA=Y

# procps: los scripts de arranque de Spark usan el comando "ps".
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl gnupg unixodbc \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
 && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends msodbcsql18 \
 && apt-get purge -y curl gnupg && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
# gunicorn: servidor web para producción (el app.run() de Flask es solo para desarrollo).
RUN pip install --no-cache-dir -r requirements.txt gunicorn==23.0.0

COPY . .

# Render define la variable PORT. Un solo proceso (Spark ocupa memoria) con varios hilos;
# timeout alto porque la primera acción con Spark tarda mientras arranca la JVM.
CMD gunicorn app:app --bind 0.0.0.0:${PORT:-10000} --workers 1 --threads 4 --timeout 180
