import os
from dotenv import load_dotenv
import pyodbc

# 1️⃣ Cargar variables desde el archivo .env
load_dotenv()

# 2️⃣ Leer valores
SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DB")
USER = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASS")
DRIVER = os.getenv("SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

# 3️⃣ Crear la cadena de conexión
connection_string = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USER};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
)

# 4️⃣ Probar la conexión
try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION;")
    version = cursor.fetchone()[0]
    print("✅ Conexión exitosa a SQL Server")
    print("Versión del servidor:", version)
    conn.close()
except Exception as e:
    print("❌ Error al conectar a SQL Server:")
    print(e)
