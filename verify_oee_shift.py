import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd

# 1. Cargar configuración
load_dotenv()

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DB")
USER = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASS")
DRIVER = os.getenv("SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

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

# 2. El query que implementamos en main.py
def get_query(day):
    return f"""
DECLARE @day DATE = '{day}';

SELECT
    wst.Name AS Turno,
    wse.StartDate,
    wse.EndDate,
    -- Fecha operativa calculada (ShiftBusinessDate)
    CASE
        WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
        ELSE CAST(wse.StartDate AS date)
    END                            AS FechaOperativa,

    wses.Oee,
    wses.Availability,
    wses.Performance,
    wses.Quality
FROM ind.WorkShiftExecutionSummaries AS wses
INNER JOIN dbo.WorkShiftExecutions AS wse
    ON wses.WorkShiftExecutionId = wse.WorkShiftExecutionId
INNER JOIN dbo.WorkShiftTemplates AS wst
    ON wse.WorkShiftTemplateId = wst.WorkShiftTemplateId
WHERE
    wse.Status = 'closed'
    AND wse.Active = 1
    AND wses.Active = 1
    AND wst.Active = 1

    -- Filtro por Fecha Operativa
    AND (
        CASE
            WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date)
        END
    ) = @day
ORDER BY
    FechaOperativa,
    CASE wst.Name
        WHEN N'Primer Turno' THEN 1
        WHEN N'Segundo Turno' THEN 2
        WHEN N'Tercer Turno' THEN 3
        ELSE 9
    END;
"""

def test_query(day):
    print(f"\n--- Verificando OEE para el día: {day} ---")
    try:
        conn = pyodbc.connect(connection_string)
        query = get_query(day)
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print(f"⚠️ No se encontraron resultados para {day}.")
        else:
            df.to_csv("verify_results.csv", index=False)
            print(f"✅ Resultados guardados en verify_results.csv")
            # También imprimir un resumen
            print(df[["Turno", "FechaOperativa", "Oee"]].to_string(index=False))
            
        conn.close()
    except Exception as e:
        print("❌ Error:")
        print(e)

if __name__ == "__main__":
    # Fecha de ejemplo que pidió el usuario
    test_query("2026-02-09")
