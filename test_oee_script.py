
import pyodbc
import pandas as pd

SERVER = "sidonv2-prod-sqlserver.database.windows.net"
DATABASE = "sidonv2-prod-bafar"
USER = "SIDONv2Data"
PASSWORD = "SQRyvRya4W.D4ht2FU5QmOVbO6IP/BzaMQkSxAo3R9jZ8.fYq2ld."
DRIVER = "{ODBC Driver 18 for SQL Server}"

conn_str = (
    f"DRIVER={DRIVER};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USER};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
)

def run_test():
    try:
        conn = pyodbc.connect(conn_str)
        # Query de OEE Agregado (Marzo 2026 completo para tener volumen)
        query = """
        DECLARE @fromDay DATE = '2026-03-01';
        DECLARE @toDay DATE   = '2026-03-31';

        SELECT
            COUNT(wses.WorkShiftExecutionId) AS TotalTurnos,
            SUM(wses.AvailableTimeMin)       AS MinutosDisp,
            SUM(wses.WorkshiftDurationMin)   AS MinutosDur,
            SUM(wses.CurrentProductionSummary)  AS RealKg,
            SUM(wses.ExpectedProductionSummary) AS EsperadaKg,
            
            CAST(SUM(wses.AvailableTimeMin) AS FLOAT) / NULLIF(SUM(wses.WorkshiftDurationMin), 0) AS Disponibilidad,
            CAST(SUM(wses.CurrentProductionSummary) AS FLOAT) / NULLIF(SUM(wses.ExpectedProductionSummary), 0) AS Desempeno,
            
            (CAST(SUM(wses.AvailableTimeMin) AS FLOAT) / NULLIF(SUM(wses.WorkshiftDurationMin), 0)) *
            (CAST(SUM(wses.CurrentProductionSummary) AS FLOAT) / NULLIF(SUM(wses.ExpectedProductionSummary), 0)) AS OEE
            
        FROM ind.WorkShiftExecutionSummaries AS wses
        INNER JOIN dbo.WorkShiftExecutions AS wse ON wses.WorkShiftExecutionId = wse.WorkShiftExecutionId
        INNER JOIN dbo.WorkShiftTemplates AS wst ON wse.WorkShiftTemplateId = wst.WorkShiftTemplateId
        WHERE
            wse.Status = 'closed'
            AND wse.Active = 1
            AND wses.Active = 1
            AND (CASE WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date)) ELSE CAST(wse.StartDate AS date) END) 
            BETWEEN @fromDay AND @toDay;
        """
        df = pd.read_sql(query, conn)
        print(df.to_json(orient='records'))
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_test()
