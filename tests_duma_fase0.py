# -*- coding: utf-8 -*-
"""Pruebas de la Fase 0 contra el codigo real."""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import main
import pandas as pd

ok = fail = 0


def check(nombre, cond, detalle=""):
    global ok, fail
    if cond:
        ok += 1
        print("  OK   %s" % nombre)
    else:
        fail += 1
        print("  FALLA %s  %s" % (nombre, detalle))


print("\n== 1. Validador de SQL del modelo ==")
CASOS_VALIDOS = [
    "SELECT TOP 10 * FROM dbo.ProductionLineIntervals",
    """DECLARE @d DATE='2026-08-31';
       SELECT wses.Oee FROM ind.WorkShiftExecutionSummaries wses
       INNER JOIN dbo.WorkShiftExecutions wse ON wses.WorkShiftExecutionId=wse.WorkShiftExecutionId
       INNER JOIN dbo.WorkShiftTemplates wst ON wse.WorkShiftTemplateId=wst.WorkShiftTemplateId""",
    "SELECT * FROM [dbo].[Stopages] s LEFT JOIN dbo.Motives m ON s.MotiveId=m.MotiveId",
    "SELECT a.Oee FROM (SELECT Oee FROM ind.WorkShiftExecutionSummaries) a",
]
CASOS_RECHAZADOS = [
    ("tabla fuera de allowlist", "SELECT * FROM dbo.Users"),
    ("DROP", "DROP TABLE dbo.Stopages"),
    ("DELETE encadenado", "SELECT 1; DELETE FROM dbo.Stopages"),
    ("UPDATE", "UPDATE dbo.Stopages SET Active=0"),
    ("EXEC xp_cmdshell", "SELECT * FROM dbo.Stopages; EXEC xp_cmdshell 'dir'"),
    ("SELECT INTO", "SELECT * INTO dbo.tmp FROM dbo.Stopages"),
    ("WAITFOR", "SELECT * FROM dbo.Stopages WAITFOR DELAY '00:10:00'"),
    ("no es SELECT", "GRANT CONTROL TO public"),
    ("vacia", "   "),
]

for q in CASOS_VALIDOS:
    try:
        main.validate_agent_sql(q)
        check("acepta consulta legitima", True)
    except Exception as e:
        check("acepta consulta legitima", False, "rechazo: %s | %s" % (e, q[:60]))

for nombre, q in CASOS_RECHAZADOS:
    try:
        main.validate_agent_sql(q)
        check("rechaza %s" % nombre, False, "la dejo pasar")
    except ValueError:
        check("rechaza %s" % nombre, True)
    except Exception as e:
        check("rechaza %s" % nombre, False, "excepcion inesperada %r" % e)

print("\n== 2. Grafica sin datos ==")
try:
    main.render_chart_from_df(pd.DataFrame(columns=["Fecha", "OEE"]),
                              {"chart": "line", "x": "Fecha", "ys": ["OEE"]})
    check("DataFrame vacio no genera grafica", False, "genero grafica igual")
except main.EmptyResultError:
    check("DataFrame vacio no genera grafica", True)

try:
    df_nan = pd.DataFrame({"Fecha": ["2026-08-25", "2026-08-26"], "OEE": [None, None]})
    main.render_chart_from_df(df_nan, {"chart": "line", "x": "Fecha", "ys": ["OEE"]})
    check("columnas todo-NaN no generan grafica", False, "genero grafica igual")
except main.EmptyResultError:
    check("columnas todo-NaN no generan grafica", True)

df_ok = pd.DataFrame({"Fecha": ["2026-08-25", "2026-08-26"], "OEE": [51.2, 63.4]})
url = main.render_chart_from_df(df_ok, {"chart": "line", "x": "Fecha", "ys": ["OEE"], "agg": "none"})
check("con datos si genera grafica", isinstance(url, str) and url.endswith(".html"), url)

print("\n== 3. run_sql distingue error de vacio ==")
rows, cols = main.run_sql("SELECT TOP 0 * FROM dbo.ProductionLineIntervals", raise_on_error=True)
check("consulta valida sin filas devuelve vacio", rows == [] and len(cols) > 0, "cols=%s" % len(cols))
try:
    main.run_sql("SELECT * FROM dbo.TablaQueNoExiste", raise_on_error=True)
    check("consulta rota lanza error", False, "devolvio vacio en silencio")
except main.SqlExecutionError:
    check("consulta rota lanza error", True)
rows2, cols2 = main.run_sql("SELECT * FROM dbo.TablaQueNoExiste")
check("compatibilidad dashboard: sigue devolviendo ([],[])", rows2 == [] and cols2 == [])

print("\n== 4. Normalizacion del tipo de paro ==")
for entrada, esperado in [("no programado", "NP"), ("NP", "NP"), ("Programado", "P"),
                          ("p", "P"), (None, "TODOS"), ("todos", "TODOS")]:
    got = main._normalize_stop_type(entrada)
    check("type %r -> %s" % (entrada, esperado), got == esperado, "obtuve %s" % got)

print("\n== 5. Velocidad esperada real del periodo ==")
rate = main.expected_rate_kg_h("2026-08-25", "2026-08-31")
check("calcula kg/h desde datos reales", rate is None or rate > 0, "rate=%s" % rate)
print("     velocidad esperada 25-31 ago: %s kg/h (la constante vieja era 1300)" % rate)

print("\n----- %d OK, %d FALLAS -----" % (ok, fail))
sys.exit(1 if fail else 0)
