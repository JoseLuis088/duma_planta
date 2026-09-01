# -*- coding: utf-8 -*-
"""Pruebas de la Fase 1: OEE intradia validado contra los resumenes de turno."""
import sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import main

ok = fail = 0
DIA = "2026-08-31"   # dia con turnos cerrados


def check(nombre, cond, detalle=""):
    global ok, fail
    if cond:
        ok += 1
        print("  OK   %s  %s" % (nombre, detalle))
    else:
        fail += 1
        print("  FALLA %s  %s" % (nombre, detalle))


print("\n== 1. Reconstruccion intradia ==")
buckets, resumen = main.build_intraday_buckets(DIA)
check("genera buckets por hora", len(buckets) > 0, "%d horas" % len(buckets))
check("cada bucket trae OEE de la hora",
      all("OEE_del_periodo" in b for b in buckets))
print("     linea: %s | velocidad esperada: %s kg/h" % (resumen["linea"], resumen["velocidad_esperada_kg_h"]))
for b in buckets[:4] + buckets[-2:]:
    print("     %s  OEE_hora=%-7s acum=%-7s D=%-7s P=%-7s kg=%-8s paroNP=%s" % (
        b["hora"], b["OEE_del_periodo"], b["OEE_acumulado_turno"], b["Disponibilidad"],
        b["Desempeno"], b["Kg_producidos"], b["Min_paro_no_programado"]))

print("\n== 2. Validacion cruzada contra ind.WorkShiftExecutionSummaries ==")
sql = """
DECLARE @d DATE = CONVERT(date, '%s');
SELECT
  SUM(CAST(wses.CurrentProductionSummary AS float)) AS Kg,
  SUM(CAST(ISNULL(wses.UnscheduledStopageMin,0) AS float)) AS ParoNP,
  SUM(CAST(ISNULL(wses.ScheduledStopageMin,0) AS float))   AS ParoP,
  SUM(CAST(wses.ProductiveTimeMin AS float))               AS Productivo
FROM ind.WorkShiftExecutionSummaries wses
JOIN dbo.WorkShiftExecutions wse ON wses.WorkShiftExecutionId=wse.WorkShiftExecutionId
JOIN dbo.WorkShiftTemplates wst ON wse.WorkShiftTemplateId=wst.WorkShiftTemplateId
WHERE wse.Status='closed' AND wse.Active=1 AND wses.Active=1 AND wse.DayOff=0
  AND (CASE WHEN wst.EndTime<wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date) END) = @d;
""" % DIA
rows, cols = main.run_sql(sql, raise_on_error=True)
turno = dict(zip(cols, rows[0]))

kg_intraday = sum(b["Kg_producidos"] for b in buckets)
np_intraday = sum(b["Min_paro_no_programado"] for b in buckets)
prod_intraday = sum(b["Min_productivos"] for b in buckets)


def cerca(a, b, tol=0.06):
    a, b = float(a or 0), float(b or 0)
    if max(abs(a), abs(b)) == 0:
        return True
    return abs(a - b) / max(abs(a), abs(b)) <= tol


print("     kg        intradia=%.1f  turnos=%.1f" % (kg_intraday, float(turno["Kg"] or 0)))
print("     paro NP   intradia=%.1f  turnos=%.1f" % (np_intraday, float(turno["ParoNP"] or 0)))
print("     productivo intradia=%.1f turnos=%.1f" % (prod_intraday, float(turno["Productivo"] or 0)))
check("kg reconstruidos coinciden con los turnos", cerca(kg_intraday, turno["Kg"]))
check("paro NP reconstruido coincide", cerca(np_intraday, turno["ParoNP"]))
check("tiempo productivo reconstruido coincide", cerca(prod_intraday, turno["Productivo"]))

print("\n== 3. Rango horario y granularidad ==")
b2, r2 = main.build_intraday_buckets(DIA, from_hour=10, to_hour=13)
check("filtra por rango horario", all(10 <= int(b["hora"][:2]) <= 13 for b in b2),
      "%s..%s" % (b2[0]["hora"], b2[-1]["hora"]))
try:
    main.build_intraday_buckets("2019-01-01")
    check("dia sin datos lanza EmptyResultError", False, "no lanzo")
except main.EmptyResultError:
    check("dia sin datos lanza EmptyResultError", True)

print("\n== 4. Graficas ==")
plots = main.plot_oee_intraday(b2, DIA)
check("genera 2 graficas", len(plots) == 2, str([p["title"] for p in plots]))
import os
check("los archivos existen", all(os.path.exists(p["url"]) for p in plots))

print("\n== 5. Grafica de paros (Pareto) ==")
rows_sp, cols_sp = main.run_sql("""
DECLARE @f DATE='2026-08-25', @t DATE='2026-08-31';
SELECT TOP 20 ISNULL(mt.Name,N'Sin Clasificar') AS Tipo_General,
  ISNULL(m.Name,N'Sin Clasificar') AS Motivo_Particular,
  ISNULL(m.StoppageType,s.Type) AS Clasificacion,
  SUM(DATEDIFF(SECOND,s.StartDate,s.EndDate))/60.0 AS Duracion_Min, COUNT(*) AS Eventos
FROM dbo.Stopages s
LEFT JOIN dbo.Motives m ON s.MotiveId=m.MotiveId
LEFT JOIN dbo.MotivesType mt ON m.MotiveTypeId=mt.MotiveTypeId
JOIN dbo.WorkShiftExecutions wse ON s.WorkshiftExecutionId=wse.WorkshiftExecutionId
JOIN dbo.WorkShiftTemplates wst ON wse.WorkShiftTemplateId=wst.WorkShiftTemplateId
WHERE s.Active=1 AND (CASE WHEN wst.EndTime<wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
     ELSE CAST(wse.StartDate AS date) END) BETWEEN @f AND @t
GROUP BY mt.Name,m.Name,m.StoppageType,s.Type ORDER BY Duracion_Min DESC;""", raise_on_error=True)
sp = [dict(zip(cols_sp, r)) for r in rows_sp]
pareto_plots = main.plot_pareto_stop_reasons(sp, "25 al 31 de agosto", False, lang="es")
check("el Pareto del dashboard es reutilizable por el agente", len(pareto_plots) >= 1,
      str([p["title"] for p in pareto_plots]))

print("\n== 6. Limpieza de bloques de codigo ==")
_fence = re.compile(r"```[a-zA-Z]*\s*\n.*?```", re.DOTALL)
texto = "El OEE de ayer fue 72%.\n\n```sql\nSELECT * FROM dbo.ProductionLineIntervals\n```\n\nRecomiendo revisar el IQF."
limpio = _fence.sub("", texto).strip()
check("quita el bloque SQL", "SELECT" not in limpio and "OEE de ayer" in limpio, repr(limpio[:60]))

print("\n----- %d OK, %d FALLAS -----" % (ok, fail))
sys.exit(1 if fail else 0)
