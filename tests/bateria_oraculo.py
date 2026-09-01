# -*- coding: utf-8 -*-
"""
Oraculo: la verdad calculada directamente contra SQL Server.

No pasa por el agente ni por sus herramientas: son consultas independientes cuyo
resultado sirve para juzgar si lo que responde Duma es correcto.
"""
import os, sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import main

DIA = "2026-08-31"
DESDE, HASTA = "2026-08-25", "2026-08-31"

SQL_TURNOS = """
DECLARE @f DATE = CONVERT(date,'%s'), @t DATE = CONVERT(date,'%s');
SELECT
  CASE WHEN wst.EndTime < wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
       ELSE CAST(wse.StartDate AS date) END              AS Fecha,
  wst.Name                                               AS Turno,
  CAST(wses.Oee AS float)                                AS OEE,
  CAST(wses.Availability AS float)                       AS Disponibilidad,
  CAST(wses.Performance AS float)                        AS Desempeno,
  CAST(wses.Quality AS float)                            AS Conforme,
  CAST(wses.AvailableTimeMin AS float)                   AS DisponibleMin,
  CAST(wses.ProductiveTimeMin AS float)                  AS ProductivoMin,
  CAST(wses.CurrentProductionSummary AS float)           AS RealKg,
  CAST(wses.ExpectedProductionSummaryModified AS float)  AS EsperadoKg,
  CAST(ISNULL(wses.UnscheduledStopageMin,0) AS float)    AS ParoNPMin,
  CAST(ISNULL(wses.ScheduledStopageMin,0) AS float)      AS ParoPMin,
  CAST(ISNULL(wses.UnscheduledStopagesCount,0) AS int)   AS ParoNPCont,
  CAST(ISNULL(wses.ScheduledStopagesCount,0) AS int)     AS ParoPCont
FROM ind.WorkShiftExecutionSummaries wses
JOIN dbo.WorkShiftExecutions wse ON wses.WorkShiftExecutionId=wse.WorkShiftExecutionId
JOIN dbo.WorkShiftTemplates  wst ON wse.WorkShiftTemplateId =wst.WorkShiftTemplateId
WHERE wse.Status='closed' AND wse.Active=1 AND wses.Active=1 AND wse.DayOff=0
  AND (CASE WHEN wst.EndTime<wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date) END) BETWEEN @f AND @t
ORDER BY Fecha, CASE wst.Name WHEN N'Primer Turno' THEN 1 WHEN N'Segundo Turno' THEN 2 ELSE 3 END;
""" % (DESDE, HASTA)

SQL_PAROS = """
DECLARE @f DATE = CONVERT(date,'%s'), @t DATE = CONVERT(date,'%s');
SELECT
  CASE WHEN wst.EndTime < wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
       ELSE CAST(wse.StartDate AS date) END        AS Fecha,
  ISNULL(mt.Name,N'Sin Clasificar')                AS Tipo,
  ISNULL(m.Name,N'Sin Clasificar')                 AS Motivo,
  ISNULL(m.StoppageType,s.Type)                    AS Clase,
  SUM(DATEDIFF(SECOND,s.StartDate,s.EndDate))/60.0 AS Min,
  COUNT(*)                                         AS Eventos
FROM dbo.Stopages s
LEFT JOIN dbo.Motives m      ON s.MotiveId=m.MotiveId
LEFT JOIN dbo.MotivesType mt ON m.MotiveTypeId=mt.MotiveTypeId
JOIN dbo.WorkShiftExecutions wse ON s.WorkshiftExecutionId=wse.WorkshiftExecutionId
JOIN dbo.WorkShiftTemplates  wst ON wse.WorkShiftTemplateId=wst.WorkShiftTemplateId
WHERE s.Active=1
  AND (CASE WHEN wst.EndTime<wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date) END) BETWEEN @f AND @t
GROUP BY CASE WHEN wst.EndTime < wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
              ELSE CAST(wse.StartDate AS date) END, mt.Name, m.Name, m.StoppageType, s.Type
ORDER BY Min DESC;
""" % (DESDE, HASTA)


def filas(sql):
    r, c = main.run_sql(sql, raise_on_error=True)
    return [dict(zip(c, f)) for f in r]


def oee_ponderado(rs):
    if not rs:
        return None
    disp = sum(x["DisponibleMin"] for x in rs)
    prod = sum(x["ProductivoMin"] for x in rs)
    real = sum(x["RealKg"] for x in rs)
    esp = sum(x["EsperadoKg"] for x in rs)
    conf = sum(x["RealKg"] * x["Conforme"] / 100.0 for x in rs)
    if not (disp and esp and real):
        return None
    d, p, q = prod / disp * 100, real / esp * 100, conf / real * 100
    return {"OEE": round(d * p * q / 10000, 2), "Disponibilidad": round(d, 2),
            "Desempeno": round(p, 2), "Conforme": round(q, 2),
            "RealKg": round(real, 1), "EsperadoKg": round(esp, 1),
            "BrechaKg": round(esp - real, 1), "ProductivoMin": round(prod, 1),
            "DisponibleMin": round(disp, 1)}


turnos = filas(SQL_TURNOS)
paros = filas(SQL_PAROS)

del_dia = [t for t in turnos if str(t["Fecha"])[:10] == DIA]
paros_dia = [p for p in paros if str(p["Fecha"])[:10] == DIA]

# Agregados por causa en la semana
por_motivo = {}
for p in paros:
    k = (p["Motivo"], p["Clase"])
    e = por_motivo.setdefault(k, {"Min": 0.0, "Eventos": 0, "Tipo": p["Tipo"]})
    e["Min"] += float(p["Min"] or 0)
    e["Eventos"] += int(p["Eventos"] or 0)
top_semana = sorted(por_motivo.items(), key=lambda kv: -kv[1]["Min"])

por_motivo_dia = {}
for p in paros_dia:
    k = (p["Motivo"], p["Clase"])
    e = por_motivo_dia.setdefault(k, {"Min": 0.0, "Eventos": 0})
    e["Min"] += float(p["Min"] or 0)
    e["Eventos"] += int(p["Eventos"] or 0)
top_dia = sorted(por_motivo_dia.items(), key=lambda kv: -kv[1]["Min"])

# OEE por dia
por_dia = {}
for t in turnos:
    por_dia.setdefault(str(t["Fecha"])[:10], []).append(t)
oee_por_dia = {d: oee_ponderado(v) for d, v in por_dia.items()}

# Snapshot en tiempo real
rt_rows, rt_cols = main.run_sql(main._sql_oee_realtime(), raise_on_error=True)
rt = dict(zip(rt_cols, rt_rows[0])) if rt_rows else {}

# Intradia del dia de referencia
buckets, resumen_intra = main.build_intraday_buckets(DIA)

# Catalogo de sensores
sensores = main.get_critical_vars()

oraculo = {
    "dia": DIA, "desde": DESDE, "hasta": HASTA,
    "turnos_dia": [{k: (round(v, 2) if isinstance(v, float) else v) for k, v in t.items()
                    if k != "Fecha"} for t in del_dia],
    "global_dia": oee_ponderado(del_dia),
    "global_semana": oee_ponderado(turnos),
    "oee_por_dia": {d: (v or {}).get("OEE") for d, v in sorted(oee_por_dia.items())},
    "peor_dia": min(((d, v["OEE"]) for d, v in oee_por_dia.items() if v), key=lambda x: x[1]),
    "mejor_dia": max(((d, v["OEE"]) for d, v in oee_por_dia.items() if v), key=lambda x: x[1]),
    "paros_dia": {
        "np_min": round(sum(float(p["Min"]) for p in paros_dia if p["Clase"] == "NP"), 1),
        "np_eventos": sum(int(p["Eventos"]) for p in paros_dia if p["Clase"] == "NP"),
        "p_min": round(sum(float(p["Min"]) for p in paros_dia if p["Clase"] == "P"), 1),
        "p_eventos": sum(int(p["Eventos"]) for p in paros_dia if p["Clase"] == "P"),
        "top": [{"motivo": k[0], "clase": k[1], "min": round(v["Min"], 1), "eventos": v["Eventos"]}
                for k, v in top_dia[:5]],
    },
    "paros_semana": {
        "np_min": round(sum(float(p["Min"]) for p in paros if p["Clase"] == "NP"), 1),
        "np_eventos": sum(int(p["Eventos"]) for p in paros if p["Clase"] == "NP"),
        "total_min": round(sum(float(p["Min"]) for p in paros), 1),
        "top": [{"motivo": k[0], "clase": k[1], "tipo": v["Tipo"],
                 "min": round(v["Min"], 1), "eventos": v["Eventos"]}
                for k, v in top_semana[:8]],
    },
    "tiempo_real": {k: (float(v) if isinstance(v, (int, float)) else str(v))
                    for k, v in rt.items()},
    "intradia_dia": {
        "horas": len(buckets),
        "kg_total": round(sum(b["Kg_producidos"] for b in buckets), 1),
        "hora_mas_paro": max(buckets, key=lambda b: b["Min_paro_no_programado"] or 0)["hora"],
        "max_paro_min": round(max(b["Min_paro_no_programado"] or 0 for b in buckets), 1),
        "ventana": resumen_intra["dia_operativo"],
    },
    "sensores": {"total": len(sensores), "nombres": sorted(list(sensores.keys()))[:12]},
}

ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oraculo.json")
with io.open(ruta, "w", encoding="utf-8") as f:
    json.dump(oraculo, f, ensure_ascii=False, indent=2, default=str)

print(json.dumps({k: v for k, v in oraculo.items()
                  if k in ("global_dia", "global_semana", "oee_por_dia", "peor_dia",
                           "paros_dia", "intradia_dia")},
                 ensure_ascii=False, indent=2, default=str)[:2600])
print("\nturnos del dia:", [(t["Turno"], t["OEE"]) for t in del_dia])
print("sensores:", oraculo["sensores"]["total"])
print("OK -> oraculo.json")
