import time
import json
import os
import asyncio
from main import run_sql, _sql_oee_realtime, plot_oee_realtime_snapshot, plot_pareto_stop_reasons, ai_oee_realtime

async def diag():
    print("--- Diagnostic starting ---")
    start = time.time()
    
    # SQL 1
    s1 = time.time()
    rows, cols = run_sql(_sql_oee_realtime())
    print(f"SQL 1 (_sql_oee_realtime) took: {time.time()-s1:.2f}s")
    
    if not rows:
        print("No rows found, ending.")
        return

    raw_snap = dict(zip(cols, rows[0]))
    
    # SQL 2 (Pareto)
    s2 = time.time()
    from_day = time.strftime("%Y-%m-%d")
    # Using the same logic as in main.py
    pareto_sql = f"""
DECLARE @today DATE = '{from_day}';
SELECT TOP 10
    mt.Name          AS Tipo_General,
    m.Name           AS Motivo_Particular,
    m.StoppageType   AS Clasificacion,
    SUM(DATEDIFF(SECOND, s.StartDate, s.EndDate)) / 60.0 AS Duracion_Min,
    COUNT(*)                                              AS Eventos,
    AVG(DATEDIFF(SECOND, s.StartDate, s.EndDate)) / 60.0 AS Duracion_Promedio_Min
FROM dbo.Stopages s
JOIN dbo.Motives m            ON s.MotiveId           = m.MotiveId
JOIN dbo.MotivesType mt       ON m.MotiveTypeId        = mt.MotiveTypeId
JOIN dbo.WorkShiftExecutions wse ON s.WorkshiftExecutionId = wse.WorkshiftExecutionId
JOIN dbo.WorkShiftTemplates wst  ON wse.WorkShiftTemplateId = wst.WorkShiftTemplateId
WHERE s.Active = 1
  AND wse.DayOff = 0
  AND (CASE WHEN wst.EndTime < wst.StartTime
            THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date)
       END) = @today
GROUP BY mt.Name, m.Name, m.StoppageType
ORDER BY Duracion_Min DESC;
"""
    rows_p, cols_p = run_sql(pareto_sql)
    stop_reasons = [dict(zip(cols_p, r)) for r in rows_p]
    print(f"SQL 2 (Pareto SQL) took: {time.time()-s2:.2f}s")

    # Plots 1
    s3 = time.time()
    plots = plot_oee_realtime_snapshot(raw_snap)
    print(f"Plots 1 (Base Snapshot) took: {time.time()-s3:.2f}s")
    
    # Plots 2
    s4 = time.time()
    if stop_reasons:
        plots.extend(plot_pareto_stop_reasons(stop_reasons, f"Hoy ({from_day})"))
    print(f"Plots 2 (Pareto/Treemap) took: {time.time()-s4:.2f}s")
    
    # AI
    s5 = time.time()
    # Mocking snap_formatted for AI
    snap_f = {c: str(v) for dict_r in [dict(zip(cols, rows[0]))] for c,v in dict_r.items()}
    ai = await ai_oee_realtime(snap_f, stop_reasons)
    print(f"AI Analysis task (standalone) took: {time.time()-s5:.2f}s")
    
    print("\n--- Simulating Parallel Execution (as in main.py) ---")
    start_parallel = time.time()
    tasks = [
        asyncio.to_thread(plot_oee_realtime_snapshot, raw_snap)
    ]
    if stop_reasons:
        tasks.append(asyncio.to_thread(plot_pareto_stop_reasons, stop_reasons, f"Hoy ({from_day})"))
    tasks.append(ai_oee_realtime(snap_f, stop_reasons))
    
    await asyncio.gather(*tasks)
    total_parallel = time.time() - start_parallel
    print(f"Parallel execution (Plots + AI) took: {total_parallel:.2f}s")

    total = time.time() - start
    print(f"--- Total diagnostic sequence took: {total:.2f}s ---")

if __name__ == "__main__":
    asyncio.run(diag())
