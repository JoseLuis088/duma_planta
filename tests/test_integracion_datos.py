# -*- coding: utf-8 -*-
"""
Pruebas contra SQL Server. Se saltan solas si la base no responde.

La mas importante es la reconstruccion intradia: valida los KPIs por hora contra los
resumenes de turno, que son una fuente independiente. Esa comparacion fue la que
detecto que los contadores del MES se corrigen hacia atras y que el dia operativo no
es el dia calendario.
"""
import pytest

pytestmark = pytest.mark.integracion


def test_una_consulta_rota_no_se_confunde_con_falta_de_datos(duma, bd):
    """
    run_sql devolvia ([], []) tanto si fallaba como si no habia filas, y el modelo
    narraba cifras sobre el vacio.
    """
    with pytest.raises(duma.SqlExecutionError):
        duma.run_sql("SELECT * FROM dbo.TablaQueNoExiste", raise_on_error=True)

    # Los endpoints del dashboard conservan el comportamiento anterior.
    assert duma.run_sql("SELECT * FROM dbo.TablaQueNoExiste") == ([], [])


def test_consulta_valida_sin_filas(duma, bd):
    filas, columnas = duma.run_sql(
        "SELECT TOP 0 * FROM dbo.ProductionLineIntervals", raise_on_error=True)
    assert filas == [] and len(columnas) > 0


def test_velocidad_esperada_sale_del_dato_real(duma, bd, dia_cerrado):
    """Antes era la constante 1300 kg/h, que contradecia a la propia linea."""
    tasa = duma.expected_rate_kg_h(dia_cerrado)
    assert tasa is None or tasa > 0


def test_intradia_cuadra_con_los_resumenes_de_turno(duma, bd, dia_cerrado):
    buckets, resumen = duma.build_intraday_buckets(dia_cerrado)
    assert buckets, "deberia haber snapshots del dia operativo"

    sql = """
DECLARE @d DATE = CONVERT(date, '%s');
SELECT
  SUM(CAST(wses.CurrentProductionSummary AS float))        AS Kg,
  SUM(CAST(ISNULL(wses.UnscheduledStopageMin,0) AS float)) AS ParoNP,
  SUM(CAST(wses.ProductiveTimeMin AS float))               AS Productivo
FROM ind.WorkShiftExecutionSummaries wses
JOIN dbo.WorkShiftExecutions wse ON wses.WorkShiftExecutionId=wse.WorkShiftExecutionId
JOIN dbo.WorkShiftTemplates wst ON wse.WorkShiftTemplateId=wst.WorkShiftTemplateId
WHERE wse.Status='closed' AND wse.Active=1 AND wses.Active=1 AND wse.DayOff=0
  AND (CASE WHEN wst.EndTime<wst.StartTime THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date) END) = @d;
""" % dia_cerrado
    filas, columnas = duma.run_sql(sql, raise_on_error=True)
    turnos = dict(zip(columnas, filas[0]))
    if turnos["Kg"] is None:
        pytest.skip("el dia %s no tiene turnos cerrados" % dia_cerrado)

    kg = sum(b["Kg_producidos"] for b in buckets)
    paro = sum(b["Min_paro_no_programado"] for b in buckets)
    productivo = sum(b["Min_productivos"] for b in buckets)

    assert kg == pytest.approx(float(turnos["Kg"]), rel=0.02)
    assert paro == pytest.approx(float(turnos["ParoNP"]), rel=0.02)
    assert productivo == pytest.approx(float(turnos["Productivo"]), rel=0.02)


def test_el_dia_operativo_arranca_con_el_primer_turno(duma, bd, dia_cerrado):
    """No es el dia calendario: cruza la medianoche."""
    inicio, fin = duma.operational_day_window(dia_cerrado)
    assert (fin - inicio).total_seconds() == 24 * 3600
    assert inicio.strftime("%Y-%m-%d") == dia_cerrado


def test_rango_horario(duma, bd, dia_cerrado):
    buckets, _ = duma.build_intraday_buckets(dia_cerrado, from_hour=10, to_hour=13)
    assert buckets
    assert all(10 <= int(b["hora"][:2]) <= 13 for b in buckets)


def test_dia_sin_datos_avisa_en_vez_de_devolver_vacio(duma, bd):
    with pytest.raises(duma.EmptyResultError):
        duma.build_intraday_buckets("2019-01-01")


def test_el_pareto_del_dashboard_sirve_al_agente(duma, bd, dia_cerrado):
    """plot_pareto_stop_reasons ya existia para el dashboard y no estaba expuesto."""
    sql = """
DECLARE @d DATE = CONVERT(date, '%s');
SELECT TOP 20 ISNULL(mt.Name,N'Sin Clasificar') AS Tipo_General,
  ISNULL(m.Name,N'Sin Clasificar') AS Motivo_Particular,
  ISNULL(m.StoppageType,s.Type) AS Clasificacion,
  SUM(DATEDIFF(SECOND,s.StartDate,s.EndDate))/60.0 AS Duracion_Min, COUNT(*) AS Eventos
FROM dbo.Stopages s
LEFT JOIN dbo.Motives m ON s.MotiveId=m.MotiveId
LEFT JOIN dbo.MotivesType mt ON m.MotiveTypeId=mt.MotiveTypeId
JOIN dbo.WorkShiftExecutions wse ON s.WorkshiftExecutionId=wse.WorkshiftExecutionId
JOIN dbo.WorkShiftTemplates wst ON wse.WorkShiftTemplateId=wst.WorkShiftTemplateId
WHERE s.Active=1 AND (CASE WHEN wst.EndTime<wst.StartTime
      THEN DATEADD(day,-1,CAST(wse.EndDate AS date))
      ELSE CAST(wse.StartDate AS date) END) = @d
GROUP BY mt.Name,m.Name,m.StoppageType,s.Type ORDER BY Duracion_Min DESC;
""" % dia_cerrado
    filas, columnas = duma.run_sql(sql, raise_on_error=True)
    if not filas:
        pytest.skip("sin paros registrados el %s" % dia_cerrado)
    motivos = [dict(zip(columnas, f)) for f in filas]
    graficas = duma.plot_pareto_stop_reasons(motivos, dia_cerrado, False, lang="es")
    assert graficas and all(g.get("url") for g in graficas)
