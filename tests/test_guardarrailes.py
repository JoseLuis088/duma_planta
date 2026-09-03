# -*- coding: utf-8 -*-
"""
Guardarrailes del SQL que escribe el modelo.

Antes, viz_render ejecutaba contra produccion cualquier consulta que el modelo
inventara: la lista de tablas permitidas existia solo como texto dentro del prompt.
"""
import pytest

CONSULTAS_VALIDAS = [
    "SELECT TOP 10 * FROM dbo.ProductionLineIntervals",
    """DECLARE @d DATE='2026-08-31';
       SELECT wses.Oee FROM ind.WorkShiftExecutionSummaries wses
       INNER JOIN dbo.WorkShiftExecutions wse ON wses.WorkShiftExecutionId=wse.WorkShiftExecutionId
       INNER JOIN dbo.WorkShiftTemplates wst ON wse.WorkShiftTemplateId=wst.WorkShiftTemplateId""",
    "SELECT * FROM [dbo].[Stopages] s LEFT JOIN dbo.Motives m ON s.MotiveId=m.MotiveId",
    "SELECT a.Oee FROM (SELECT Oee FROM ind.WorkShiftExecutionSummaries) a",
    "SELECT * FROM dbo.ProductionLineIntervals WHERE 1=1 -- EXEC xp_cmdshell 'dir'",
]

CONSULTAS_RECHAZADAS = [
    ("tabla fuera de la allowlist", "SELECT * FROM dbo.Users"),
    ("DROP", "DROP TABLE dbo.Stopages"),
    ("DELETE encadenado", "SELECT 1; DELETE FROM dbo.Stopages"),
    ("UPDATE", "UPDATE dbo.Stopages SET Active=0"),
    ("EXEC xp_cmdshell", "SELECT * FROM dbo.Stopages; EXEC xp_cmdshell 'dir'"),
    ("SELECT INTO", "SELECT * INTO dbo.tmp FROM dbo.Stopages"),
    ("WAITFOR", "SELECT * FROM dbo.Stopages WAITFOR DELAY '00:10:00'"),
    ("no es una lectura", "GRANT CONTROL TO public"),
    ("consulta vacia", "   "),
]


@pytest.mark.parametrize("consulta", CONSULTAS_VALIDAS)
def test_acepta_consultas_legitimas(duma, consulta):
    assert duma.validate_agent_sql(consulta) == consulta


@pytest.mark.parametrize("caso,consulta", CONSULTAS_RECHAZADAS)
def test_rechaza_consultas_peligrosas(duma, caso, consulta):
    with pytest.raises(ValueError):
        duma.validate_agent_sql(consulta)


def test_el_mensaje_de_error_nombra_la_tabla(duma):
    with pytest.raises(ValueError, match="dbo.users"):
        duma.validate_agent_sql("SELECT * FROM dbo.Users")


@pytest.mark.parametrize("entrada,esperado", [
    ("no programado", "NP"), ("NP", "NP"), ("np", "NP"),
    ("Programado", "P"), ("p", "P"),
    (None, "TODOS"), ("todos", "TODOS"), ("cualquier cosa", "TODOS"),
])
def test_normaliza_el_tipo_de_paro(duma, entrada, esperado):
    """El codigo filtra por NP/P; el modelo escribe 'no programado'."""
    assert duma._normalize_stop_type(entrada) == esperado


# ---------- A que dia apunta "ese dia" ----------
# Secuencia real que fallaba: se pregunto por el 31 de agosto, luego por el estado
# actual de la linea y despues "cuanto tiempo productivo hubo ese dia?". Como el turno
# anterior era de tiempo real, el agente contestaba del dia de hoy sin avisar.

def _hist(*mensajes):
    return [{"role": "user", "content": m} for m in mensajes]


@pytest.mark.parametrize("mensaje,historial,apunta_a", [
    ("¿Cuánto tiempo productivo hubo ese día?",
     _hist("Dame el OEE del 31 de agosto de 2026", "Ahora dime el estado actual de la línea"),
     "31 de agosto de 2026"),
    ("¿Y el OEE de ese día?", _hist("¿Cómo estuvo el 2026-08-27?"), "2026-08-27"),
    ("¿Cuántos paros hubo ese día?", _hist("¿Cómo nos fue ayer?"), "ayer"),
    ("Compara ese periodo contra el plan",
     _hist("Dame el resumen del 25 al 31 de agosto de 2026"), "31 de agosto de 2026"),
])
def test_resuelve_a_que_dia_apunta_el_demostrativo(duma, mensaje, historial, apunta_a):
    aviso = duma.referencia_de_fecha(mensaje, historial)
    assert aviso and apunta_a in aviso


@pytest.mark.parametrize("mensaje,historial", [
    # Trae fecha propia: no hay nada que desambiguar.
    ("¿Cuál fue el OEE del 31 de agosto de 2026?", _hist("hola")),
    ("Dame el OEE de ese día, 30 de agosto de 2026", _hist("¿Cómo va hoy?")),
    # Sin demostrativo, el seguimiento se resuelve solo con el contexto del hilo.
    ("¿Y el mejor?", _hist("OEE de la semana del 25 al 31 de agosto de 2026")),
    ("¿Qué es el OEE?", _hist()),
    # Sin historial no hay dia al que apuntar: mejor callar que inventar uno.
    ("¿Cuánto produjimos ese día?", _hist()),
])
def test_no_mete_ruido_cuando_no_hace_falta(duma, mensaje, historial):
    assert duma.referencia_de_fecha(mensaje, historial) == ""
