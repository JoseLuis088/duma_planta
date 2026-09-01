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
