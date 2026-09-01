# -*- coding: utf-8 -*-
"""
Configuracion comun de las pruebas de Duma.

Importar main.py ya NO toca la base de datos (ver _CriticalVarIdsProxy), asi que las
pruebas de logica pura corren en cualquier maquina. Las que necesitan SQL Server van
marcadas con @pytest.mark.integracion y se saltan solas si la base no responde.

Uso:
    pytest tests -q                      # todo
    pytest tests -q -m "not integracion" # solo logica pura, sin base de datos
"""
import os
import sys

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RAIZ not in sys.path:
    sys.path.insert(0, RAIZ)

# Las pruebas no deben ensuciar la salida con el SQL completo de cada consulta.
os.environ.setdefault("SQL_DEBUG", "0")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integracion: requiere SQL Server; se salta si no hay conexion")


@pytest.fixture(scope="session")
def duma():
    """El modulo main ya importado."""
    import main
    return main


@pytest.fixture(scope="session")
def bd(duma):
    """Salta la prueba si la base de datos de planta no responde."""
    try:
        duma.run_sql("SELECT 1", raise_on_error=True)
    except Exception as e:
        pytest.skip("SQL Server no disponible: %s" % str(e)[:120])
    return True


@pytest.fixture(scope="session")
def dia_cerrado():
    """Un dia con los tres turnos cerrados, usado por las pruebas de integracion."""
    return os.environ.get("DUMA_TEST_DAY", "2026-08-31")
