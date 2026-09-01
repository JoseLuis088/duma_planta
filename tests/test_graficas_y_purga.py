# -*- coding: utf-8 -*-
"""
Dos protecciones que evitan reportar cosas que no existen o llenar el disco.

1. Una grafica sin datos debe fallar, no dibujar ejes vacios: de ahi salio el
   "OEE promedio 52.5%" narrado sobre una grafica en blanco.
2. La purga borra por antiguedad y por tamano, pero nunca lo generado hoy.
"""
import os
import time

import pandas as pd
import pytest


# --------------------------------------------------------------- graficas
def test_dataframe_vacio_no_genera_grafica(duma):
    with pytest.raises(duma.EmptyResultError):
        duma.render_chart_from_df(
            pd.DataFrame(columns=["Fecha", "OEE"]),
            {"chart": "line", "x": "Fecha", "ys": ["OEE"]})


def test_serie_toda_nula_no_genera_grafica(duma):
    df = pd.DataFrame({"Fecha": ["2026-08-25", "2026-08-26"], "OEE": [None, None]})
    with pytest.raises(duma.EmptyResultError):
        duma.render_chart_from_df(df, {"chart": "line", "x": "Fecha", "ys": ["OEE"]})


def test_con_datos_si_genera_grafica(duma, tmp_path):
    df = pd.DataFrame({"Fecha": ["2026-08-25", "2026-08-26"], "OEE": [51.2, 63.4]})
    url = duma.render_chart_from_df(
        df, {"chart": "line", "x": "Fecha", "ys": ["OEE"], "agg": "none"})
    assert url.endswith(".html")
    assert os.path.exists(url)
    with open(url, encoding="utf-8") as f:
        cabeza = f.read(800)
    # plotly.js debe salir de static/vendor, no de un CDN
    assert "../vendor/plotly.min.js" in cabeza
    assert "cdn.plot.ly" not in cabeza
    os.remove(url)


# ------------------------------------------------------------------ purga
def _crear(ruta, kb, dias):
    with open(ruta, "wb") as f:
        f.write(b"x" * (kb * 1024))
    t = time.time() - dias * 86400
    os.utime(ruta, (t, t))


@pytest.fixture
def carpeta_sandbox(duma, tmp_path, monkeypatch):
    """Purga sobre una carpeta temporal: nunca sobre las graficas reales."""
    carpeta = tmp_path / "plots"
    carpeta.mkdir()
    monkeypatch.setattr(duma, "GENERATED_DIRS", [str(carpeta)])
    return carpeta


def test_borra_lo_mas_viejo_que_la_retencion(duma, carpeta_sandbox, monkeypatch):
    monkeypatch.setattr(duma, "PLOTS_RETENTION_DAYS", 30)
    monkeypatch.setattr(duma, "PLOTS_MAX_GB", 100)
    _crear(str(carpeta_sandbox / "viejo_45d.html"), 10, 45)
    _crear(str(carpeta_sandbox / "limite_31d.html"), 10, 31)
    _crear(str(carpeta_sandbox / "reciente_2d.html"), 10, 2)
    _crear(str(carpeta_sandbox / "de_hoy.html"), 10, 0)

    r = duma.purge_generated_files()

    assert r["archivos_borrados"] == 2
    assert sorted(os.listdir(str(carpeta_sandbox))) == ["de_hoy.html", "reciente_2d.html"]


def test_el_tope_de_tamano_respeta_lo_de_hoy(duma, carpeta_sandbox, monkeypatch):
    monkeypatch.setattr(duma, "PLOTS_RETENTION_DAYS", 30)
    monkeypatch.setattr(duma, "PLOTS_MAX_GB", 3.0 / 1024)  # 3 MB
    _crear(str(carpeta_sandbox / "de_hoy.html"), 1024, 0)
    for i in range(5):
        _crear(str(carpeta_sandbox / ("grande_%d.html" % i)), 1024, 10 - i)

    duma.purge_generated_files()
    quedan = os.listdir(str(carpeta_sandbox))

    total = sum(os.path.getsize(os.path.join(str(carpeta_sandbox), f)) for f in quedan)
    assert total <= 3.2 * 1024 * 1024
    assert "de_hoy.html" in quedan, "nunca debe borrar lo generado en las ultimas 24 h"
    assert "grande_0.html" not in quedan, "debe empezar por lo mas antiguo"


def test_carpeta_inexistente_no_rompe(duma, tmp_path, monkeypatch):
    monkeypatch.setattr(duma, "GENERATED_DIRS", [str(tmp_path / "no_existe")])
    assert duma.purge_generated_files()["archivos_borrados"] == 0


def test_la_purga_automatica_esta_apagada_por_defecto(duma):
    """
    Arrancar el servidor no debe borrar nada: levantar la app local para una prueba
    llego a borrar 3.6 GB de graficas de desarrollo.
    """
    import os as _os
    assert _os.getenv("PLOTS_PURGE_ENABLED", "0") in ("0", "false", "no", "")
