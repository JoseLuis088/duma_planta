# -*- coding: utf-8 -*-
"""
Troceado e indice del manual.

Lo que se prueba aqui es lo que puede romperse EN SILENCIO: si el troceado pierde el
capitulo, la respuesta deja de poder citar de donde salio; si el indice se usa con un
modelo distinto al que lo genero, la busqueda devuelve secciones irrelevantes sin dar
ningun error. Las respuestas del agente las cubre tests/bateria_manual.py.
"""
import io
import os
import sys
import json

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import indexar_manual  # noqa: E402


MANUAL = """# Capitulo 1 · Antes de empezar

Texto introductorio del capitulo.

## 1.1 Que es el OEE

El OEE mide disponibilidad, desempeno y calidad.

## 1.2 Perfiles

Hay cinco perfiles de usuario.

# Anexo A · Glosario

Definiciones.
"""


def test_cada_trozo_sabe_de_que_capitulo_es():
    """
    Sin el capitulo, un trozo que dice "Como se usa la pantalla" no se distingue de
    otros diez iguales, y la respuesta no puede citar su origen.
    """
    trozos = indexar_manual.trocear(MANUAL, "m.md")
    por_seccion = {t["seccion"]: t["capitulo"] for t in trozos}
    assert por_seccion["1.1 Que es el OEE"] == "Capitulo 1 · Antes de empezar"
    assert por_seccion["1.2 Perfiles"] == "Capitulo 1 · Antes de empezar"
    assert por_seccion["Anexo A · Glosario"] == "Anexo A · Glosario"


def test_no_pierde_contenido_al_trocear():
    trozos = indexar_manual.trocear(MANUAL, "m.md")
    junto = " ".join(t["texto"] for t in trozos)
    for frase in ("disponibilidad, desempeno y calidad", "cinco perfiles", "Definiciones"):
        assert frase in junto


def test_parte_las_secciones_demasiado_largas():
    """
    Un trozo enorme diluye la busqueda: su vector acaba siendo el promedio de varios
    temas y no se parece a ninguna pregunta concreta.
    """
    largo = "# Cap\n\n## Seccion larga\n\n" + ("palabra " * 3000)
    largo += "\n\n### Subseccion A\n\n" + ("otra " * 800)
    trozos = indexar_manual.trocear(largo, "m.md")
    assert len(trozos) > 2, "una seccion enorme deberia partirse"
    for t in trozos:
        assert indexar_manual.contar_tokens(t["texto"]) < 4000


def test_los_trozos_vacios_se_descartan():
    trozos = indexar_manual.trocear("# Solo titulo\n\n\n", "m.md")
    assert all(t["texto"].strip() for t in trozos)


def test_el_indice_se_rechaza_si_cambio_el_modelo(duma, tmp_path, monkeypatch):
    """
    Comparar vectores de dos modelos distintos no da error: da resultados irrelevantes,
    que es peor. El indice guarda con que modelo se hizo y se niega a usarse si no
    coincide con el configurado.
    """
    ruta = tmp_path / "indice.json"
    with io.open(str(ruta), "w", encoding="utf-8") as f:
        json.dump({"modelo": "un-modelo-viejo", "trozos":
                   [{"texto": "x", "vector": [0.1] * 8, "capitulo": "c", "seccion": "s"}]}, f)

    monkeypatch.setattr(duma, "RUTA_INDICE_MANUAL", str(ruta))
    monkeypatch.setattr(duma, "EMBEDDING_DEPLOYMENT", "otro-modelo")
    monkeypatch.setattr(duma, "_INDICE_MANUAL",
                        {"cargado": False, "trozos": [], "matriz": None, "modelo": None})

    assert duma.cargar_indice_manual() is False
    assert duma.buscar_en_manual("lo que sea") == []


def test_sin_indice_la_busqueda_no_revienta(duma, tmp_path, monkeypatch):
    """Sin manual, la herramienta devuelve vacio y el agente lo dice: no falla."""
    monkeypatch.setattr(duma, "RUTA_INDICE_MANUAL", str(tmp_path / "no_existe.json"))
    monkeypatch.setattr(duma, "_INDICE_MANUAL",
                        {"cargado": False, "trozos": [], "matriz": None, "modelo": None})
    assert duma.hay_manual() is False
    assert duma.buscar_en_manual("lo que sea") == []


@pytest.mark.parametrize("pregunta", [
    "¿Cómo cierro sesión?",
    "¿Qué perfiles hay?",
    "¿Cada cuánto escala una alerta de WhatsApp?",
    "¿Dónde veo el histórico de una variable?",
])
def test_avisa_que_el_manual_cubre_estas_preguntas(duma, pregunta):
    assert duma.aviso_pregunta_de_manual(pregunta)


@pytest.mark.parametrize("pregunta", [
    "¿Cuál fue el OEE del 31 de agosto de 2026?",
    "¿Cuántos paros no programados hubo ayer?",
    "Dame el informe ejecutivo de ayer",
])
def test_no_manda_al_manual_las_preguntas_de_datos(duma, pregunta):
    assert duma.aviso_pregunta_de_manual(pregunta) == ""
