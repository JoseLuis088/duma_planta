# -*- coding: utf-8 -*-
"""
Construye el indice del manual para que Duma pueda responder sobre el.

El manual son ~19,700 tokens. Para ese tamano un servicio de busqueda seria
desproporcionado: los vectores caben en memoria y se comparan en microsegundos. Se
calculan con text-embedding-ada-002 y se guardan junto a la app.

    python indexar_manual.py                       # indexa manuales/*.md
    python indexar_manual.py otro_manual.md        # indexa uno concreto

NO hace falta acordarse de correrlo: main.py compara al arrancar la huella de los .md
contra la que guarda el indice, y lo reconstruye si falta o si el manual cambio. Este
script existe para forzarlo a mano y para ver el desglose de trozos.

El indice (manuales/indice_manual.json) no se versiona: son megas que se regeneran en
un minuto y cuestan centavos.
"""
import io
import os
import re
import sys
import json
import hashlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import main as duma  # noqa: E402

CARPETA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manuales")
INDICE = os.path.join(CARPETA, "indice_manual.json")

# Un trozo demasiado grande diluye la busqueda: el vector acaba siendo un promedio de
# varios temas y no se parece a ninguna pregunta concreta. Los tres que pasan de este
# tamano se parten por subsección.
MAX_TOKENS_TROZO = 900


def contar_tokens(texto: str) -> int:
    try:
        import tiktoken
        return len(tiktoken.get_encoding("cl100k_base").encode(texto))
    except Exception:
        return max(1, len(texto) // 4)


def _subsecciones(titulo: str, bloque: str):
    """
    (titulo, texto) de cada subseccion del bloque, o el bloque entero si no tiene.

    Cada pieza arrastra el encabezado de su seccion: sin el, un trozo que empieza en
    "### Alertamiento por WhatsApp" no dice de que pantalla habla.
    """
    partes = bloque.split("\n### ")
    if len(partes) == 1:
        return [(titulo, bloque.strip())]
    salida = []
    if partes[0].strip():
        salida.append((titulo, partes[0].strip()))
    for sub in partes[1:]:
        sub_titulo = sub.splitlines()[0].strip()
        salida.append(("%s › %s" % (titulo, sub_titulo),
                       "## %s\n\n### %s" % (titulo, sub.strip())))
    return salida


def trocear(markdown: str, fuente: str):
    """
    Parte el manual por secciones, arrastrando el capitulo a cada trozo.

    Sin el capitulo, un trozo que dice "Como se usa la pantalla" no se distingue de
    otros diez iguales. Con el, la busqueda y la cita saben de que parte del manual
    salio.
    """
    trozos = []
    capitulo = ""
    # Cada bloque empieza en un encabezado de nivel 1 o 2.
    for bloque in re.split(r"(?m)^(?=#{1,2} )", markdown):
        bloque = bloque.strip()
        if not bloque:
            continue
        primera = bloque.splitlines()[0].strip()
        titulo = primera.lstrip("#").strip()
        if primera.startswith("# "):
            capitulo = titulo

        # Se aplican los DOS cortes, no uno u otro: primero por subseccion y despues,
        # dentro de cada una, por pregunta frecuente.
        #
        # Con solo el corte por pregunta, toda la prosa previa quedaba en un unico trozo
        # de 1,184 tokens cuyo vector era un promedio borroso: el escalamiento de las
        # alertas y el numero pivote dejaron de aparecer entre los seis primeros
        # resultados aun preguntando por ellos. Con solo el corte por subseccion pasaba
        # lo mismo con el Anexo C, que reune decenas de preguntas sueltas.
        for sub_titulo, pieza in _subsecciones(titulo, bloque):
            preguntas = re.findall(r"(?m)^\*\*(.*\?)\*\*\s*$", pieza)
            if len(preguntas) < 2:
                trozos.append((capitulo, sub_titulo, pieza))
                continue
            partes = re.split(r"(?m)^(?=\*\*.*\?\*\*\s*$)", pieza)
            if partes[0].strip():
                trozos.append((capitulo, sub_titulo, partes[0].strip()))
            for parte in partes[1:]:
                if parte.strip():
                    # Se repite el encabezado para que el trozo siga siendo legible por
                    # si solo y la respuesta pueda citar de donde salio.
                    trozos.append((capitulo, sub_titulo,
                                   "## %s\n\n%s" % (sub_titulo, parte.strip())))
    return [{"fuente": fuente, "capitulo": c, "seccion": t, "texto": x}
            for c, t, x in trozos if x.strip()]


def vectorizar(textos, lote=64, avisar=lambda *_: None):
    """Vectores de cada texto, en lotes para no hacer una llamada por trozo."""
    salida = []
    for i in range(0, len(textos), lote):
        parte = textos[i:i + lote]
        r = duma.client.embeddings.create(model=duma.EMBEDDING_DEPLOYMENT, input=parte)
        salida.extend([d.embedding for d in sorted(r.data, key=lambda d: d.index)])
        avisar("   vectorizados %d/%d" % (min(i + lote, len(textos)), len(textos)))
    return salida


def construir_indice(archivos=None, avisar=lambda *_: None):
    """
    Construye el indice y lo escribe. Devuelve cuantos trozos quedaron.

    Vive aqui y no en main.py para no cargar el indexado en cada arranque del servidor,
    pero main.py la llama al arrancar si el indice falta o el manual cambio.
    """
    if not archivos:
        archivos = [os.path.join(CARPETA, f) for f in sorted(os.listdir(CARPETA))
                    if f.lower().endswith(".md")]
    if not archivos:
        raise FileNotFoundError("No hay ningun .md en %s" % CARPETA)

    trozos = []
    for ruta in archivos:
        with io.open(ruta, encoding="utf-8") as f:
            contenido = f.read()
        nuevos = trocear(contenido, os.path.basename(ruta))
        avisar("%-42s %d secciones" % (os.path.basename(ruta), len(nuevos)))
        trozos.extend(nuevos)

    tam = sorted(contar_tokens(t["texto"]) for t in trozos)
    avisar("%d trozos   tokens: minimo %d, mediana %d, maximo %d"
           % (len(trozos), tam[0], tam[len(tam) // 2], tam[-1]))

    avisar("Vectorizando con %s..." % duma.EMBEDDING_DEPLOYMENT)
    for t, v in zip(trozos, vectorizar([t["texto"] for t in trozos], avisar=avisar)):
        t["vector"] = v

    # La firma se calcula sobre los .md, no sobre los trozos: main.py la compara al
    # arrancar para saber si el manual cambio desde el ultimo indexado.
    with io.open(INDICE, "w", encoding="utf-8") as f:
        json.dump({"modelo": duma.EMBEDDING_DEPLOYMENT,
                   "firma": duma.firma_de_los_manuales(),
                   "trozos": trozos}, f, ensure_ascii=False)
    avisar("Indice escrito en %s (%.1f MB)" % (INDICE, os.path.getsize(INDICE) / 1e6))
    return len(trozos)


def main():
    try:
        construir_indice(sys.argv[1:], avisar=print)
    except FileNotFoundError as e:
        print(e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
