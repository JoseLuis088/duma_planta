# -*- coding: utf-8 -*-
"""
Construye el indice del manual para que Duma pueda responder sobre el.

El manual son ~19,700 tokens en 56 secciones. Para ese tamano un servicio de busqueda
seria desproporcionado: los vectores completos ocupan 0.3 MB y caben en memoria. Se
calculan una vez con text-embedding-ada-002 y se guardan junto a la app.

    python indexar_manual.py                       # indexa manuales/*.md
    python indexar_manual.py otro_manual.md        # indexa uno concreto

Reindexar cuesta centavos, asi que se puede rehacer cada vez que cambie el manual. El
indice queda en manuales/indice_manual.json y NO se versiona: se regenera.
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


def vectorizar(textos, lote=64):
    """Vectores de cada texto, en lotes para no hacer una llamada por trozo."""
    salida = []
    for i in range(0, len(textos), lote):
        parte = textos[i:i + lote]
        r = duma.client.embeddings.create(model=duma.EMBEDDING_DEPLOYMENT, input=parte)
        salida.extend([d.embedding for d in sorted(r.data, key=lambda d: d.index)])
        print("   vectorizados %d/%d" % (min(i + lote, len(textos)), len(textos)))
    return salida


def main():
    archivos = sys.argv[1:]
    if not archivos:
        archivos = [os.path.join(CARPETA, f) for f in sorted(os.listdir(CARPETA))
                    if f.lower().endswith(".md")]
    if not archivos:
        print("No hay ningun .md en %s" % CARPETA)
        return 1

    trozos = []
    for ruta in archivos:
        with io.open(ruta, encoding="utf-8") as f:
            contenido = f.read()
        nuevos = trocear(contenido, os.path.basename(ruta))
        print("%-42s %d secciones" % (os.path.basename(ruta), len(nuevos)))
        trozos.extend(nuevos)

    tam = sorted(contar_tokens(t["texto"]) for t in trozos)
    print("\n%d trozos   tokens: minimo %d, mediana %d, maximo %d"
          % (len(trozos), tam[0], tam[len(tam) // 2], tam[-1]))

    print("\nVectorizando con %s..." % duma.EMBEDDING_DEPLOYMENT)
    vectores = vectorizar([t["texto"] for t in trozos])

    for t, v in zip(trozos, vectores):
        t["vector"] = v

    firma = hashlib.md5("".join(t["texto"] for t in trozos).encode("utf-8")).hexdigest()
    with io.open(INDICE, "w", encoding="utf-8") as f:
        json.dump({"modelo": duma.EMBEDDING_DEPLOYMENT, "firma": firma,
                   "trozos": trozos}, f, ensure_ascii=False)

    print("\nIndice escrito en %s (%.1f MB)" % (INDICE, os.path.getsize(INDICE) / 1e6))
    return 0


if __name__ == "__main__":
    sys.exit(main())
