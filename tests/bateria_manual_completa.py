# -*- coding: utf-8 -*-
"""
Bateria COMPLETA del manual: las 97 preguntas frecuentes que trae el propio documento.

El oraculo no lo escribo yo: son las preguntas y respuestas que ya estan en el manual,
redactadas por quien lo hizo. Eso importa porque hoy me equivoque tres veces escribiendo
a mano lo que "deberia" contestar el agente, y en las tres el fallo era mio y no suyo.
La mas clara: di por hecho que el manual no traia ningun telefono y reprobe una
respuesta correcta que citaba el numero pivote, que si esta en el documento.

De cada respuesta del manual se extraen sus datos falsificables -cifras y terminos
propios del sistema- y se exige que la respuesta del agente los contenga. Las cifras se
exigen TODAS: son lo mas verificable y lo que de verdad se usa para decidir.

    python tests/bateria_manual_completa.py                    # las 97
    python tests/bateria_manual_completa.py --muestra 20        # una muestra
    python tests/bateria_manual_completa.py http://172.168.10.106:8002
"""
import os
import re
import sys
import io
import json
import time
import random
import urllib.request
import urllib.error
import urllib.parse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MANUAL = os.path.join(BASE, "manuales", "MANUAL_SIDON_INDUSTRIAL_RAG.md")
USUARIO = "QA_ManualFull"
CLAVE = "qa-manual-full"

# Palabras que no distinguen nada: aparecen en cualquier respuesta.
VACIAS = set("""
a al algo alguna algunas alguno algunos ante antes aqui asi aun aunque cada como con
contra cual cuales cuando cuanto de del desde donde dos el ella ellas ello ellos en
entre era eran es esa esas ese eso esos esta estan estas este esto estos ha hace hacer
hasta hay la las le les lo los mas me mi mientras muy no nos o para pero por porque que
quien se ser si sin sobre solo son su sus tambien tiene tienen todo todos tu un una uno
unos ver y ya sidon industrial sistema pantalla usuario usuarios manual seccion capitulo
puede pueden debe deben esta estan mismo misma cuenta forma parte vez caso casos
""".split())


def sin_acentos(t):
    for a, b in zip("áéíóúüñ", "aeiouun"):
        t = t.replace(a, b)
    return t


def cargar_preguntas():
    """Pares (pregunta, respuesta, capitulo) tal como estan en el manual."""
    with io.open(MANUAL, encoding="utf-8") as f:
        lineas = f.read().splitlines()

    pares, capitulo, pregunta, respuesta = [], "", None, []
    for linea in lineas:
        if linea.startswith("# "):
            capitulo = linea.lstrip("#").strip()
        # Cualquier linea en negrita que termine en interrogacion es una pregunta
        # frecuente. Exigir que empezara por "**¿" dejaba fuera las redactadas como
        # "**La bascula no responde, ¿que hago?**", y esas se quedaban pegadas a la
        # respuesta anterior: el oraculo salia con dos respuestas juntas y reprobaba
        # al agente por contestar solo la que se le pregunto.
        m = re.match(r"^\*\*(.*\?)\*\*\s*$", linea.strip())
        if m:
            if pregunta:
                pares.append((pregunta, " ".join(respuesta).strip(), capitulo))
            pregunta, respuesta = m.group(1), []
            continue
        if pregunta is not None:
            if linea.startswith("#") or re.match(r"^\*\*.*\?\*\*\s*$", linea.strip()):
                pares.append((pregunta, " ".join(respuesta).strip(), capitulo))
                pregunta, respuesta = None, []
            elif linea.strip():
                respuesta.append(linea.strip())
    if pregunta:
        pares.append((pregunta, " ".join(respuesta).strip(), capitulo))
    return [(p, r, c) for p, r, c in pares if r]


def claves(respuesta, pregunta):
    """
    Datos falsificables de la respuesta del manual: cifras y terminos propios.

    Se descartan las palabras que ya estan en la pregunta -repetirlas no demuestra nada-
    y las vacias. Sin esto, "¿que es el OEE?" se aprobaria con solo decir "el OEE".
    """
    texto = re.sub(r"\*\*|`|>", " ", respuesta)
    # Las referencias cruzadas del manual ("ver seccion 5.3", "Capitulo 7") no son datos
    # que el agente deba repetir: son senalizacion interna del documento. Exigirlas
    # reprobaba respuestas mejores que la del manual, como contestar "sujeto al limite de
    # 7 dias" en vez de "sujeto a los limites de la seccion 5.3".
    texto = re.sub(r"(secci[oó]n|cap[ií]tulo|anexo|ver)\s+[A-Z]?\.?\s*\d+(\.\d+)*",
                   " ", texto, flags=re.IGNORECASE)
    numeros = re.findall(r"\d+(?:[.,]\d+)?\s*%?", texto)
    en_pregunta = {sin_acentos(w.lower()) for w in re.findall(r"\w+", pregunta)}
    palabras = []
    for w in re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{5,}", texto):
        base = sin_acentos(w.lower())
        if base in VACIAS or base in en_pregunta or base in palabras:
            continue
        palabras.append(base)
    return [n.strip() for n in numeros], palabras


def evalua(respuesta_agente, nums, pals, minimo_palabras=0.5):
    """
    Aprueba si estan TODAS las cifras y al menos la mitad de los terminos propios.

    Las cifras no se negocian: son lo que alguien usa para decidir. Los terminos se
    piden a medias porque el agente parafrasea, y exigirlos todos reprobaria respuestas
    correctas escritas con otras palabras.
    """
    bajo = sin_acentos((respuesta_agente or "").lower())
    faltan_num = []
    for n in nums:
        limpio = n.replace(" ", "").rstrip("%")
        if limpio and limpio not in bajo.replace(" ", ""):
            faltan_num.append(n)
    presentes = sum(1 for p in pals if p in bajo)
    ratio = (presentes / len(pals)) if pals else 1.0
    return (not faltan_num and ratio >= minimo_palabras), faltan_num, round(ratio, 2)


# 400 s era demasiado: una pregunta atascada frenaba la corrida entera. Pasado este
# tiempo la respuesta ya no le sirve a nadie, asi que se cuenta como fallo y se sigue.
def pedir(metodo, ruta, cuerpo=None, espera=240):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(SERVIDOR + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    inicio = time.time()
    try:
        with urllib.request.urlopen(req, timeout=espera) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, {"message": "[HTTP %s] %s" % (e.code, e.read()[:200])}
    except Exception as e:
        return 0, {"message": "[SIN RESPUESTA tras %.0f s] %s: %s"
                              % (time.time() - inicio, type(e).__name__, e)}


def limpiar():
    _st, d = pedir("GET", "/chat/threads/%s?owner_key=%s"
                   % (USUARIO, urllib.parse.quote(CLAVE)))
    for t in d.get("threads", []):
        pedir("DELETE", "/chat/threads/%s?owner_key=%s"
              % (t["thread_id"], urllib.parse.quote(CLAVE)))


def correr(muestra=None):
    st, _ = pedir("GET", "/health")
    if st != 200:
        print("El servidor %s no responde." % SERVIDOR)
        return 1

    pares = cargar_preguntas()
    if muestra:
        random.seed(7)
        pares = random.sample(pares, min(muestra, len(pares)))
    print("PREGUNTAS FRECUENTES DEL MANUAL — %d casos contra %s\n" % (len(pares), SERVIDOR))

    fallos, por_capitulo = [], {}
    for i, (pregunta, esperada, capitulo) in enumerate(pares, 1):
        nums, pals = claves(esperada, pregunta)
        _st, r = pedir("POST", "/chat/", {"input": pregunta, "username": USUARIO,
                                          "owner_key": CLAVE, "lang": "es"})
        obtenida = r.get("message") or ""
        ok, faltan, ratio = evalua(obtenida, nums, pals)
        bien, total = por_capitulo.get(capitulo, (0, 0))
        por_capitulo[capitulo] = (bien + int(ok), total + 1)
        if not ok:
            fallos.append((pregunta, esperada, obtenida, faltan, ratio))
        print("  %-5s %3d/%d  %s" % ("OK" if ok else "FALLA", i, len(pares), pregunta[:66]))

    print("\n" + "=" * 78)
    print("MANUAL COMPLETO: %d/%d OK (%.0f%%)"
          % (len(pares) - len(fallos), len(pares),
             (len(pares) - len(fallos)) * 100.0 / max(1, len(pares))))
    print("\nPor capitulo:")
    for cap, (bien, total) in sorted(por_capitulo.items()):
        marca = "" if bien == total else "   <--"
        print("  %-52s %d/%d%s" % (cap[:52], bien, total, marca))

    for pregunta, esperada, obtenida, faltan, ratio in fallos:
        print("\nFALLA: %s" % pregunta)
        if faltan:
            print("   faltan cifras: %s" % faltan)
        print("   coincidencia de terminos: %.0f%%" % (ratio * 100))
        print("   manual: %s" % esperada.replace("\n", " ")[:190])
        print("   agente: %s" % obtenida.replace("\n", " ")[:190])

    limpiar()
    return len(fallos)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    SERVIDOR = next((a for a in args if a.startswith("http")), "http://127.0.0.1:8011")
    n = None
    if "--muestra" in sys.argv:
        idx = sys.argv.index("--muestra")
        n = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 20
    sys.exit(1 if correr(muestra=n) else 0)
