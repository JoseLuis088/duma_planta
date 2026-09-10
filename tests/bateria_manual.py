# -*- coding: utf-8 -*-
"""
Bateria del MANUAL: Duma responde sobre el manual de Sidon Industrial sin inventar.

Un agente que cita un documento puede inventarse citas, y eso es peor que no tener el
documento: una cifra equivocada se detecta, una cita falsa se cree. Por eso la bateria
tiene tres clases de caso y las tres importan por igual:

  RESPONDE  el manual lo dice -> debe decirlo, con su dato exacto.
  NO INVENTA el manual NO lo dice -> debe reconocerlo, no completar con lo razonable.
  NO SE CRUZA una pregunta de datos NO debe contestarse con el manual, ni al reves.

Ya paso lo contrario a inventar: contesto "el manual no especifica el escalamiento de
las alertas" teniendo delante la seccion que lo detalla. Por eso los casos RESPONDE
exigen el dato concreto, no que se mencione el tema.

    python tests/bateria_manual.py
    python tests/bateria_manual.py http://172.168.10.106:8002
"""
import os
import sys
import io
import json
import time
import urllib.request
import urllib.error
import urllib.parse
import re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

SERVIDOR = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8011"
USUARIO = "QA_Manual"
CLAVE = "qa-manual"
REPETICIONES = int(os.getenv("DUMA_REPETICIONES_MANUAL", "2"))


def dice(*claves):
    return lambda t: all(c.lower() in (t or "").lower() for c in claves)


def alguna(*claves):
    return lambda t: any(c.lower() in (t or "").lower() for c in claves)


# --- El manual SI lo dice: se exige el dato, no el tema -------------------------
RESPONDE = [
    ("¿Cada cuánto escala una alerta de WhatsApp si nadie la atiende?",
     lambda t: "15 minuto" in t.lower() and alguna("1 hora", "una hora", "60 minuto")(t),
     "15 min de gracia y luego 1 hora"),
    ("¿A qué perfil llega la primera alerta de WhatsApp?",
     dice("supervisor"), "al Supervisor"),
    ("¿En qué perfil se detiene el escalamiento de alertas?",
     dice("gerente"), "se detiene en Gerente"),
    ("¿Qué perfiles de usuario existen en Sidón Industrial?",
     dice("operador", "supervisor", "coordinador", "gerente", "administrador"),
     "los cinco perfiles"),
    ("¿A partir de qué OEE se considera Clase Mundial?",
     alguna("85", "85%"), "85% o mas"),
    ("¿El OEE penaliza los paros programados?",
     lambda t: alguna("no penaliza", "no lo penaliza", "no penalizan", "no")(t)
               and "programado" in t.lower(),
     "no los penaliza"),
    ("¿Qué pasa con las alertas de WhatsApp durante un paro programado?",
     alguna("suspend", "se detien", "no se envían", "no se envian"),
     "se suspenden"),
    ("¿Cómo se apaga una alerta de WhatsApp?",
     alguna("automátic", "automatic", "sola", "por sí sola", "regresa a su rango"),
     "sola, al volver la variable a rango"),
    # Este caso nacio de un error mio: lo puse como "el manual no lo dice" dando por
    # hecho que no traia ningun telefono. Si lo trae, y Duma lo cito bien. Un dato tan
    # concreto es de los mejores casos que hay, asi que se queda, pero del lado correcto.
    ("¿Hay algún número excluido de las alertas de WhatsApp?",
     dice("614-123-4567"), "el numero pivote 614-123-4567"),
    ("¿Un usuario con perfil Administrador recibe alertas de WhatsApp?",
     lambda t: "no" in t.lower() and "administrador" in t.lower(),
     "no, el Administrador no las recibe"),
]

# --- El manual NO lo dice: no debe inventarlo -----------------------------------
# Son temas creibles, del mismo mundo que el manual, para que la tentacion de completar
# sea real. La AUSENCIA de cada uno esta verificada contra el documento con grep, no
# supuesta: el primer intento de esta lista incluia el telefono de las alertas dando por
# hecho que no aparecia, y si aparece (el numero pivote). Reprobaba una respuesta buena.
NO_INVENTA = [
    # (pregunta, patron que SOLO apareceria si se hubiera inventado un dato concreto)
    ("¿Cuál es el correo de soporte técnico de Sidón Industrial?",
     r"[a-z0-9._%+-]+@[a-z0-9.-]+"),
    ("¿Qué navegadores son compatibles con Sidón Industrial?",
     r"chrome|firefox|edge|safari"),
    ("¿Cuánto cuesta la licencia de Sidón Industrial por usuario?",
     r"\d+\s*(usd|d[oó]lares|pesos|mxn|euros)"),
    ("¿Cuántos usuarios simultáneos soporta Sidón Industrial?",
     r"\d+\s*usuarios"),
    ("¿En qué versión de Sidón Industrial se agregó el módulo de costos?",
     r"versi[oó]n\s*\d"),
    ("¿Cada cuánto se respalda la base de datos de Sidón Industrial?",
     r"cada\s+\d+\s*(hora|d[ií]a|minuto)|diariamente|semanalmente|cada noche"),
]

# Reconocer que el manual no lo cubre y declinar por alcance son las DOS formas
# honestas de no saber. Lo unico que se reprueba es inventarse el dato.
#
# La primera version de esta bateria exigia la primera forma y reprobaba la segunda,
# y ademas buscaba frases como "usuarios simultaneos" como senal de invencion: aparecen
# al repetir la pregunta, asi que reprobaba la respuesta correcta "el manual no
# especifica cuantos usuarios simultaneos soporta". Media la forma, no el fondo.
SENAL_NO_SABE = ("no especifica", "no indica", "no menciona", "no lo dice", "no aparece",
                 "no detalla", "no incluye", "no cubre", "no contiene", "no encontré",
                 "no encontre", "no hay informaci", "no dispone", "no se especifica",
                 "no proporciona", "no está", "no esta",
                 "no puedo ayudarte", "estoy enfocado")

# --- Preguntas de datos: no se contestan con el manual --------------------------
NO_SE_CRUZA = [
    ("¿Cuál fue el OEE del 31 de agosto de 2026?", "63", "el dato real, no el manual"),
    ("¿Cuántos paros no programados hubo el 31 de agosto de 2026?", "27", "27 eventos"),
]


def pedir(metodo, ruta, cuerpo=None, espera=400):
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


def preguntar(texto):
    _st, r = pedir("POST", "/chat/", {"input": texto, "username": USUARIO,
                                      "owner_key": CLAVE, "lang": "es"})
    return r.get("message") or ""


def limpiar():
    _st, d = pedir("GET", "/chat/threads/%s?owner_key=%s"
                   % (USUARIO, urllib.parse.quote(CLAVE)))
    for t in d.get("threads", []):
        pedir("DELETE", "/chat/threads/%s?owner_key=%s"
              % (t["thread_id"], urllib.parse.quote(CLAVE)))


def correr():
    st, _ = pedir("GET", "/health")
    if st != 200:
        print("El servidor %s no responde." % SERVIDOR)
        return 1

    fallos = []

    print("EL MANUAL LO DICE — debe dar el dato  (%d repeticiones)" % REPETICIONES)
    print("=" * 78)
    for pregunta, comprobar, nota in RESPONDE:
        aciertos = 0
        ultima = ""
        for _ in range(REPETICIONES):
            ultima = preguntar(pregunta)
            aciertos += bool(comprobar(ultima))
        ok = aciertos == REPETICIONES
        if not ok:
            fallos.append((pregunta, "esperado: %s" % nota, ultima))
        print("  %-5s %-56s %d/%d" % ("OK" if ok else "FALLA", pregunta[:56],
                                      aciertos, REPETICIONES))

    print("\nEL MANUAL NO LO DICE — no debe inventarlo")
    print("=" * 78)
    for pregunta, patron_inventado in NO_INVENTA:
        texto = preguntar(pregunta)
        bajo = texto.lower()
        honesto = any(s in bajo for s in SENAL_NO_SABE)
        invento = re.search(patron_inventado, bajo, re.IGNORECASE)
        ok = honesto and not invento
        if not ok:
            fallos.append((pregunta,
                           "se invento un dato: %r" % invento.group(0) if invento
                           else "ni reconocio que no lo sabe ni declino", texto))
        print("  %-5s %s" % ("OK" if ok else "FALLA", pregunta[:64]))

    print("\nPREGUNTAS DE DATOS — no se responden con el manual")
    print("=" * 78)
    for pregunta, esperado, nota in NO_SE_CRUZA:
        texto = preguntar(pregunta)
        ok = esperado in texto and "manual" not in texto.lower()
        if not ok:
            fallos.append((pregunta, "esperado: %s (y sin citar el manual)" % nota, texto))
        print("  %-5s %s" % ("OK" if ok else "FALLA", pregunta[:64]))

    total = len(RESPONDE) + len(NO_INVENTA) + len(NO_SE_CRUZA)
    print("\n" + "=" * 78)
    print("MANUAL: %d/%d OK" % (total - len(fallos), total))
    for pregunta, motivo, respuesta in fallos:
        print("\nFALLA: %s" % pregunta)
        print("   %s" % motivo)
        print("   obtuvo: %s" % (respuesta or "").replace("\n", " ")[:200])

    limpiar()
    return len(fallos)


if __name__ == "__main__":
    sys.exit(1 if correr() else 0)
