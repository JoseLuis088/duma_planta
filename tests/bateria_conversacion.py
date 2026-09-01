# -*- coding: utf-8 -*-
"""
Bateria CONVERSACIONAL: preguntas encadenadas en un mismo hilo, por HTTP.

La bateria de casos aislados daba 56/56 mientras el agente fallaba en la interfaz real:
ahi el usuario encadena preguntas y el historial previo entra en el contexto. Por eso
esta bateria va por el endpoint /chat/ (el mismo que usa el navegador), que persiste
cada turno; llamar a run_assistant_cycle directamente NO guarda nada y el agente se
queda sin historial, que fue justo lo que enmascaro estos fallos.

    python tests/bateria_conversacion.py                  # contra localhost:8011
    python tests/bateria_conversacion.py http://otro:8000
"""
import os
import sys
import io
import json
import re
import time
import urllib.request
import urllib.error
import urllib.parse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = os.path.dirname(os.path.abspath(__file__))
SERVIDOR = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8011"
USUARIO = "QA_Conversacion"
CLAVE = "qa-conversacional"

O = json.load(io.open(os.path.join(BASE, "oraculo.json"), encoding="utf-8"))
GD, GS = O["global_dia"], O["global_semana"]
TD = {t["Turno"]: t for t in O["turnos_dia"]}
NUM = re.compile(r"-?\d[\d,]*\.?\d*")


def numeros(t):
    out = []
    for b in NUM.findall(t or ""):
        try:
            out.append(float(b.replace(",", "")))
        except ValueError:
            pass
    return out


def tiene(valor, tol):
    return lambda t: any(abs(n - valor) <= tol for n in numeros(t))


def dice(*claves):
    return lambda t: any(c.lower() in (t or "").lower() for c in claves)


def todos(*fns):
    return lambda t: all(f(t) for f in fns)


def ninguno(*fns):
    return lambda t: not any(f(t) for f in fns)


CONVERSACIONES = [
    ("OEE del dia y luego detalle", [
        ("Dame el OEE de ayer desglosado por turnos",
         todos(tiene(TD["Primer Turno"]["OEE"], 0.7),
               tiene(TD["Tercer Turno"]["OEE"], 0.7)),
         "los tres turnos con su OEE"),
        ("¿En qué hora del 31 de agosto de 2026 se perdió más tiempo por paros?",
         dice("03:00", "3:00", "3 de la mañana", "madrugada"),
         "la hora correcta es 03:00, no 08:00"),
        ("¿Cuál fue el OEE del 29 de agosto de 2026?",
         dice("no oper", "no trabaj", "paro programado", "sin operación", "sin operacion"),
         "el 29 la linea no opero"),
        ("¿Cuántos kilos se perdieron del 25 al 31 de agosto de 2026?",
         todos(dice("super", "por encima", "no hubo", "no se perdieron"),
               ninguno(tiene(7807, 300))),
         "la planta supero el plan: no hubo perdida"),
    ]),
    ("Paros encadenados con referencias", [
        ("¿Cuántos paros no programados hubo el 31 de agosto de 2026?",
         tiene(28, 1), "28 eventos"),
        ("¿Y cuántos minutos fueron?",
         dice("8 horas", "489"), "debe entender que habla de esos paros"),
        ("¿Cuál fue la causa principal?",
         dice("sin clasificar"), "Sin Clasificar"),
        ("¿Cuántos kilos representó esa pérdida?",
         dice("teóric", "teoric", "nominal", "brecha", "plan"),
         "distinguir techo teorico de brecha real"),
    ]),
    ("Comparaciones dentro del hilo", [
        ("¿Cuál fue el OEE de la semana del 25 al 31 de agosto de 2026?",
         tiene(GS["OEE"], 1.0), "OEE semanal ponderado"),
        ("¿Y qué día fue el peor?", dice("25"), "el peor fue el 25"),
        ("¿Y el mejor?", dice("27"), "el mejor fue el 27"),
        ("¿La planta cumplió el plan esa semana?",
         dice("super", "por encima", "cumpl", "excedi"), "si lo supero"),
    ]),
    ("Cambio de tema a media conversacion", [
        ("Dame el OEE del 31 de agosto de 2026", tiene(GD["OEE"], 0.7), "OEE del dia"),
        ("Ahora dime el estado actual de la línea",
         dice("oee", "paro", "produc"), "responde tiempo real"),
        ("¿Cuánto tiempo productivo hubo ese día?",
         dice("13 horas", "836"), "836 min = 13 horas y 56 minutos"),
        ("Dame el OEE de la línea de salchichas de ese día",
         dice("no existe", "hamburguesas", "no hay", "única"), "esa linea no existe"),
    ]),
]


def pedir(metodo, ruta, cuerpo=None, espera=300):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(SERVIDOR + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=espera) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, {}


def limpiar():
    """Borra las conversaciones de prueba para no ensuciar el historial real."""
    st, d = pedir("GET", "/chat/threads/%s?owner_key=%s" % (USUARIO, urllib.parse.quote(CLAVE)))
    for t in d.get("threads", []):
        pedir("DELETE", "/chat/threads/%s?owner_key=%s" % (t["thread_id"], urllib.parse.quote(CLAVE)))


def correr():
    st, _ = pedir("GET", "/health")
    if st != 200:
        print("El servidor %s no responde. Levantalo antes de correr esta bateria." % SERVIDOR)
        return 1

    total = fallos = 0
    detalle = []
    for titulo, turnos in CONVERSACIONES:
        print("\n" + "=" * 78)
        print("CONVERSACION: %s" % titulo)
        print("=" * 78)
        hilo = None
        for i, (pregunta, comprobar, nota) in enumerate(turnos, 1):
            t0 = time.time()
            st, r = pedir("POST", "/chat/", {"input": pregunta, "thread_id": hilo,
                                             "username": USUARIO, "owner_key": CLAVE,
                                             "lang": "es"})
            hilo = r.get("thread_id") or hilo
            texto = r.get("message") or ""
            ok = bool(comprobar(texto))
            total += 1
            if not ok:
                fallos += 1
                detalle.append((titulo, i, pregunta, nota, texto[:280]))
            print("  turno %d  %-5s  %-50s (%.0fs, %d graf)  [%s]" % (
                i, "OK" if ok else "FALLA", pregunta[:50], time.time() - t0,
                len(r.get("images") or []), nota))

    print("\n" + "=" * 78)
    print("RESULTADO CONVERSACIONAL: %d/%d turnos OK" % (total - fallos, total))
    for t, i, p, n, resp in detalle:
        print("\nFALLA [%s] turno %d: %s" % (t, i, p))
        print("   esperado: %s" % n)
        print("   obtuvo  : %s" % resp.replace("\n", " ")[:200])

    limpiar()
    return fallos


if __name__ == "__main__":
    sys.exit(1 if correr() else 0)
