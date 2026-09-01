# -*- coding: utf-8 -*-
"""
Bateria de calidad del agente Duma: 55 preguntas verificadas contra el oraculo.

Cada caso declara comprobaciones automaticas. Las numericas buscan la cifra en la
respuesta con tolerancia; asi no se penaliza el redondeo ni la redaccion, pero si se
detecta una cifra equivocada o inventada.
"""
import os, sys, io, json, re, time, traceback
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import main

BASE = os.path.dirname(os.path.abspath(__file__))
O = json.load(io.open(BASE + r"\oraculo.json", encoding="utf-8"))
GD, GS = O["global_dia"], O["global_semana"]
TD = {t["Turno"]: t for t in O["turnos_dia"]}
PD, PS = O["paros_dia"], O["paros_semana"]

DIA = O["dia"]                 # 2026-08-31
NUM = re.compile(r"-?\d[\d,.]*")


def numeros(texto):
    """Todos los numeros del texto, normalizando separadores de miles."""
    salida = []
    for bruto in NUM.findall(texto or ""):
        t = bruto.rstrip(".,")
        # 20,300.2 -> 20300.2 ; 1.687 (miles) se deja como esta y se prueban ambas
        candidatos = {t.replace(",", "")}
        if "," in t and "." not in t:
            candidatos.add(t.replace(",", "."))
        for c in candidatos:
            try:
                salida.append(float(c))
            except ValueError:
                pass
    return salida


# ---------------------------------------------------------------- comprobaciones
def num(valor, tol=None, etiqueta=""):
    """La respuesta debe contener esta cifra."""
    def check(r):
        if valor is None:
            return True, "sin referencia"
        t = tol if tol is not None else max(abs(valor) * 0.02, 0.6)
        hallados = numeros(r["message"])
        ok = any(abs(n - valor) <= t for n in hallados)
        return ok, "%s=%s%s" % (etiqueta or "valor", valor, "" if ok else " NO APARECE")
    return check


def duracion_min(valor, etiqueta=""):
    """Acepta el valor en minutos o expresado en horas y minutos."""
    def check(r):
        hallados = numeros(r["message"])
        horas, mins = divmod(int(round(valor)), 60)
        ok = (any(abs(n - valor) <= max(valor * 0.02, 1) for n in hallados)
              or (any(abs(n - horas) <= 0.2 for n in hallados)
                  and any(abs(n - mins) <= 1 for n in hallados))
              or any(abs(n - valor / 60.0) <= 0.15 for n in hallados))
        return ok, "%s=%s min%s" % (etiqueta or "duracion", valor, "" if ok else " NO APARECE")
    return check


def no_num(valor, tol=None, etiqueta=""):
    """La respuesta NO debe contener esta cifra (tipicamente una equivocada)."""
    def check(r):
        t = tol if tol is not None else max(abs(valor) * 0.01, 0.3)
        malo = any(abs(n - valor) <= t for n in numeros(r["message"]))
        return not malo, "no debe decir %s%s" % (valor, " PERO LO DICE" if malo else "")
    return check


def contiene(*claves):
    def check(r):
        t = (r["message"] or "").lower()
        faltan = [c for c in claves if c.lower() not in t]
        return not faltan, "menciona %s" % (claves,) if not faltan else "falta %s" % faltan
    return check


def alguna(*claves):
    def check(r):
        t = (r["message"] or "").lower()
        ok = any(c.lower() in t for c in claves)
        return ok, "alguna de %s" % (claves,)
    return check


def no_contiene(*claves):
    def check(r):
        t = (r["message"] or "").lower()
        malas = [c for c in claves if c.lower() in t]
        return not malas, "no dice %s" % (claves,) if not malas else "DICE %s" % malas
    return check


def graficas(minimo=1):
    def check(r):
        n = len(r.get("images") or [])
        return n >= minimo, "graficas>=%d (hubo %d)" % (minimo, n)
    return check


def sin_graficas():
    def check(r):
        n = len(r.get("images") or [])
        return n == 0, "sin graficas (hubo %d)" % n
    return check


def sin_codigo():
    def check(r):
        t = r["message"] or ""
        malo = "```" in t or re.search(r"\bSELECT\b[\s\S]{0,200}\bFROM\b", t, re.I)
        return not malo, "sin codigo ni SQL" + (" PERO LO TRAE" if malo else "")
    return check


def maximo(chars):
    def check(r):
        n = len(r["message"] or "")
        return n <= chars, "largo<=%d (fue %d)" % (chars, n)
    return check


def tabla():
    def check(r):
        t = r["message"] or ""
        filas = [l for l in t.splitlines() if l.strip().startswith("|") and l.count("|") >= 3]
        return len(filas) >= 3, "tabla markdown (%d filas)" % len(filas)
    return check


def dice_sin_datos():
    def check(r):
        t = (r["message"] or "").lower()
        ok = any(f in t for f in ["no hay", "no existen", "sin datos", "no se registr",
                                  "no cuento con", "no dispongo", "no encontr", "no hubo",
                                  "no tengo", "fuera del rango de datos", "no es posible"])
        return ok, "declara ausencia de datos"
    return check


