# -*- coding: utf-8 -*-
"""
Revision diaria del piloto: que preguntaron de verdad y que contesto Duma.

Las baterias solo prueban lo que ya se nos ocurrio. De los cinco defectos del 3 de
septiembre, cuatro salieron de usar el agente a mano, no de las pruebas. Este script
lee el uso real -que queda entero en dbo.duma_messages- y marca lo que merece una
mirada humana, para no tener que leer cien conversaciones a ojo.

    python scripts/revision_piloto.py                  # el dia de hoy
    python scripts/revision_piloto.py 2026-09-04       # un dia concreto
    python scripts/revision_piloto.py 2026-09-04 --todo  # sin filtrar, todo el dia

No marca "errores": marca SOSPECHAS. Cada una hay que confirmarla a mano; el valor esta
en reducir de cien conversaciones a diez las que hay que leer.
"""
import os
import sys
import io
import re
import json
from datetime import datetime, timedelta

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyodbc  # noqa: E402
import main as duma  # noqa: E402

# Los usuarios del piloto no se declaran en ningun lado: salen de la propia base. Aqui
# solo se listan los mios, y el informe SIEMPRE dice a quien excluyo, porque una lista
# negra esconde lo que se te olvida meter (Deploy_Check paso por usuario real hasta que
# lo vi de casualidad).
USUARIOS_DE_PRUEBA = ("QA_", "Test", "Deploy_Check")

# "Anonimo" es el nombre por defecto cuando nadie lo escribio: son personas reales, y de
# hecho el grupo mas activo. Cuentan como uso real, pero se marcan aparte porque sus
# conversaciones no se pueden atribuir ni seguir con nadie.
SIN_NOMBRE = ("Anónimo", "Anonimo")

# Una respuesta con cifras que no consulto nada es la senal mas peligrosa: el modelo
# puede estar recitando de memoria del turno anterior en vez de mirar los datos.
RE_CIFRA = re.compile(r"\d[\d,]*\.?\d*\s*(%|kg|min|horas|minutos)", re.IGNORECASE)
RE_SQL_FILTRADO = re.compile(r"\bSELECT\b[\s\S]{0,200}\bFROM\b", re.IGNORECASE)


def conversaciones_del_dia(dia):
    """Devuelve [(thread_id, usuario, titulo, [(rol, texto, hora), ...]), ...]."""
    ini = datetime.strptime(dia, "%Y-%m-%d")
    fin = ini + timedelta(days=1)
    salida = []
    with pyodbc.connect(duma.HISTORY_CONN_STR) as cn:
        cur = cn.cursor()
        cur.execute("""
            SELECT c.thread_id, c.user_name, c.title
            FROM dbo.duma_conversations c
            WHERE EXISTS (SELECT 1 FROM dbo.duma_messages m
                          WHERE m.thread_id = c.thread_id
                            AND m.created_at >= ? AND m.created_at < ?)
            ORDER BY c.created_at
        """, (ini, fin))
        hilos = cur.fetchall()
        for tid, usuario, titulo in hilos:
            cur.execute("""
                SELECT role, text, created_at FROM dbo.duma_messages
                WHERE thread_id = ? AND created_at >= ? AND created_at < ?
                ORDER BY message_id
            """, (tid, ini, fin))
            salida.append((tid, usuario or "?", titulo or "", list(cur.fetchall())))
    return salida


def sospechas(turnos):
    """
    Motivos por los que esta conversacion merece una lectura humana.

    Cada regla nacio de un defecto real, y va comentada con cual.
    """
    marcas = []
    for i, (rol, texto, _) in enumerate(turnos):
        if rol != "assistant":
            continue
        t = texto or ""
        bajo = t.lower()
        pregunta = turnos[i - 1][1] if i and turnos[i - 1][0] == "user" else ""

        # El guardarrail de alcance declinando algo que parece de planta. Bloquear una
        # pregunta legitima es peor que responder una ajena, y ya paso dos veces.
        if duma.respuesta_fuera_de_alcance(t):
            marcas.append(("declino", pregunta))

        # Codigo o SQL en la respuesta: el segundo defecto que reportaron los clientes.
        if "```" in t or RE_SQL_FILTRADO.search(t):
            marcas.append(("suelta codigo o SQL", pregunta))

        # Fallo controlado: el usuario vio un mensaje de error.
        if "no pude completar" in bajo or "vuelve a intentarlo" in bajo:
            marcas.append(("error controlado", pregunta))

        # Respuesta larga a una pregunta corta: el cuarto defecto de los clientes.
        if len(t) > 2500 and len(pregunta) < 120 and "informe" not in pregunta.lower():
            marcas.append(("respuesta de %d caracteres a una pregunta corta" % len(t), pregunta))

        # Duda sobre datos: si dice que no hay datos, conviene comprobar que es cierto.
        if any(f in bajo for f in ("no hay registros", "no se registraron",
                                   "no encontre", "no encontré", "sin datos")):
            marcas.append(("dice que no hay datos", pregunta))

        # Cifras sin consultar nada. Solo aplica si en toda la conversacion no hubo
        # ninguna grafica ni tabla: es una heuristica floja, por eso va la ultima.
        if RE_CIFRA.search(t) and len(t) < 400 and i > 2:
            pass  # demasiado ruidosa por si sola; se deja documentada, no activa
    return marcas


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    todo = "--todo" in sys.argv
    dia = args[0] if args else datetime.now().strftime("%Y-%m-%d")

    convs = conversaciones_del_dia(dia)
    es_prueba = lambda u: any(u.startswith(p) for p in USUARIOS_DE_PRUEBA)
    reales = [c for c in convs if not es_prueba(c[1])]

    print("REVISION DEL PILOTO — %s" % dia)
    print("=" * 78)
    print("conversaciones: %d (%d de usuarios reales, %d de pruebas)"
          % (len(convs), len(reales), len(convs) - len(reales)))

    usuarios, preguntas = {}, 0
    for _, usuario, _, turnos in reales:
        n = sum(1 for r, _, _ in turnos if r == "user")
        preguntas += n
        usuarios[usuario] = usuarios.get(usuario, 0) + n
    print("preguntas de usuarios reales: %d" % preguntas)
    for u, n in sorted(usuarios.items(), key=lambda x: -x[1]):
        nota = "  <- sin nombre: no se puede saber quien pregunto" if u in SIN_NOMBRE else ""
        print("   %-28s %3d preguntas%s" % (u, n, nota))

    # Siempre a la vista: si algun dia excluyo por error a alguien de verdad, se ve aqui
    # en vez de desaparecer en silencio.
    excluidos = sorted({c[1] for c in convs if es_prueba(c[1])})
    if excluidos:
        print("excluidos por ser de prueba: %s" % ", ".join(excluidos))

    print("\nPARA REVISAR A MANO")
    print("=" * 78)
    revisar = 0
    for tid, usuario, titulo, turnos in reales:
        marcas = [] if todo else sospechas(turnos)
        if not marcas and not todo:
            continue
        revisar += 1
        print("\n[%s] %s — %s" % (usuario, titulo[:50], tid[-8:]))
        for motivo, pregunta in marcas:
            print("   ! %-46s  <- %s" % (motivo, (pregunta or "")[:60]))
        for rol, texto, hora in turnos:
            quien = "USUARIO" if rol == "user" else "DUMA   "
            print("   %s %s | %s" % (hora.strftime("%H:%M"), quien,
                                     (texto or "").replace("\n", " ")[:150]))

    print("\n" + "=" * 78)
    if todo:
        print("Mostradas las %d conversaciones reales del dia." % len(reales))
    else:
        print("%d de %d conversaciones piden una mirada. Con --todo se ven todas."
              % (revisar, len(reales)))


if __name__ == "__main__":
    main()
