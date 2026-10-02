# -*- coding: utf-8 -*-
"""
Comprueba que el ETL no miente: cuenta un dia en el origen y lo compara.

POR QUE ESTO EXISTE. El ETL agrupa, convierte husos y arma sesiones; cualquiera
de los tres pasos puede estar mal sin que nada falle ni avise. La unica forma de
saberlo es contar el mismo dia en los dos lados y ver si cuadra.

    python verificar.py 2026-09-30

TARDA. La consulta al origen es un barrido completo -no hay indice utilizable
sobre RequestDate- asi que cuenta con varios minutos. Es el precio de una sola
comprobacion contra la tranquilidad de que el informe dice la verdad. No hace
falta correrla a diario: una vez al empezar, y otra si algo huele raro.

QUE NO COMPRUEBA. Que las sesiones esten bien cortadas: eso no tiene con que
contrastarse, porque el origen no registra sesiones. Lo que si comprueba es lo
falsificable -peticiones, usuarios distintos, logins-, y si eso cuadra, el resto
del proceso leyó las mismas filas.
"""
import argparse
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import etl  # noqa: E402


def contar_en_origen(cn, desde, hasta):
    cur = cn.cursor()
    cur.execute("""
        SELECT COUNT(*) AS peticiones,
               COUNT(DISTINCT UserMail) AS usuarios,
               SUM(CASE WHEN Module = 'login' THEN 1 ELSE 0 END) AS logins
        FROM dbo.SystemLogs
        WHERE RequestDate >= ? AND RequestDate < ?""", desde, hasta)
    return cur.fetchone()


def contar_en_resumen(cn, fecha):
    cur = cn.cursor()
    cur.execute("""
        SELECT (SELECT ISNULL(SUM(peticiones), 0) FROM dbo.uso_diario WHERE fecha = ?),
               (SELECT COUNT(DISTINCT usuario) FROM dbo.uso_diario WHERE fecha = ?),
               (SELECT COUNT(*) FROM dbo.logins WHERE fecha = ?)""",
                fecha, fecha, fecha)
    return cur.fetchone()


def main(argv=None):
    p = argparse.ArgumentParser(description="Compara un dia contra el origen.")
    p.add_argument("dia", help="AAAA-MM-DD, en hora de planta")
    a = p.parse_args(argv)

    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    etl.ZONA = etl._zona()

    fecha = dt.datetime.strptime(a.dia, "%Y-%m-%d").date()

    # Un dia que todavia no termina no se puede comparar: el sistema sigue
    # escribiendo entre que el ETL lee y esta consulta cuenta, asi que el origen
    # siempre tendra algunas filas de mas. Se midio: con tres minutos de diferencia
    # sobraban 7, con segundos sobraba 1. No es un defecto, es que el dia esta vivo.
    hoy = dt.datetime.now(etl.ZONA).date()
    if fecha >= hoy:
        print("")
        print("AVISO: el %s todavia no termina en planta (hoy es %s)."
              % (fecha, hoy))
        print("El origen va a tener filas de mas, las que se escriban mientras")
        print("comparamos. Para una comprobacion limpia, usa un dia ya cerrado.")
        print("")

    # La ventana en UTC que corresponde a ese dia LOCAL. Calcularla igual que el
    # ETL es la mitad del valor de la prueba: si el huso estuviera mal en los dos
    # sitios, cuadrarian igual y no nos enterariamos. Por eso abajo se imprime
    # tambien la ventana, para poder mirarla con los ojos.
    inicio = dt.datetime.combine(fecha, dt.time.min, tzinfo=etl.ZONA)
    fin = inicio + dt.timedelta(days=1)
    desde_utc = inicio.astimezone(dt.timezone.utc).replace(tzinfo=None)
    hasta_utc = fin.astimezone(dt.timezone.utc).replace(tzinfo=None)

    print("Dia local      : %s" % fecha)
    print("Ventana en UTC : %s  a  %s" % (desde_utc, hasta_utc))
    print("Leyendo el origen (barrido completo, varios minutos)...")
    sys.stdout.flush()

    cn_o = etl.conectar_origen()
    origen = contar_en_origen(cn_o, desde_utc, hasta_utc)
    cn_o.close()

    cn_d = etl.conectar_destino()
    resumen = contar_en_resumen(cn_d, fecha)
    cn_d.close()

    nombres = ("peticiones", "usuarios distintos", "logins")
    print("")
    print("%-20s %12s %12s   %s" % ("", "origen", "resumen", ""))
    print("-" * 60)
    todo_bien = True
    for nombre, a_, b_ in zip(nombres, origen, resumen):
        a_ = a_ or 0
        b_ = b_ or 0
        igual = (a_ == b_)
        todo_bien = todo_bien and igual
        print("%-20s %12d %12d   %s"
              % (nombre, a_, b_, "ok" if igual else "NO CUADRA"))

    print("")
    if todo_bien:
        print("Cuadra. El resumen de ese dia dice lo mismo que el origen.")
        return 0
    print("NO cuadra. No uses el informe hasta entender por que.")
    print("Lo primero que miraria: que el dia este procesado (etl_control),")
    print("y que la ventana en UTC de arriba sea la que esperabas.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
