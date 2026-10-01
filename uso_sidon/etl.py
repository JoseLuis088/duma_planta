# -*- coding: utf-8 -*-
"""
Resume el uso de Sidon Industrial: lee dbo.SystemLogs y escribe Sidon_Uso.

POR QUE EXISTE. La tabla de origen tiene 1,986,171 filas y sus columnas de texto
son nvarchar(max), que SQL Server no puede indexar. Eso hace que cualquier
consulta que agrupe por usuario o por modulo tenga que leerla entera: se midio,
y tarda mas de quince minutos. Un informe que tarda eso no lo abre nadie dos
veces. Este proceso paga ese costo UNA VEZ AL DIA, de madrugada, y deja el
resultado en tablas chicas donde el informe responde al instante.

COMO SE CORRE

    python etl.py                      # desde donde se quedo hasta ayer
    python etl.py --desde 2026-01-01   # primera corrida o relleno historico
    python etl.py --probar             # lee y reporta, no escribe nada

En la VM, una vez al dia:

    docker exec duma_planta python /usr/local/app/uso_sidon/etl.py

DOS DECISIONES QUE EXPLICAN EL CODIGO

  Se pide ORDER BY UserMail, RequestDate. Cuesta una ordenacion en el servidor,
  pero permite armar las sesiones sobre la marcha sin guardar en memoria dos
  millones de marcas de tiempo. Este proceso comparte contenedor con Duma; no
  puede permitirse crecer cientos de megas.

  Se escribe hacia adelante y no se reconstruye nunca. Hay indicios de que el
  origen purga bitacora vieja, asi que estas tablas son el unico sitio donde
  esos meses van a seguir existiendo. Reconstruirlas desde la fuente manana
  borraria justo lo que la fuente ya no tiene.
"""
import argparse
import datetime as dt
import logging
import os
import re
import sys
import time

import pyodbc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROCESO = "uso_sidon"

# El corte entre una sesion y la siguiente. Media hora sin pedir nada se
# considera que la persona se fue, aunque no haya cerrado sesion.
HUECO = dt.timedelta(minutes=30)

# RequestDate viene en UTC -confirmado contra el sistema el 1 oct 2026- y se
# pregunta en hora de planta ("quien entro el martes temprano"). Se guardan las
# dos: el dia local para agrupar, el instante UTC por si la conversion resulta
# estar mal y hay que recalcular sin volver a tocar la fuente.
HUSO_PLANTA = os.getenv("USO_HUSO", "America/Mexico_City")

_RE_CODIGO = re.compile(r'"StatusCode"\s*:\s*(\d+)')

log = logging.getLogger("uso_sidon")


def _zona():
    """La zona de planta, o un desfase fijo si la imagen no trae tzdata.

    Importa que no reviente: sin conversion correcta las horas del informe
    saldrian corridas seis horas, que es justo el dato que se pidio.
    """
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(HUSO_PLANTA)
    except Exception as e:
        log.warning("sin tzdata para %s (%s); se usa UTC-6 fijo", HUSO_PLANTA, e)
        return dt.timezone(dt.timedelta(hours=-6))


ZONA = None  # se resuelve en main(), despues de configurar el log


def local(momento_utc):
    """Un instante UTC de la fuente, visto en hora de planta."""
    return momento_utc.replace(tzinfo=dt.timezone.utc).astimezone(ZONA)


def _cadena(servidor, base, usuario, clave):
    return ("DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;"
            "Encrypt=yes;TrustServerCertificate=yes;Connect Timeout=60;" % (
                os.getenv("SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
                servidor, base, usuario, clave))


def conectar_origen():
    """La base de Sidon. Solo se lee de aqui, nunca se escribe."""
    return pyodbc.connect(_cadena(
        os.environ["SQL_SERVER"], os.environ["SQL_DB"],
        os.environ["SQL_USER"], os.environ["SQL_PASS"]), timeout=60)


def conectar_destino():
    """Sidon_Uso, en la VM. Usuario propio, no `sa`."""
    return pyodbc.connect(_cadena(
        os.getenv("USO_SQL_SERVER", os.getenv("HISTORY_SQL_SERVER", "172.168.10.106")),
        os.getenv("USO_SQL_DB", "Sidon_Uso"),
        os.environ["USO_SQL_USER"], os.environ["USO_SQL_PASS"]),
        timeout=60, autocommit=False)


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------

# Cuatro columnas y media. `Body` es la columna que hace pesada la tabla -es la
# que tiene a cualquier consulta esperando minutos- pero en las filas de login
# trae el StatusCode, que separa una entrada lograda de un intento fallido. El
# CASE se lleva treinta caracteres de esas filas y de ninguna otra.
CONSULTA = """
SELECT UserMail, Module, RequestDate, RequestIp,
       CASE WHEN Module = 'login' THEN LEFT(Body, 40) END AS estado
FROM dbo.SystemLogs
WHERE RequestDate >= ? AND RequestDate < ?
ORDER BY UserMail, RequestDate
"""


class Sesion:
    """Un tramo de actividad continua de una persona."""

    __slots__ = ("usuario", "inicio", "fin", "peticiones", "modulos", "abrio")

    def __init__(self, usuario, momento, modulo):
        self.usuario = usuario
        self.inicio = momento
        self.fin = momento
        self.peticiones = 1
        self.modulos = {modulo}
        # Si el primer movimiento del tramo fue un login, la sesion empezo de
        # verdad ahi. Si no, el tramo viene de antes: la persona ya estaba
        # dentro, o la ventana que procesamos lo parte por la mitad.
        self.abrio = (modulo == "login")

    def sumar(self, momento, modulo):
        self.fin = momento
        self.peticiones += 1
        self.modulos.add(modulo)

    def fila(self):
        minutos = int((self.fin - self.inicio).total_seconds() // 60)
        return (self.usuario, self.inicio, self.fin,
                local(self.inicio).date(), minutos,
                self.peticiones, len(self.modulos), int(self.abrio))


def leer_y_resumir(cn_origen, desde, hasta):
    """Una pasada por la ventana. Devuelve (uso, logins, sesiones, cuentas, filas).

    Se consume en streaming: la consulta devuelve las filas ordenadas por usuario
    y momento, asi que las sesiones se cierran sobre la marcha y en memoria no
    vive mas que la sesion en curso.
    """
    uso = {}          # (fecha, usuario, modulo) -> [peticiones, primera, ultima, ips]
    logins = []
    sesiones = []
    cuentas = {}      # usuario -> [primer dia visto, ultimo dia visto]
    abierta = None
    filas = 0

    cur = cn_origen.cursor()
    cur.execute(CONSULTA, desde, hasta)
    while True:
        lote = cur.fetchmany(10000)
        if not lote:
            break
        for usuario, modulo, momento, ip, estado in lote:
            filas += 1
            usuario = (usuario or "").strip()[:200]
            modulo = (modulo or "").strip()[:200]
            fecha = local(momento).date()

            clave = (fecha, usuario, modulo)
            fila = uso.get(clave)
            if fila is None:
                uso[clave] = [1, momento, momento, {ip}]
            else:
                fila[0] += 1
                if momento < fila[1]:
                    fila[1] = momento
                if momento > fila[2]:
                    fila[2] = momento
                fila[3].add(ip)

            visto = cuentas.get(usuario)
            if visto is None:
                cuentas[usuario] = [fecha, fecha]
            else:
                if fecha < visto[0]:
                    visto[0] = fecha
                if fecha > visto[1]:
                    visto[1] = fecha

            if modulo == "login":
                codigo = None
                if estado:
                    m = _RE_CODIGO.search(estado)
                    if m:
                        codigo = int(m.group(1))
                momento_local = local(momento)
                logins.append((usuario, momento, momento_local.date(),
                               momento_local.time().replace(microsecond=0),
                               (ip or "")[:64] or None, codigo,
                               int(codigo == 200) if codigo is not None else 0))

            if (abierta is None or abierta.usuario != usuario
                    or momento - abierta.fin > HUECO):
                if abierta is not None:
                    sesiones.append(abierta.fila())
                abierta = Sesion(usuario, momento, modulo)
            else:
                abierta.sumar(momento, modulo)

    if abierta is not None:
        sesiones.append(abierta.fila())

    filas_uso = [(f, u, m, v[0], v[1], v[2], len(v[3]))
                 for (f, u, m), v in uso.items()]
    return filas_uso, logins, sesiones, cuentas, filas


# ---------------------------------------------------------------------------
# Escritura
# ---------------------------------------------------------------------------

def escribir(cn_destino, desde_fecha, hasta_fecha, filas_uso, logins, sesiones,
             cuentas, marca, filas_leidas, segundos):
    """Todo o nada.

    Borra los dias de ESTA ventana y vuelve a insertarlos, para que repetir una
    corrida de un dia de el mismo resultado y no lo duplique. Nunca toca dias
    anteriores: ahi vive la historia que el origen puede haber borrado ya.
    """
    cur = cn_destino.cursor()
    cur.fast_executemany = True

    for tabla in ("uso_diario", "logins", "sesiones"):
        cur.execute("DELETE FROM dbo.%s WHERE fecha >= ? AND fecha <= ?"
                    % tabla, desde_fecha, hasta_fecha)

    if filas_uso:
        cur.executemany(
            "INSERT INTO dbo.uso_diario (fecha, usuario, modulo, peticiones,"
            " primera_utc, ultima_utc, ips) VALUES (?,?,?,?,?,?,?)", filas_uso)
    if logins:
        cur.executemany(
            "INSERT INTO dbo.logins (usuario, momento_utc, fecha, hora_local,"
            " ip, codigo, exitoso) VALUES (?,?,?,?,?,?,?)", logins)
    if sesiones:
        cur.executemany(
            "INSERT INTO dbo.sesiones (usuario, inicio_utc, fin_utc, fecha,"
            " minutos, peticiones, modulos, abrio_sesion)"
            " VALUES (?,?,?,?,?,?,?,?)", sesiones)

    # Cuentas nuevas: se dan de alta como persona y marcadas, porque equivocarse
    # contando de mas a una maquina es peor que pedirle a alguien que mire. Las
    # que ya existen solo actualizan el rango de dias en que se las ha visto.
    for usuario, (primero, ultimo) in cuentas.items():
        cur.execute("""
            MERGE dbo.cuentas AS d
            USING (SELECT ? AS usuario, ? AS primero, ? AS ultimo) AS o
            ON d.usuario = o.usuario
            WHEN MATCHED THEN UPDATE SET
                visto_desde = CASE WHEN d.visto_desde IS NULL OR o.primero < d.visto_desde
                                   THEN o.primero ELSE d.visto_desde END,
                visto_hasta = CASE WHEN d.visto_hasta IS NULL OR o.ultimo > d.visto_hasta
                                   THEN o.ultimo ELSE d.visto_hasta END
            WHEN NOT MATCHED THEN INSERT (usuario, es_persona, nota, visto_desde, visto_hasta)
                VALUES (o.usuario, 1, N'Cuenta nueva, sin revisar', o.primero, o.ultimo);
        """, usuario, primero, ultimo)

    ahora = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    cur.execute("""
        MERGE dbo.etl_control AS d
        USING (SELECT ? AS proceso) AS o ON d.proceso = o.proceso
        WHEN MATCHED THEN UPDATE SET
            procesado_hasta_utc = ?, corrida_utc = ?, filas_leidas = ?,
            segundos = ?, resultado = ?
        WHEN NOT MATCHED THEN INSERT (proceso, procesado_hasta_utc, corrida_utc,
            filas_leidas, segundos, resultado) VALUES (o.proceso, ?, ?, ?, ?, ?);
    """, PROCESO, marca, ahora, filas_leidas, segundos, "ok",
         marca, ahora, filas_leidas, segundos, "ok")

    cn_destino.commit()


def marca_anterior(cn_destino):
    cur = cn_destino.cursor()
    cur.execute("SELECT procesado_hasta_utc FROM dbo.etl_control WHERE proceso = ?",
                PROCESO)
    f = cur.fetchone()
    return f[0] if f else None


# ---------------------------------------------------------------------------

def corte_de_hoy():
    """El inicio del dia local de hoy, en UTC.

    No se procesa el dia en curso: quedaria a medias y manana habria que
    reescribirlo. El informe mira hasta ayer.
    """
    hoy = dt.datetime.now(ZONA).date()
    inicio = dt.datetime.combine(hoy, dt.time.min, tzinfo=ZONA)
    return inicio.astimezone(dt.timezone.utc).replace(tzinfo=None)


def main(argv=None):
    p = argparse.ArgumentParser(description="Resume el uso de Sidon Industrial.")
    p.add_argument("--desde", help="AAAA-MM-DD. Obligatorio la primera vez.")
    p.add_argument("--hasta", help="AAAA-MM-DD exclusivo. Por omision, hoy.")
    p.add_argument("--dias", type=int, default=400,
                   help="Tamano del bloque. El relleno historico cabe en uno"
                        " solo: sin indice en el origen, dos bloques cuestan dos"
                        " barridos completos en vez de uno.")
    p.add_argument("--probar", action="store_true",
                   help="Lee y reporta sin escribir nada.")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    global ZONA
    ZONA = _zona()

    hasta = (dt.datetime.strptime(a.hasta, "%Y-%m-%d") if a.hasta
             else corte_de_hoy())

    cn_destino = conectar_destino()
    desde = marca_anterior(cn_destino)
    if a.desde:
        desde = dt.datetime.strptime(a.desde, "%Y-%m-%d")
    if desde is None:
        log.error("Primera corrida: hay que decir desde cuando, con --desde.")
        return 2

    if desde >= hasta:
        log.info("Nada nuevo: ya esta procesado hasta %s.", desde)
        return 0

    cn_origen = conectar_origen()
    total_filas = 0
    bloque = dt.timedelta(days=a.dias)

    while desde < hasta:
        fin = min(desde + bloque, hasta)
        log.info("Leyendo de %s a %s (UTC)...", desde, fin)
        t0 = time.time()
        filas_uso, logins, sesiones, cuentas, filas = leer_y_resumir(
            cn_origen, desde, fin)
        segundos = int(time.time() - t0)
        total_filas += filas

        log.info("%d filas leidas en %d s -> %d renglones de uso, %d logins,"
                 " %d sesiones, %d cuentas",
                 filas, segundos, len(filas_uso), len(logins), len(sesiones),
                 len(cuentas))

        if a.probar:
            log.info("--probar: no se escribe nada.")
        elif filas:
            escribir(cn_destino, local(desde).date(),
                     local(fin - dt.timedelta(seconds=1)).date(),
                     filas_uso, logins, sesiones, cuentas, fin, filas, segundos)
            log.info("Escrito. Procesado hasta %s.", fin)
        else:
            log.info("Sin filas en la ventana; no se escribe.")

        desde = fin

    cn_origen.close()
    cn_destino.close()
    log.info("Listo. %d filas leidas en total.", total_filas)
    return 0


if __name__ == "__main__":
    sys.exit(main())
