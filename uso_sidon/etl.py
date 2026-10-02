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
# Las tres columnas de texto del origen son nvarchar(max), y eso no es solo un
# problema de indices: SQL Server las trata como objetos grandes, asi que ordenar
# dos millones de filas que las arrastran es carisimo. La primera corrida tardo 43
# minutos y la segunda paso de una hora sin devolver ni una fila.
#
# El CAST las acota ANTES de ordenar. Los datos no cambian -ningun correo mide 200
# caracteres, ninguna IP mide 64- pero el servidor pasa a ordenar filas compactas
# de ancho fijo en vez de arrastrar objetos grandes. Es la misma leccion que nos
# dio esta tabla, aplicada a la consulta porque el diseno no lo podemos tocar.
#
# El ORDER BY repite el CAST a proposito: ordenar por la columna original volveria
# a meter el objeto grande en la ordenacion y no habriamos ganado nada.
CONSULTA = """
SELECT RegisterId,
       UserId,
       CAST(UserMail  AS NVARCHAR(100)) AS UserMail,
       CAST(Module    AS NVARCHAR(100)) AS Module,
       CAST(Route     AS NVARCHAR(200)) AS Route,
       RequestDate,
       CAST(RequestIp AS NVARCHAR(64))  AS RequestIp,
       CASE WHEN Module = 'login' THEN CAST(LEFT(Body, 40) AS NVARCHAR(40)) END AS estado
FROM dbo.SystemLogs
WHERE RequestDate >= ? AND RequestDate < ?
ORDER BY CAST(UserMail AS NVARCHAR(100)), RequestDate
"""

# Las rutas traen identificadores: GET /api/productionLines/a1a5d0ea-edb4-...
# Agrupar por la ruta cruda daria una fila por recurso -miles- en vez de una por
# pantalla. Se sustituyen por {id} para que queden unas pocas decenas de rutas
# legibles, que es lo que el tablero necesita ensenar.
#
# Hace falta porque Module no sirve: la mitad de sus valores SON los GUIDs.
_RE_GUID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{6,14}", re.I)
_RE_NUMERO = re.compile(r"/\d+(?=/|$)")


def normalizar_ruta(ruta):
    """La ruta sin sus identificadores, para poder agrupar por pantalla."""
    limpia = _RE_GUID.sub("{id}", (ruta or "").strip())
    return _RE_NUMERO.sub("/{n}", limpia)[:200]

# Hay filas de login sin correo: intentos donde no se llego a identificar a nadie.
# Se guardan bajo este nombre en vez de como cadena vacia, para que en el informe
# se vean como lo que son y no se confundan con una persona.
SIN_NOMBRE = "(sin identificar)"


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
    uso = {}          # (fecha, usuario, modulo, ruta) -> [peticiones, 1a, ultima, ips]
    logins = []
    sesiones = []
    cuentas = {}      # usuario -> [primer dia visto, ultimo dia visto]
    # UserId -> correo, armado con las filas que traen los dos. Sirve para poner
    # nombre a los logins, que casi nunca lo traen: en el momento del POST de
    # login el sistema todavia no sabe quien eres -se lo estas preguntando- asi
    # que escribe la fila sin correo. El mismo UserId si aparece con correo en
    # las miles de peticiones ya autenticadas de esa persona.
    correo_de = {}
    abierta = None
    filas = 0

    # Avisar cada cien mil filas. Sin esto el proceso calla durante toda la lectura
    # -una hora larga en el relleno historico- y no hay forma de distinguir "va
    # lento" de "se atoro": paso en la primera corrida real y hubo que adivinarlo
    # por el ritmo de una prueba anterior.
    AVISO_CADA = 100000
    siguiente_aviso = AVISO_CADA
    arranque = time.time()

    # La consulta pide ORDER BY, y SQL Server no entrega la primera fila hasta
    # haber ordenado el resultado entero. Esa espera es la parte larga -de los 43
    # minutos de la primera corrida, la mayoria- y durante ella no hay ni una fila
    # que contar, asi que el aviso de cada cien mil no sirve de nada. Al menos que
    # se vea en que fase esta: esperando al servidor, o ya procesando.
    log.info("   esperando a que el servidor ordene el resultado...")
    cur = cn_origen.cursor()
    cur.execute(CONSULTA, desde, hasta)
    log.info("   el servidor empezo a entregar filas tras %d s. Procesando...",
             int(time.time() - arranque))

    while True:
        lote = cur.fetchmany(10000)
        if not lote:
            break
        for (registro, usuario_id, usuario, modulo, ruta,
             momento, ip, estado) in lote:
            filas += 1
            if filas >= siguiente_aviso:
                siguiente_aviso += AVISO_CADA
                transcurrido = time.time() - arranque
                log.info("   %s filas leidas en %d s (%d por segundo)",
                         "{:,}".format(filas), int(transcurrido),
                         int(filas / max(1, transcurrido)))
            usuario = (usuario or "").strip()[:100]
            identificado = bool(usuario)
            if not identificado:
                usuario = SIN_NOMBRE
            elif usuario_id is not None:
                correo_de.setdefault(usuario_id, usuario)
            modulo = (modulo or "").strip()[:100]
            ruta = normalizar_ruta(ruta)
            fecha = local(momento).date()

            clave = (fecha, usuario, modulo, ruta)
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
                # Lista y no tupla: al terminar la pasada se les pone nombre a los
                # que no lo traian, usando el mapa de UserId que para entonces ya
                # esta completo.
                logins.append([registro, usuario_id, usuario, momento,
                               momento_local.date(),
                               momento_local.time().replace(microsecond=0),
                               (ip or "")[:64] or None, codigo,
                               int(codigo == 200) if codigo is not None else 0,
                               int(identificado)])

            # Las filas sin correo no entran en las sesiones. Una sesion es el rato
            # que una PERSONA estuvo dentro, y aqui no se sabe de quien es cada
            # fila: tratarlas como un solo usuario juntaria accesos de gente
            # distinta en una sesion inventada. Como login si cuentan, porque ahi
            # el dato es el intento en si.
            if identificado:
                if (abierta is None or abierta.usuario != usuario
                        or momento - abierta.fin > HUECO):
                    if abierta is not None:
                        sesiones.append(abierta.fila())
                    abierta = Sesion(usuario, momento, modulo)
                else:
                    abierta.sumar(momento, modulo)

    if abierta is not None:
        sesiones.append(abierta.fila())

    # Ponerle nombre a los logins anonimos. Solo ahora se puede: el mapa se llena
    # con las peticiones autenticadas, que en el orden por correo llegan DESPUES
    # de las filas sin correo. Hacerlo al vuelo no funcionaria.
    puestos = 0
    for entrada in logins:
        if not entrada[9] and entrada[1] is not None:
            correo = correo_de.get(entrada[1])
            if correo:
                entrada[2] = correo
                entrada[9] = 1
                puestos += 1
    if logins:
        log.info("   logins: %d de %d quedaron con nombre (%d por el UserId)",
                 sum(1 for e in logins if e[9]), len(logins), puestos)

    filas_uso = [(f, u, m, r, v[0], v[1], v[2], len(v[3]))
                 for (f, u, m, r), v in uso.items()]
    return filas_uso, [tuple(e) for e in logins], sesiones, cuentas, filas


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
            "INSERT INTO dbo.uso_diario (fecha, usuario, modulo, ruta,"
            " peticiones, primera_utc, ultima_utc, ips)"
            " VALUES (?,?,?,?,?,?,?,?)", filas_uso)
    if logins:
        cur.executemany(
            "INSERT INTO dbo.logins (registro_id, usuario_id, usuario,"
            " momento_utc, fecha, hora_local, ip, codigo, exitoso, identificado)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)", logins)
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

def dia_local_a_utc(texto):
    """Una fecha AAAA-MM-DD, escrita en hora de planta, como instante UTC.

    Que sea local y no UTC importa: el resumen agrupa por dia local, asi que una
    ventana expresada en UTC caeria a caballo entre dos dias de planta y los
    escribiria los dos a medias. Con el desfase de -6, pedir '2026-09-30' en UTC
    seria en realidad desde las seis de la tarde del 29.
    """
    d = dt.datetime.strptime(texto, "%Y-%m-%d").date()
    return (dt.datetime.combine(d, dt.time.min, tzinfo=ZONA)
            .astimezone(dt.timezone.utc).replace(tzinfo=None))


def corte_de_hoy():
    """El inicio del dia local de hoy, en UTC.

    No se procesa el dia en curso: quedaria a medias y manana habria que
    reescribirlo. El informe mira hasta ayer.
    """
    return dia_local_a_utc(dt.datetime.now(ZONA).date().isoformat())


def main(argv=None):
    p = argparse.ArgumentParser(description="Resume el uso de Sidon Industrial.")
    p.add_argument("--desde", help="AAAA-MM-DD en hora de planta."
                                   " Obligatorio la primera vez.")
    p.add_argument("--hasta", help="AAAA-MM-DD en hora de planta, exclusivo."
                                   " Por omision, el inicio de hoy.")
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

    # Las dos fechas se escriben en hora de planta, que es como se piensan, y se
    # convierten aqui. La marca guardada ya viene en UTC.
    hasta = dia_local_a_utc(a.hasta) if a.hasta else corte_de_hoy()

    cn_destino = conectar_destino()
    desde = marca_anterior(cn_destino)
    if a.desde:
        desde = dia_local_a_utc(a.desde)
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
