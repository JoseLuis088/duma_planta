# -*- coding: utf-8 -*-
"""
Pruebas del resumen de uso de Sidon.

Lo que se prueba es `leer_y_resumir`, que es donde se decide todo: donde corta
una sesion, que cuenta como login, a quien se le atribuye, en que dia local cae
cada fila. Nada de esto avisa cuando se equivoca -el ETL correria igual y el
informe diria numeros mansamente falsos-, asi que se comprueba con filas
inventadas y a mano.

No se prueba contra las bases de verdad: para eso esta uso_sidon/verificar.py,
que cuenta un dia en el origen y lo compara. Son dos preguntas distintas. Aqui:
"la logica hace lo que creo". Alla: "lee las filas que creo".
"""
import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "uso_sidon"))

import etl  # noqa: E402


@pytest.fixture(autouse=True)
def zona():
    """Hora de planta, que es contra lo que se agrupa por dia."""
    etl.ZONA = etl._zona()


class CursorFalso:
    """Devuelve las filas que se le den, en lotes, como pyodbc."""

    def __init__(self, filas):
        self._filas = list(filas)
        self._entregadas = False

    def execute(self, *_a, **_k):
        return self

    def fetchmany(self, _n):
        if self._entregadas:
            return []
        self._entregadas = True
        return self._filas


class ConexionFalsa:
    def __init__(self, filas):
        self._cur = CursorFalso(filas)

    def cursor(self):
        return self._cur


def momento(h, m=0, dia=15):
    """Un instante UTC del 15 de septiembre de 2026."""
    return dt.datetime(2026, 9, dia, h, m, 0)


OK = '{"StatusCode":200,"ExecutionTimeMs":41}'
NEGADO = '{"StatusCode":401,"ExecutionTimeMs":12}'

ANA = "11111111-1111-1111-1111-111111111111"
BETO = "22222222-2222-2222-2222-222222222222"

_contador = [0]


def fila(usuario, modulo, cuando, ip="10.0.0.1", estado=None, uid=None, ruta=None):
    """Una fila como la devuelve la consulta."""
    _contador[0] += 1
    return ("00000000-0000-0000-0000-%012d" % _contador[0],
            uid, usuario, modulo,
            ruta if ruta is not None else "GET /api/" + modulo,
            cuando, ip, estado)


def resumir(filas):
    return etl.leer_y_resumir(ConexionFalsa(filas), momento(0), momento(23, 59))


# --- Sesiones ---------------------------------------------------------------

def test_un_login_y_su_actividad_son_una_sesion():
    """Tres peticiones seguidas sin huecos largos: una sola visita."""
    _uso, logins, sesiones, _c, filas = resumir([
        fila("ana@bafar.com", "login", momento(16, 0), estado=OK),
        fila("ana@bafar.com", "fullData", momento(16, 5)),
        fila("ana@bafar.com", "fullData", momento(16, 20)),
    ])
    assert filas == 3
    assert len(sesiones) == 1
    assert len(logins) == 1

    usuario, inicio, fin, _fecha, minutos, peticiones, modulos, abrio = sesiones[0]
    assert usuario == "ana@bafar.com"
    assert (inicio, fin) == (momento(16, 0), momento(16, 20))
    assert minutos == 20
    assert peticiones == 3
    assert modulos == 2
    assert abrio == 1


def test_un_hueco_largo_parte_la_sesion_en_dos():
    """Treinta y un minutos sin pedir nada: la persona se fue y volvio."""
    _uso, _logins, sesiones, _c, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 31)),
    ])
    assert len(sesiones) == 2
    assert [s[7] for s in sesiones] == [0, 0]


def test_treinta_minutos_justos_no_parten_la_sesion():
    """El corte es a MAS de 30 minutos."""
    _uso, _logins, sesiones, _c, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 30)),
    ])
    assert len(sesiones) == 1


def test_dos_personas_no_se_mezclan():
    _uso, _logins, sesiones, cuentas, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 5)),
        fila("beto@bafar.com", "fullData", momento(16, 2), ip="10.0.0.2"),
    ])
    assert len(sesiones) == 2
    assert {s[0] for s in sesiones} == {"ana@bafar.com", "beto@bafar.com"}
    assert set(cuentas) == {"ana@bafar.com", "beto@bafar.com"}


# --- Logins -----------------------------------------------------------------

def test_un_login_negado_se_guarda_y_se_marca_fallido():
    """Los intentos fallidos dicen tanto del uso como los que funcionan."""
    _uso, logins, _s, _c, _f = resumir([
        fila("ana@bafar.com", "login", momento(16, 0), estado=OK),
        fila("beto@bafar.com", "login", momento(16, 1), estado=NEGADO),
    ])
    assert len(logins) == 2
    por_usuario = {l[2]: l for l in logins}
    assert por_usuario["ana@bafar.com"][7] == 200
    assert por_usuario["ana@bafar.com"][8] == 1
    assert por_usuario["beto@bafar.com"][7] == 401
    assert por_usuario["beto@bafar.com"][8] == 0


def test_dos_logins_sin_correo_en_el_mismo_segundo_no_chocan():
    """El caso que tumbo la primera corrida real.

    La llave era (usuario, momento), que da por hecho que una persona no entra dos
    veces en el mismo segundo. Con el correo vacio todas las filas anonimas se ven
    como la misma persona, y dos del 20 de agosto a las 23:06:18 reventaron la
    escritura entera despues de 43 minutos de lectura.
    """
    _uso, logins, _s, _c, _f = resumir([
        fila("", "login", momento(16, 0), estado=NEGADO),
        fila("", "login", momento(16, 0), estado=NEGADO),
    ])
    assert len(logins) == 2
    assert logins[0][0] != logins[1][0]
    assert {l[2] for l in logins} == {etl.SIN_NOMBRE}
    assert [l[9] for l in logins] == [0, 0]


def test_a_un_login_sin_correo_se_le_pone_nombre_por_su_UserId():
    """Lo que la primera corrida con datos reales destapo.

    De 19 logins del 30 de septiembre, 17 salieron sin correo: en el momento del
    POST de login el sistema todavia no sabe quien eres. Pero si escribe el
    UserId, y ese mismo UserId aparece con correo en las peticiones ya
    autenticadas. Sin esto, 'quien entro y a que hora' se queda sin contestar.
    """
    _uso, logins, _s, _c, _f = resumir([
        # El login, anonimo, con su UserId. Correo vacio ordena primero.
        fila("", "login", momento(16, 0), estado=OK, uid=ANA),
        # Y despues, la misma persona ya autenticada.
        fila("ana@bafar.com", "fullData", momento(16, 1), uid=ANA),
    ])
    assert len(logins) == 1
    assert logins[0][2] == "ana@bafar.com"   # le pusimos nombre
    assert logins[0][9] == 1                 # y quedo marcado como identificado
    assert logins[0][1] == ANA               # con su UserId guardado


def test_un_login_sin_correo_y_sin_UserId_se_queda_sin_nombre():
    """No se inventa: si no hay por donde atribuirlo, se dice que no se sabe."""
    _uso, logins, _s, _c, _f = resumir([
        fila("", "login", momento(16, 0), estado=OK),
        fila("ana@bafar.com", "fullData", momento(16, 1), uid=ANA),
    ])
    assert logins[0][2] == etl.SIN_NOMBRE
    assert logins[0][9] == 0


def test_las_filas_sin_correo_no_arman_sesiones():
    """Una sesion es el rato que estuvo una PERSONA.

    Si las filas anonimas contaran, accesos de gente distinta se juntarian en una
    sesion inventada que nadie vivio.
    """
    _uso, logins, sesiones, cuentas, _f = resumir([
        fila("", "login", momento(16, 0), estado=NEGADO),
        fila("", "login", momento(16, 5), estado=NEGADO),
        fila("ana@bafar.com", "fullData", momento(16, 10)),
    ])
    assert len(logins) == 2
    assert len(sesiones) == 1
    assert sesiones[0][0] == "ana@bafar.com"
    assert etl.SIN_NOMBRE in cuentas


# --- Rutas ------------------------------------------------------------------

def test_la_ruta_pierde_sus_identificadores():
    """Sin esto habria una fila por recurso en vez de una por pantalla.

    Y hace falta porque Module no sirve: la mitad de sus valores SON los GUIDs.
    """
    assert etl.normalizar_ruta(
        "GET /api/productionLines/a1a5d0ea-edb4-4166-f3f8-08ddced0ef5e"
    ) == "GET /api/productionLines/{id}"
    assert etl.normalizar_ruta("GET /api/lineas/42") == "GET /api/lineas/{n}"
    assert etl.normalizar_ruta("POST /api/sys/upsertProductionOrder") \
        == "POST /api/sys/upsertProductionOrder"
    assert etl.normalizar_ruta(None) == ""


def test_dos_recursos_de_la_misma_pantalla_se_cuentan_juntos():
    _uso, _l, _s, _c, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0),
             ruta="GET /api/productionLines/a1a5d0ea-edb4-4166-f3f8-08ddced0ef5e"),
        fila("ana@bafar.com", "fullData", momento(16, 1),
             ruta="GET /api/productionLines/b2b6e1fb-fec5-5277-a4a9-19eedef1f06f"),
    ])
    assert len(_uso) == 1
    assert _uso[0][3] == "GET /api/productionLines/{id}"
    assert _uso[0][4] == 2


# --- Husos y agrupacion -----------------------------------------------------

def test_la_hora_se_convierte_a_hora_de_planta():
    """UTC-6: las 16:00 UTC son las 10:00 en planta, el mismo dia."""
    _uso, logins, _s, _c, _f = resumir([
        fila("ana@bafar.com", "login", momento(16, 0), estado=OK),
    ])
    momento_utc, fecha, hora = logins[0][3], logins[0][4], logins[0][5]
    assert momento_utc == momento(16, 0)
    assert fecha == dt.date(2026, 9, 15)
    assert hora == dt.time(10, 0)


def test_la_madrugada_utc_cae_en_el_dia_anterior_de_planta():
    """Las 03:00 UTC del 15 son las 21:00 del 14 en planta.

    Este es el caso que haria que un informe de 'quien entro el martes' metiera
    gente del lunes.
    """
    _uso, logins, _s, _c, _f = resumir([
        fila("ana@bafar.com", "login", momento(3, 0, dia=15), estado=OK),
    ])
    assert logins[0][4] == dt.date(2026, 9, 14)
    assert logins[0][5] == dt.time(21, 0)


def test_el_uso_se_agrupa_por_dia_usuario_modulo_y_ruta():
    uso, _l, _s, _c, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 5), ip="10.0.0.2"),
        fila("ana@bafar.com", "stopages", momento(16, 6)),
    ])
    por_modulo = {u[2]: u for u in uso}
    assert por_modulo["fullData"][4] == 2                 # peticiones
    assert por_modulo["fullData"][5] == momento(16, 0)    # primera
    assert por_modulo["fullData"][6] == momento(16, 5)    # ultima
    assert por_modulo["fullData"][7] == 2                 # dos IPs distintas
    assert por_modulo["stopages"][4] == 1


def test_sin_filas_no_inventa_nada():
    uso, logins, sesiones, cuentas, filas = resumir([])
    assert (uso, logins, sesiones, filas) == ([], [], [], 0)
    assert cuentas == {}


def test_las_fechas_de_la_linea_de_comandos_son_hora_de_planta():
    """`--desde 2026-09-30` es la medianoche DE PLANTA, no la de UTC."""
    inicio = etl.dia_local_a_utc("2026-09-30")
    assert inicio == dt.datetime(2026, 9, 30, 6, 0)
    assert etl.local(inicio).date() == dt.date(2026, 9, 30)
    assert etl.local(inicio).time() == dt.time(0, 0)


def test_una_ventana_de_un_dia_cubre_ese_dia_local_entero():
    inicio = etl.dia_local_a_utc("2026-09-30")
    fin = etl.dia_local_a_utc("2026-10-01")
    assert (fin - inicio) == dt.timedelta(days=1)
    ultimo = fin - dt.timedelta(seconds=1)
    assert etl.local(inicio).date() == etl.local(ultimo).date() == dt.date(2026, 9, 30)
