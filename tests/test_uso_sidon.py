# -*- coding: utf-8 -*-
"""
Pruebas del resumen de uso de Sidon.

Lo que se prueba es `leer_y_resumir`, que es donde se decide todo: donde corta
una sesion, que cuenta como login, en que dia local cae cada fila. Nada de esto
avisa cuando se equivoca -el ETL correria igual y el informe diria numeros
mansamente falsos-, asi que se comprueba con filas inventadas y a mano.

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

UTC = dt.timezone.utc


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

_contador = [0]


def fila(usuario, modulo, cuando, ip="10.0.0.1", estado=None):
    """Una fila como la devuelve la consulta, con su identificador de origen."""
    _contador[0] += 1
    return ("00000000-0000-0000-0000-%012d" % _contador[0],
            usuario, modulo, cuando, ip, estado)


def resumir(filas):
    return etl.leer_y_resumir(ConexionFalsa(filas), momento(0), momento(23, 59))


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
    assert modulos == 2          # login y fullData
    assert abrio == 1            # empezo con un login de verdad


def test_un_hueco_largo_parte_la_sesion_en_dos():
    """Treinta y un minutos sin pedir nada: la persona se fue y volvio."""
    _uso, _logins, sesiones, _c, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 31)),
    ])
    assert len(sesiones) == 2
    assert [s[7] for s in sesiones] == [0, 0]


def test_treinta_minutos_justos_no_parten_la_sesion():
    """El corte es a MAS de 30 minutos. Justo en el limite sigue siendo la misma."""
    _uso, _logins, sesiones, _c, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 30)),
    ])
    assert len(sesiones) == 1


def test_dos_personas_no_se_mezclan():
    """Las filas vienen ordenadas por usuario; cada quien su sesion."""
    _uso, _logins, sesiones, cuentas, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 5)),
        fila("beto@bafar.com", "fullData", momento(16, 2), ip="10.0.0.2"),
    ])
    assert len(sesiones) == 2
    assert {s[0] for s in sesiones} == {"ana@bafar.com", "beto@bafar.com"}
    assert set(cuentas) == {"ana@bafar.com", "beto@bafar.com"}


def test_un_login_negado_se_guarda_y_se_marca_fallido():
    """Los intentos fallidos dicen tanto del uso como los que funcionan."""
    _uso, logins, _s, _c, _f = resumir([
        fila("ana@bafar.com", "login", momento(16, 0), estado=OK),
        fila("beto@bafar.com", "login", momento(16, 1), estado=NEGADO),
    ])
    assert len(logins) == 2
    por_usuario = {l[1]: l for l in logins}
    assert por_usuario["ana@bafar.com"][6] == 200
    assert por_usuario["ana@bafar.com"][7] == 1
    assert por_usuario["beto@bafar.com"][6] == 401
    assert por_usuario["beto@bafar.com"][7] == 0


def test_dos_logins_sin_correo_en_el_mismo_segundo_no_chocan():
    """El caso que tumbo la primera corrida real.

    La llave era (usuario, momento), que da por hecho que una persona no entra dos
    veces en el mismo segundo. Con el correo vacio todas las filas anonimas se ven
    como la misma persona, y dos del 20 de agosto a las 23:06:18 reventaron la
    escritura entera despues de 43 minutos de lectura. Ahora la llave es el
    identificador que ya traia cada fila del origen, que es unico por definicion.
    """
    _uso, logins, _s, _c, _f = resumir([
        fila("", "login", momento(16, 0), estado=NEGADO),
        fila("", "login", momento(16, 0), estado=NEGADO),
    ])
    assert len(logins) == 2
    assert logins[0][0] != logins[1][0]          # identificadores distintos
    assert {l[1] for l in logins} == {etl.SIN_NOMBRE}
    assert [l[8] for l in logins] == [0, 0]      # ninguno identificado


def test_las_filas_sin_correo_no_arman_sesiones():
    """Una sesion es el rato que estuvo una PERSONA.

    Si las filas anonimas contaran, accesos de gente distinta se juntarian en una
    sesion inventada que nadie vivio. Como login si cuentan: ahi el dato es el
    intento en si.
    """
    _uso, logins, sesiones, cuentas, _f = resumir([
        fila("", "login", momento(16, 0), estado=NEGADO),
        fila("", "login", momento(16, 5), estado=NEGADO),
        fila("ana@bafar.com", "fullData", momento(16, 10)),
    ])
    assert len(logins) == 2
    assert len(sesiones) == 1
    assert sesiones[0][0] == "ana@bafar.com"
    # En el uso diario si aparecen, bajo su nombre propio: son peticiones reales.
    assert etl.SIN_NOMBRE in cuentas


def test_la_hora_se_convierte_a_hora_de_planta():
    """UTC-6: las 16:00 UTC son las 10:00 en planta, el mismo dia."""
    _uso, logins, _s, _c, _f = resumir([
        fila("ana@bafar.com", "login", momento(16, 0), estado=OK),
    ])
    _reg, _usuario, momento_utc, fecha, hora = logins[0][:5]
    assert momento_utc == momento(16, 0)      # el instante original, intacto
    assert fecha == dt.date(2026, 9, 15)
    assert hora == dt.time(10, 0)


def test_la_madrugada_utc_cae_en_el_dia_anterior_de_planta():
    """Las 03:00 UTC del 15 son las 21:00 del 14 en planta.

    Este es el caso que haria que un informe de 'quien entro el martes' metiera
    gente del lunes. Si alguna vez falla, el huso dejo de aplicarse.
    """
    _uso, logins, _s, _c, _f = resumir([
        fila("ana@bafar.com", "login", momento(3, 0, dia=15), estado=OK),
    ])
    fecha, hora = logins[0][3], logins[0][4]
    assert fecha == dt.date(2026, 9, 14)
    assert hora == dt.time(21, 0)


def test_el_uso_se_agrupa_por_dia_usuario_y_modulo():
    uso, _l, _s, _c, _f = resumir([
        fila("ana@bafar.com", "fullData", momento(16, 0)),
        fila("ana@bafar.com", "fullData", momento(16, 5), ip="10.0.0.2"),
        fila("ana@bafar.com", "stopages", momento(16, 6)),
    ])
    por_modulo = {u[2]: u for u in uso}
    assert por_modulo["fullData"][3] == 2        # peticiones
    assert por_modulo["fullData"][4] == momento(16, 0)   # primera
    assert por_modulo["fullData"][5] == momento(16, 5)   # ultima
    assert por_modulo["fullData"][6] == 2        # dos IPs distintas
    assert por_modulo["stopages"][3] == 1


def test_sin_filas_no_inventa_nada():
    uso, logins, sesiones, cuentas, filas = resumir([])
    assert (uso, logins, sesiones, filas) == ([], [], [], 0)
    assert cuentas == {}


def test_las_fechas_de_la_linea_de_comandos_son_hora_de_planta():
    """`--desde 2026-09-30` es la medianoche DE PLANTA, no la de UTC.

    Si se tomaran como UTC, la ventana caeria a caballo entre dos dias locales y
    una corrida real escribiria los dos a medias: el 29 le faltarian las horas
    de la manana y el 30 las de la tarde, sin que nada avisara.
    """
    inicio = etl.dia_local_a_utc("2026-09-30")
    assert inicio == dt.datetime(2026, 9, 30, 6, 0)   # UTC-6
    assert etl.local(inicio).date() == dt.date(2026, 9, 30)
    assert etl.local(inicio).time() == dt.time(0, 0)


def test_una_ventana_de_un_dia_cubre_ese_dia_local_entero():
    """Lo que de verdad importa: que el ultimo instante siga siendo el mismo dia."""
    inicio = etl.dia_local_a_utc("2026-09-30")
    fin = etl.dia_local_a_utc("2026-10-01")
    assert (fin - inicio) == dt.timedelta(days=1)
    ultimo = fin - dt.timedelta(seconds=1)
    assert etl.local(inicio).date() == etl.local(ultimo).date() == dt.date(2026, 9, 30)
