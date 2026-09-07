# -*- coding: utf-8 -*-
"""
Calculo del OEE global y semaforo.

El modelo agregaba los turnos a mano y aplicaba el umbral por su cuenta: en un informe
reporto 63.2% "EN RIESGO" cuando el valor real era 61.61%, que por su propia regla es
CRITICO. Ahora ambas cosas se calculan aqui.
"""
import pytest

TURNOS = [
    # Disponible, productivo, real, esperado, calidad
    {"TiempoDisponibleMin": 510, "TiempoProductivoMin": 245, "ProduccionRealKg": 5222.6,
     "ProduccionEstimadaKg": 6092.0, "Producto Conforme": 100.0},
    {"TiempoDisponibleMin": 450, "TiempoProductivoMin": 261, "ProduccionRealKg": 6377.0,
     "ProduccionEstimadaKg": 6500.0, "Producto Conforme": 100.0},
    {"TiempoDisponibleMin": 480, "TiempoProductivoMin": 330, "ProduccionRealKg": 8700.6,
     "ProduccionEstimadaKg": 8196.8, "Producto Conforme": 100.0},
]


@pytest.mark.parametrize("oee,esperado", [
    (99.9, "CLASE MUNDIAL"), (85.0, "CLASE MUNDIAL"),
    (84.99, "EN RIESGO"), (65.0, "EN RIESGO"),
    (64.99, "CRÍTICO"), (61.61, "CRÍTICO"), (0.0, "CRÍTICO"),
])
def test_semaforo_respeta_los_umbrales(duma, oee, esperado):
    assert esperado in duma.estado_oee(oee)


def test_semaforo_sin_dato(duma):
    assert duma.estado_oee(None) is None


def test_oee_global_no_promedia_porcentajes(duma):
    """
    Regla de oro: se suman los datos crudos. El promedio simple de los OEE por turno
    daria un numero distinto y mas favorable.
    """
    k = duma.oee_global_from_rows(TURNOS)
    assert k is not None

    disponible = sum(t["TiempoDisponibleMin"] for t in TURNOS)
    productivo = sum(t["TiempoProductivoMin"] for t in TURNOS)
    real = sum(t["ProduccionRealKg"] for t in TURNOS)
    esperado = sum(t["ProduccionEstimadaKg"] for t in TURNOS)

    assert k["Disponibilidad"] == pytest.approx(productivo / disponible * 100, abs=0.01)
    assert k["Desempeno"] == pytest.approx(real / esperado * 100, abs=0.01)
    assert k["OEE_global"] == pytest.approx(
        k["Disponibilidad"] * k["Desempeno"] * k["Producto_Conforme"] / 10000, abs=0.05)


def test_un_turno_corto_no_pesa_igual_que_uno_completo(duma):
    """
    El caso que motiva la regla: un turno de 30 min con OEE psimo no puede arrastrar
    el indicador del dia igual que un turno completo. El promedio simple lo hace;
    la suma de crudos, no.
    """
    turnos = [
        {"TiempoDisponibleMin": 480, "TiempoProductivoMin": 460, "ProduccionRealKg": 10000.0,
         "ProduccionEstimadaKg": 10000.0, "Producto Conforme": 100.0},
        {"TiempoDisponibleMin": 30, "TiempoProductivoMin": 3, "ProduccionRealKg": 50.0,
         "ProduccionEstimadaKg": 600.0, "Producto Conforme": 100.0},
    ]
    k = duma.oee_global_from_rows(turnos)

    promedio_ingenuo = sum(
        (t["TiempoProductivoMin"] / t["TiempoDisponibleMin"]) *
        (t["ProduccionRealKg"] / t["ProduccionEstimadaKg"]) * 100
        for t in turnos) / len(turnos)

    assert k["OEE_global"] > promedio_ingenuo + 5, (
        "el promedio simple castiga de mas: %.2f contra %.2f ponderado"
        % (promedio_ingenuo, k["OEE_global"]))


def test_el_semaforo_acompana_al_numero(duma):
    k = duma.oee_global_from_rows(TURNOS)
    assert k["estado"] == duma.estado_oee(k["OEE_global"])


def test_brecha_contra_plan(duma):
    k = duma.oee_global_from_rows(TURNOS)
    assert k["brecha_vs_plan_kg"] == pytest.approx(
        k["produccion_esperada_kg"] - k["produccion_real_kg"], abs=0.5)


@pytest.mark.parametrize("filas", [[], [{"Turno": "Primero"}]])
def test_sin_columnas_utiles_devuelve_none(duma, filas):
    assert duma.oee_global_from_rows(filas) is None


def test_acepta_los_nombres_de_columna_del_dashboard(duma):
    """Las consultas del agente y las del modulo historico usan alias distintos."""
    filas = [{"AvailableTimeMin": 510, "ProductiveTimeMin": 245,
              "CurrentProduction": 5222.6, "ExpectedProduction": 6092.0, "Quality": 100.0}]
    assert duma.oee_global_from_rows(filas) is not None


# ---------- Un KPI por encima del 100% se explica ----------
# El OEE de las 13:00 del 31/08 salio 102.82% y el tercer turno cerro con desempeno
# 104.76%. Es correcto -la linea produjo mas de lo que preveia su velocidad nominal-
# pero sin explicacion parece un error de calculo.

def test_calla_cuando_los_kpis_son_normales(duma):
    assert duma.nota_por_encima_de_cien(desempeno=95.0, oee=63.5) == ""
    assert duma.nota_por_encima_de_cien(desempeno=100.0, oee=100.0) == ""
    assert duma.nota_por_encima_de_cien() == ""


def test_explica_el_desempeno_por_encima_de_cien(duma):
    nota = duma.nota_por_encima_de_cien(desempeno=104.76, oee=82.9)
    assert "desempeño" in nota.lower() and "oee supera" not in nota.lower()
    assert "velocidad nominal" in nota


def test_explica_el_oee_por_encima_de_cien(duma):
    nota = duma.nota_por_encima_de_cien(desempeno=110.2, oee=102.82,
                                        real_kg=8700.6, esperado_kg=8206.1)
    assert "OEE y el desempeño superan" in nota
    # Las cifras concretas son lo que convence a quien lee que el dato no esta mal.
    assert "8,700.6" in nota and "8,206.1" in nota


def test_lee_los_kpis_aunque_lleguen_como_texto(duma):
    """
    wses.Oee y wses.Performance salen de pyodbc como cadenas ('104.76'). Comprobar
    isinstance(v, (int, float)) descartaba justo los datos que hacen falta y la
    explicacion se quedaba vacia en el desglose por turnos.
    """
    assert duma.a_numero("104.76") == 104.76
    assert duma.a_numero("1,234.5") == 1234.5
    assert duma.a_numero(98) == 98.0
    assert duma.a_numero(None) is None
    assert duma.a_numero("") is None
    assert duma.a_numero("sin dato") is None
    assert duma.a_numero(True) is None          # un bool no es una medicion
    assert duma.nota_por_encima_de_cien(desempeno="104.76", oee="82.90")
    assert duma.nota_por_encima_de_cien(desempeno="98.25", oee="64.43") == ""


# ---------- La frase de kilos contra el plan ----------
# brecha = esperado - real, asi que NEGATIVO significa que la planta produjo de mas. Ese
# signo se ha malinterpretado en los dos sitios donde se reportan kilos: el chat hablo de
# 7,807 kg "perdidos" en una semana donde la planta supero el plan por 1,531, y el informe
# del tablero publico "una perdida de 4,564.2 kg" junto a un cumplimiento del 103.9%.

def test_no_llama_perdida_a_superar_el_plan(duma):
    frase = duma.frase_kilos_vs_plan(-4232.1)
    assert "superó el plan" in frase and "4,232.1" in frase
    assert "no hubo kilos perdidos" in frase.lower()


def test_reporta_la_brecha_cuando_falto_produccion(duma):
    frase = duma.frase_kilos_vs_plan(488.6)
    assert "dejaron de producir" in frase and "488.6" in frase
    assert "superó" not in frase


def test_la_frase_no_lleva_instrucciones_dentro(duma):
    """
    Se publica tal cual en el informe del cliente. Una version anterior terminaba en
    "No hables de perdida de produccion" y el modelo la copio entera al PDF.
    """
    for brecha in (-4232.1, 488.6, 0):
        frase = duma.frase_kilos_vs_plan(brecha)
        for orden in ("no hables", "copia", "tal cual", "nunca", "no derives"):
            assert orden not in frase.lower(), frase


def test_acepta_el_cero_y_descarta_lo_que_no_es_numero(duma):
    assert "no hubo kilos perdidos" in duma.frase_kilos_vs_plan(0).lower()
    assert duma.frase_kilos_vs_plan(None) == ""
    assert duma.frase_kilos_vs_plan("sin dato") == ""
