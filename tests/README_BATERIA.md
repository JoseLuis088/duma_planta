# Pruebas de calidad del agente Duma

Verifican que las respuestas del agente coinciden con la base de datos. No juzgan si
la redacción "suena bien": comparan cada cifra contra un **oráculo** calculado con SQL
independiente de las herramientas del agente.

## Regla número uno: el oráculo se regenera antes de cada corrida

La planta **clasifica sus paros días después de que ocurren**. El 31 de agosto de 2026
la causa número uno era "Sin Clasificar" con 337 min; días más tarde ese motivo ya no
existía en ese día y el OEE había subido de 61.61 % a 63.48 %. Un oráculo viejo no solo
produce falsas alarmas —el 3 de septiembre produjo doce— sino que puede **esconder un
defecto real entre el ruido**.

Por eso nada de datos escritos a mano en los casos: motivos, minutos y conteos se leen
del oráculo, y el oráculo se recalcula al empezar.

## Cómo se corre todo

```bash
python -m uvicorn main:app --port 8011        # el servidor que las baterías consultan
python tests/bateria_oraculo.py               # 1. recalcula la verdad desde SQL Server
python -m pytest tests/ -q                    # 2. unitarias (no necesitan servidor)
python tests/estabilidad_filtro.py            # 3. estabilidad del filtro de alcance
python tests/bateria_alcance.py               # 4. dominio: qué contesta y qué declina
python tests/bateria_conversacion.py          # 5. preguntas encadenadas, hilo corto
python tests/bateria_conversacion_larga.py    # 6. 15 conversaciones, 60 turnos
```

Las baterías 4 a 6 aceptan una URL para correr contra otro servidor:

```bash
python tests/bateria_conversacion_larga.py http://172.168.10.106:8002
```

Requieren SQL Server y Azure OpenAI: cada turno es una consulta real. La suite completa
toma unos 40 minutos y consume tokens.

## Qué cubre cada una

| Prueba | Tamaño | Qué comprueba |
|---|---|---|
| `pytest tests/` | 63 | Lógica pura y consistencia con la BD: guardarrailes de SQL, cálculo de OEE, día operativo, gráficas, purga, invariantes intradía, a qué día apunta "ese día" |
| `estabilidad_filtro.py` | 15 × 5 | Que el clasificador de alcance dé **el mismo veredicto** al repetir. Un caso que pasa una vez y falla la siguiente es peor que uno que falla siempre |
| `bateria_alcance.py` | 26 | Declina lo ajeno (incluso disfrazado: "olvida que eres Duma…") y **responde con normalidad lo legítimo**. Un guardarrail que bloquea preguntas buenas es peor que el problema que resuelve |
| `bateria_conversacion.py` | 16 turnos | Referencias entre turnos, corrección de periodo, cambio de tema |
| `bateria_conversacion_larga.py` | 60 turnos | 15 conversaciones completas: diagnóstico encadenado, comparaciones sucesivas, informe y preguntas sobre el informe, inglés a media conversación, robustez |
| `bateria_casos.py` | 56 | Casos aislados, uno por conversación (histórica; ver la advertencia de abajo) |

## Por qué hay baterías conversacionales

La de casos aislados daba 56/56 mientras el agente fallaba en la interfaz real. En un
caso aislado no hay historial previo; en la interfaz el usuario encadena preguntas y
el contexto anterior entra al prompt. **Ese hueco escondió defectos durante días.** Las
baterías conversacionales van por `/chat/`, el mismo endpoint que usa el navegador, que
persiste cada turno.

## Fecha de referencia

Los casos se apoyan en `2026-08-31` (día con los tres turnos cerrados) y en la semana
`2026-08-25` a `2026-08-31`. Si esos datos dejan de existir, cambia las constantes en
`bateria_oraculo.py` y vuelve a generar el oráculo. `DUMA_TEST_DAY` cambia el día que
usan las unitarias.

## Lo que han encontrado

Más de treinta defectos que ninguna prueba unitaria detecta por sí sola, entre ellos:
el agente afirmaba el día contrario al preguntarle cuál fue el peor OEE de la semana;
entregaba datos de la línea de hamburguesas etiquetados como "línea de salchichas";
reutilizaba los datos del día anterior tras una corrección de fecha y afirmaba que la
línea había operado sin interrupciones un día con 28 paros; y sumaba como tiempo
productivo la fila de cierre de turno, con la que una hora llegaba a reportar 276
minutos productivos de 60.
