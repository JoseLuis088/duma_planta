# Batería de calidad del agente Duma

Verifica que las respuestas del agente coinciden con la base de datos. No juzga
si la redacción "suena bien": compara cada cifra contra un **oráculo** calculado
con SQL independiente de las herramientas del agente.

## Cómo se usa

```bash
python tests/bateria_oraculo.py          # 1. recalcula la verdad desde SQL Server
python tests/bateria_casos.py            # 2. corre los 56 casos
python tests/bateria_casos.py B1 C3 G3   # solo algunos casos
```

El paso 1 escribe `tests/oraculo.json` y hay que rehacerlo cuando cambien los datos
de referencia. Los resultados quedan en `tests/resultados.json` con la respuesta
completa de cada caso, para poder revisar los fallos.

Requiere conexión a SQL Server y a Azure OpenAI: cada caso es una consulta real
al agente. Una corrida completa toma unos 7 minutos y consume tokens.

## Qué cubre

| Grupo | Casos | Qué comprueba |
|---|---|---|
| A | 5 | Tiempo real: OEE, estado, producción, velocidad |
| B | 10 | Un día concreto: global, por turno, kilos, disponibilidad, brecha |
| C | 8 | Rangos: semana, mejor y peor día, comparación entre días |
| D | 10 | Paros: eventos, minutos, causas, Pareto, gráficas |
| E | 6 | Intradía: OEE por hora, hora con más paro, franjas |
| F | 5 | Sensores: catálogo, fuera de rango, correlación |
| G | 12 | Robustez: fechas sin datos, línea inexistente, orden de borrado, inyección de prompt, saludo, inglés, informe |

## Fecha de referencia

Los casos se apoyan en `2026-08-31` (día con los tres turnos cerrados) y en la
semana `2026-08-25` a `2026-08-31`. Si esos datos dejan de existir, cambia las
constantes en `bateria_oraculo.py` y vuelve a generar el oráculo.

## Por qué existe

La primera corrida encontró cinco defectos que ninguna prueba unitaria detecta,
entre ellos que el agente afirmaba el día contrario al real al preguntarle cuál
había sido el peor OEE de la semana, y que entregaba los datos de la línea de
hamburguesas etiquetados como "línea de salchichas".
