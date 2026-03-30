import os
from openai import AzureOpenAI
from dotenv import load_dotenv
import json

load_dotenv()

client = AzureOpenAI(
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
)

assistant_id = os.environ["ASSISTANT_ID"]

asst = client.beta.assistants.retrieve(assistant_id)
tools = asst.tools if asst.tools else []

# Definición actualizada de viz_render
updated_viz_render = {
    "type": "function",
    "function": {
        "name": "viz_render",
        "description": "Renderiza un gráfico a partir de una consulta SQL o datos tabulares.",
        "parameters": {
            "type": "object",
            "properties": {
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Nombres de las columnas si se proporcionan datos tabulares manualmente."
                },
                "rows": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {"type": ["string", "number", "boolean", "null"]}
                    },
                    "description": "Filas de datos tabulares. Debe alinear con 'columns'."
                },
                "select_sql": {
                    "type": "string",
                    "description": "Consulta SQL SELECT que devuelve los datos para graficar (alternativa a columns+rows)."
                },
                "spec": {
                    "type": "object",
                    "properties": {
                        "chart": {
                            "type": "string",
                            "enum": ["line", "bar", "heatmap", "corr"],
                            "description": "Tipo de gráfico a generar."
                        },
                        "title": {
                            "type": "string",
                            "description": "Título del gráfico."
                        },
                        "x": {
                            "type": "string",
                            "description": "Columna para eje X (cuando aplique)."
                        },
                        "ys": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Columnas para eje Y (una o varias)."
                        },
                        "hue": {
                            "type": "string",
                            "description": "Columna categórica para dividir los datos en múltiples series (ej. 'Turno')."
                        },
                        "y_format": {
                            "type": "string",
                            "enum": ["percent", "none"],
                            "description": "Formato del eje Y (ej: 'percent' para mostrar %). "
                        },
                        "y_min": {
                            "type": "number",
                            "description": "Valor mínimo para el eje Y."
                        },
                        "y_max": {
                            "type": "number",
                            "description": "Valor máximo para el eje Y."
                        },
                        "sort_x": {
                            "type": "boolean",
                            "description": "Si es True, ordena el eje X cronológicamente/numéricamente. Por defecto True."
                        },
                        "agg": {
                            "type": "string",
                            "enum": ["none", "sum", "mean", "max", "min"],
                            "description": "Agregación opcional para series."
                        },
                        "style": {
                            "type": "object",
                            "properties": {
                                "height": {"type": "integer"},
                                "width": {"type": "integer"}
                            },
                            "description": "Tamaño del gráfico en pixeles."
                        }
                    },
                    "required": ["chart"],
                    "description": "Especificación del gráfico a renderizar."
                }
            },
            "required": ["spec"]
        }
    }
}

# Reemplazar la herramienta existente
new_tools = []
found = False
for t in tools:
    if t.type == "function" and t.function.name == "viz_render":
        new_tools.append(updated_viz_render)
        found = True
    else:
        new_tools.append(t)

if not found:
    new_tools.append(updated_viz_render)

client.beta.assistants.update(
    assistant_id,
    tools=new_tools
)

print(f"Successfully updated assistant {assistant_id} tools (added hue and Y-axis controls to viz_render)")
