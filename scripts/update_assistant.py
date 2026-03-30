import os
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

assistant_id = os.getenv("ASSISTANT_ID")

new_tool = {
    "type": "function",
    "function": {
        "name": "get_control_variables",
        "description": "Obtiene el resumen de variables de control (sensores como Chiller, IQF, etc.) para un día específico.",
        "parameters": {
            "type": "object",
            "properties": {
                "day": {
                    "type": "string",
                    "description": "Fecha en formato YYYY-MM-DD"
                }
            },
            "required": ["day"]
        }
    }
}

# Obtener herramientas actuales
asst = client.beta.assistants.retrieve(assistant_id)
tools = asst.tools if asst.tools else []

# Evitar duplicados
if not any(t.type == "function" and t.function.name == "get_control_variables" for t in tools):
    tools.append(new_tool)
    client.beta.assistants.update(
        assistant_id,
        tools=tools
    )
    print(f"Successfully added get_control_variables tool to assistant {assistant_id}")
else:
    print("Tool already exists.")
