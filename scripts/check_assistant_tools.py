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
print(f"Assistant Name: {asst.name}")
print("Tools:")
for tool in asst.tools:
    if tool.type == "function":
        print(f" - Function: {tool.function.name}")
        print(json.dumps(tool.function.parameters, indent=2))
    else:
        print(f" - Tool: {tool.type}")
