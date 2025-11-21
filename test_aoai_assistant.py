import os, time
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()  # si usas python-dotenv

client = AzureOpenAI(
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
)

assistant_id = os.environ["ASSISTANT_ID"]

print("Endpoint:", os.environ["AZURE_OPENAI_ENDPOINT"])
print("Assistant ID:", assistant_id)

# 1) Crea un thread
t = client.beta.threads.create()
# 2) Agrega un mensaje del usuario
client.beta.threads.messages.create(
    thread_id=t.id, role="user", content="Hola, ¿estás vivo?"
)
# 3) Lanza el run con tu assistant_id
run = client.beta.threads.runs.create(thread_id=t.id, assistant_id=assistant_id)
# 4) Espera a que termine
while True:
    r = client.beta.threads.runs.retrieve(thread_id=t.id, run_id=run.id)
    if r.status == "completed":
        break
    if r.status in ("failed", "expired", "cancelled"):
        raise RuntimeError(f"Run terminó en estado {r.status}: {r.last_error}")
    time.sleep(0.8)

# 5) Lee la respuesta
msgs = client.beta.threads.messages.list(thread_id=t.id, order="desc", limit=5)
for m in msgs.data:
    if m.role == "assistant":
        out = "".join(
            c.text.value for c in m.content if getattr(c, "type", "") == "text"
        )
        print("\nAssistant dice:\n", out)
        break
