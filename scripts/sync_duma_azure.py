import os, sys
from pathlib import Path
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

ASSISTANT_ID = os.getenv("ASSISTANT_ID")
BASE = Path(__file__).parent.parent

print("=== Duma Azure Sync ===")

# 1. Actualizar instrucciones
instructions = (BASE / "System prompt.txt").read_text(encoding="utf-8")
updated = client.beta.assistants.update(ASSISTANT_ID, instructions=instructions)
print(f"[1] Instrucciones actualizadas -> {updated.name} ({ASSISTANT_ID})")

# 2. Subir archivos de conocimiento
uploaded = []
for fname in ["duma_cookbook.txt", "schema.md"]:
    fpath = BASE / fname
    if not fpath.exists():
        print(f"[!] Archivo no encontrado: {fname}")
        continue
    with open(fpath, "rb") as f:
        resp = client.files.create(file=f, purpose="assistants")
    uploaded.append((fname, resp.id))
    print(f"[2] Archivo subido: {fname} -> {resp.id}")

print("\n=== COMPLETADO ===")
print(f"  Instrucciones: {len(instructions)} caracteres")
print(f"  Archivos subidos: {len(uploaded)}")
for name, fid in uploaded:
    print(f"    {name}: {fid}")

print("""
SIGUIENTE PASO (MANUAL - 2 minutos):
  1. Abre https://oai.azure.com/
  2. Ve a tu asistente DumaPlanta_OEE
  3. En 'Archivos de busqueda', elimina los archivos viejos
  4. Agrega los nuevos: duma_cookbook.txt y schema.md
  5. Guarda
""")
