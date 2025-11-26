import os
import uuid
import json
import time
import base64
from typing import List, Optional

import pyodbc
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # backend sin pantalla
import matplotlib.pyplot as plt

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from openai import AzureOpenAI

# ---------- Carga de variables de entorno ----------
load_dotenv()

AZURE_OPENAI_ENDPOINT = os.environ["AZURE_OPENAI_ENDPOINT"]
AZURE_OPENAI_API_KEY = os.environ["AZURE_OPENAI_API_KEY"]
AZURE_OPENAI_API_VERSION = os.environ.get(
    "AZURE_OPENAI_API_VERSION",
    "2024-12-01-preview",
)
ASSISTANT_ID = os.environ["ASSISTANT_ID"]

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASS = os.getenv("SQL_PASS")
SQL_DRIVER = os.getenv("SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

CONN_STR = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DB};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASS};"
    "TrustServerCertificate=yes;"
)

# ---------- Cliente Azure OpenAI ----------
client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
)

# ---------- App FastAPI ----------
app = FastAPI(title="Duma Planta Backend", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Carpeta de estáticos (index.html, imágenes, gráficos)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ---------- Helper SQL ----------
def run_sql(select_sql: str):
    """
    Ejecuta un SELECT y regresa (rows, columns).
    rows es lista de listas serializable a JSON (datetime/Decimal→str, binario→base64).
    """
    with pyodbc.connect(CONN_STR) as conn:
        cur = conn.cursor()
        cur.execute(select_sql)
        rows_raw = cur.fetchall()
        cols = [c[0] for c in cur.description]

        rows = []
        for r in rows_raw:
            out_row = []
            for v in r:
                if isinstance(v, (bytes, bytearray)):
                    out_row.append(base64.b64encode(v).decode("utf-8"))
                else:
                    try:
                        json.dumps(v)
                        out_row.append(v)
                    except Exception:
                        out_row.append(str(v))
            rows.append(out_row)

        return rows, cols

# ---------- Helper gráficos ----------
PLOTS_DIR = os.path.join("static", "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)


def render_chart_from_df(df: pd.DataFrame, spec: dict) -> str:
    """
    Genera un gráfico (line, bar, heatmap, corr) desde un DataFrame
    y retorna la URL pública bajo /static/plots/...
    """
    import numpy as np
    from matplotlib.ticker import PercentFormatter

    spec = spec or {}
    chart = spec.get("chart", "line")
    title = spec.get("title") or ""
    x = spec.get("x")
    ys = spec.get("ys") or []
    style = spec.get("style") or {}
    width = style.get("width", 900)
    height = style.get("height", 500)

    # Opciones para eje Y / orden del X
    y_format = spec.get("y_format")  # "percent" | None
    y_min = spec.get("y_min")
    y_max = spec.get("y_max")
    sort_x = spec.get("sort_x", True)

    # Convertir Y a numérico
    for y in ys:
        if y in df.columns:
            df[y] = pd.to_numeric(df[y], errors="coerce")

    # Ordenar X si aplica
    if x:
        if np.issubdtype(df[x].dtype, np.number) is False:
            try:
                df[x] = pd.to_datetime(df[x], errors="ignore")
            except Exception:
                pass
        if sort_x:
            df = df.sort_values(by=x)

    fig, ax = plt.subplots(figsize=(width / 100.0, height / 100.0))

    if chart in ("line", "bar"):
        if not (x and ys):
            raise ValueError("Para line/bar especifica 'x' y 'ys'.")
        for y in ys:
            if chart == "line":
                ax.plot(df[x], df[y], label=y, marker="o")
            else:
                ax.bar(df[x], df[y], label=y)
        ax.set_xlabel(x or "")
        ax.set_ylabel(", ".join(ys))
        ax.legend()
    elif chart == "heatmap":
        data = df.select_dtypes(include="number")
        im = ax.imshow(data.values, aspect="auto")
        plt.colorbar(im, ax=ax)
        ax.set_xticks(range(len(data.columns)))
        ax.set_xticklabels(data.columns, rotation=45, ha="right")
        ax.set_yticks(range(len(data.index)))
        ax.set_yticklabels(data.index)
    elif chart == "corr":
        data = df.select_dtypes(include="number")
        corr = data.corr(numeric_only=True)
        im = ax.imshow(corr.values, vmin=-1, vmax=1, cmap="coolwarm")
        plt.colorbar(im, ax=ax)
        ax.set_xticks(range(len(corr.columns)))
        ax.set_xticklabels(corr.columns, rotation=45, ha="right")
        ax.set_yticks(range(len(corr.index)))
        ax.set_yticklabels(corr.index)
    else:
        raise ValueError(f"Tipo de gráfico no soportado: {chart}")

    # Eje Y como porcentaje 0–100 opcional
    if y_format == "percent":
        if ys and df[ys].max(numeric_only=True).max() <= 1.0:
            for y in ys:
                df[y] = df[y] * 100.0
        ax.yaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
        if y_min is None and y_max is None:
            ax.set_ylim(0, 100)

    # Límites manuales de Y
    if y_min is not None or y_max is not None:
        ax.set_ylim(
            bottom=y_min if y_min is not None else ax.get_ylim()[0],
            top=y_max if y_max is not None else ax.get_ylim()[1],
        )

    if title:
        ax.set_title(title)

    fname = f"{uuid.uuid4().hex}.png"
    fpath = os.path.join(PLOTS_DIR, fname)
    plt.tight_layout()
    plt.savefig(fpath, dpi=120)
    plt.close(fig)

    return f"/static/plots/{fname}"


# ---------- Núcleo de orquestación con el asistente ----------
def run_assistant_cycle(user_text: str, thread_id: Optional[str]) -> dict:
    """
    Crea/usa un thread, envía el mensaje y resuelve tool calls (sql_query, viz_render),
    devolviendo el último texto y los recursos generados.
    La lógica de negocio (tiempo real, turnos, KPIs) vive en el system prompt,
    schema.md y duma_cookbook.txt del asistente de Azure.
    """
    import logging
    import re

    logging.basicConfig(level=logging.INFO)

    images_out: List[str] = []
    captions_out: List[str] = []
    last_text = ""

    MAX_WAIT_SECONDS = 45
    POLL_INTERVAL_SEC = 0.5

    # Tablas permitidas como guardrail (coincide con tu schema.md)
    ALLOWED_TABLES = {
        "dbo.productionlineintervals",
        "dbo.productionlines",
        "dbo.workshiftexecutions",
        "dbo.workshifttemplates",
        "ind.workshiftexecutionsummaries",
    }

    def handle_run(t_id: str, run_id: str):
        """Sondea el run y atiende las tool calls hasta que termine."""
        nonlocal images_out, captions_out
        start_time = time.time()

        while True:
            r = client.beta.threads.runs.retrieve(thread_id=t_id, run_id=run_id)
            status = r.status or "unknown"

            if status in ("completed", "failed", "expired", "cancelled", "incomplete"):
                break

            if time.time() - start_time > MAX_WAIT_SECONDS:
                logging.warning("⏳ Timeout esperando respuesta del asistente.")
                try:
                    client.beta.threads.runs.cancel(thread_id=t_id, run_id=run_id)
                except Exception:
                    pass
                break

            if status == "requires_action":
                tool_outputs = []

                for tool in r.required_action.submit_tool_outputs.tool_calls:
                    name = tool.function.name
                    try:
                        args = json.loads(tool.function.arguments or "{}")
                    except Exception:
                        args = {}

                    try:
                        if name == "sql_query":
                            select_sql = (args.get("select_sql") or "").strip()

                            # Seguridad básica: sólo SELECT
                            if not select_sql.lower().startswith("select"):
                                raise ValueError("Solo se permiten consultas SELECT.")

                            # Guardrail de tablas (esquema.tabla)
                            tables_found = {
                                f"{schema.lower()}.{table.lower()}"
                                for (schema, table) in re.findall(
                                    r"(?:\b\[?([A-Za-z0-9_]+)\]?)\.\[?([A-Za-z0-9_]+)\]?",
                                    select_sql,
                                    flags=re.IGNORECASE,
                                )
                            }
                            if tables_found and not tables_found.issubset(ALLOWED_TABLES):
                                unknown = ", ".join(sorted(tables_found - ALLOWED_TABLES))
                                allowed = ", ".join(sorted(ALLOWED_TABLES))
                                raise ValueError(
                                    "Tablas desconocidas: "
                                    + unknown
                                    + ". Usa exclusivamente: "
                                    + allowed
                                    + "."
                                )

                            rows, columns = run_sql(select_sql)
                            tool_outputs.append(
                                {
                                    "tool_call_id": tool.id,
                                    "output": json.dumps(
                                        {"columns": columns, "rows": rows},
                                        ensure_ascii=False,
                                        default=str,
                                    ),
                                }
                            )

                        elif name == "viz_render":
                            rows = args.get("rows")
                            columns = args.get("columns")
                            select_sql = args.get("select_sql")
                            spec = args.get("spec", {}) or {}

                            if rows and columns:
                                df = pd.DataFrame(rows, columns=columns)
                            elif select_sql:
                                rws, cols = run_sql(select_sql)
                                df = pd.DataFrame(rws, columns=cols)
                            else:
                                raise ValueError(
                                    "Para viz_render proporciona 'rows/columns' o 'select_sql'."
                                )

                            img_url = render_chart_from_df(df, spec)
                            images_out.append(img_url)
                            captions_out.append(spec.get("title") or "Gráfico")

                            tool_outputs.append(
                                {
                                    "tool_call_id": tool.id,
                                    "output": json.dumps(
                                        {"image_url": img_url}, ensure_ascii=False
                                    ),
                                }
                            )

                        else:
                            tool_outputs.append(
                                {
                                    "tool_call_id": tool.id,
                                    "output": json.dumps(
                                        {"error": f"Función no reconocida: {name}"},
                                        ensure_ascii=False,
                                    ),
                                }
                            )

                    except Exception as ex:
                        tool_outputs.append(
                            {
                                "tool_call_id": tool.id,
                                "output": json.dumps(
                                    {"error": str(ex)}, ensure_ascii=False
                                ),
                            }
                        )

                client.beta.threads.runs.submit_tool_outputs(
                    thread_id=t_id, run_id=run_id, tool_outputs=tool_outputs
                )

            time.sleep(POLL_INTERVAL_SEC)

    # ---------------- Lógica principal ----------------
    try:
        # 1) Thread
        if thread_id:
            t_id = thread_id
        else:
            t = client.beta.threads.create()
            t_id = t.id

        # 2) Mensaje del usuario
        client.beta.threads.messages.create(
            thread_id=t_id,
            role="user",
            content=user_text,
        )

        # 3) Instrucciones ligeras a nivel de run
        msg_low = (user_text or "").strip().lower()
        greeting_set = {
            "hola",
            "holi",
            "buenos días",
            "buenas",
            "buenas tardes",
            "buenas noches",
            "qué tal",
            "que tal",
            "hi",
            "hello",
            "hey",
        }
        is_pure_greeting = msg_low in greeting_set or msg_low.rstrip("!.?") in greeting_set

        extra_instructions = (
            "Responde siempre en español. "
            "No muestres consultas SQL en la respuesta final salvo que el usuario lo pida explícitamente. "
            "Cuando necesites datos de la base, utiliza las herramientas definidas (por ejemplo sql_query y viz_render) "
            "siguiendo exactamente las reglas descritas en tu system prompt, en schema.md y en duma_cookbook.txt. "
            "Esos archivos contienen toda la lógica de tiempo real, turnos, KPIs y formato de respuesta; "
            "síguelos al pie de la letra."
        )

        if is_pure_greeting:
            extra_instructions += (
                " El mensaje del usuario parece ser un saludo; responde con un saludo breve "
                "y una pregunta sobre en qué puedes ayudarle con la línea de producción."
            )

        # 4) Crear run
        run = client.beta.threads.runs.create(
            thread_id=t_id,
            assistant_id=ASSISTANT_ID,
            instructions=extra_instructions,
        )

        handle_run(t_id, run.id)

        # 5) Leer último mensaje del asistente
        try:
            msgs = client.beta.threads.messages.list(
                thread_id=t_id, order="desc", limit=10
            )
            for m in msgs.data:
                if m.role == "assistant":
                    chunks = []
                    for c in m.content:
                        if getattr(c, "type", "") == "text":
                            chunks.append(c.text.value)
                    last_text = "\n".join(chunks).strip()
                    if last_text:
                        break
        except Exception as e:
            logging.error(f"Error leyendo mensajes del hilo: {e}")  # type: ignore[name-defined]

        if not last_text:
            last_text = "No se recibió respuesta del asistente."

        return {
            "thread_id": t_id,
            "message": last_text,
            "images": images_out,
            "captions": captions_out,
        }

    except Exception as e:
        logging.exception("Error en run_assistant_cycle")  # type: ignore[name-defined]
        return {
            "thread_id": thread_id or "",
            "message": f"⚠️ Ocurrió un error al procesar tu solicitud: {e}",
            "images": images_out,
            "captions": captions_out,
        }


# ---------- Rutas HTTP ----------

@app.get("/", response_class=HTMLResponse)
def home():
    return FileResponse("static/index.html")


@app.post("/chat")
async def chat(request: Request):
    """
    Body esperado: { "input": string, "thread_id"?: string }
    Respuesta: { "thread_id", "message", "images"?, "captions"? }
    """
    try:
        body = await request.json()
        user_text = (body.get("input") or "").strip()
        thread_id = body.get("thread_id")

        if not user_text:
            return JSONResponse({"error": "input vacío"}, status_code=400)

        out = run_assistant_cycle(user_text, thread_id)
        return JSONResponse(out)

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------
# 🔥 Alias para compatibilidad con el frontend:
#     /Bafar/chat  → funciona igual que /chat
# ---------------------------------------------------------
@app.post("/Bafar/chat")
async def chat_bafar(request: Request):
    return await chat(request)


# Para correr local:
# uvicorn main:app --host 0.0.0.0 --port 8000 --env-file .env


