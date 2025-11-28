import os
import io
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

# ---------- Carga de variables ----------
load_dotenv()

AZURE_OPENAI_ENDPOINT = os.environ["AZURE_OPENAI_ENDPOINT"]
AZURE_OPENAI_API_KEY = os.environ["AZURE_OPENAI_API_KEY"]
AZURE_OPENAI_API_VERSION = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
ASSISTANT_ID = os.environ["ASSISTANT_ID"]

SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DB       = os.getenv("SQL_DB")
SQL_USER     = os.getenv("SQL_USER")
SQL_PASS     = os.getenv("SQL_PASS")
SQL_DRIVER   = os.getenv("SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

CONN_STR = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DB};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASS};"
    "TrustServerCertificate=yes;"
)

# ---------- Cliente Azure ----------
client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
)

# ---------- App FastAPI ----------
app = FastAPI(title="Duma Planta Backend", version="1.0.2")

# CORS si vas a servir desde otro origen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Montar estáticos (sirve index.html, imágenes y gráficos)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ---------- Helpers SQL ----------
def run_sql(select_sql: str):
    """
    Ejecuta un SELECT y regresa (rows, columns).
    rows es lista de listas JSON-serializable (convierte tipos a str).
    """
    with pyodbc.connect(CONN_STR) as conn:
        cur = conn.cursor()
        cur.execute(select_sql)
        rows_raw = cur.fetchall()
        cols = [c[0] for c in cur.description]
        # Convertir cualquier tipo no JSON (datetime, Decimal, etc.) a str
        rows = []
        for r in rows_raw:
            out_row = []
            for v in r:
                if isinstance(v, (bytes, bytearray)):
                    out_row.append(base64.b64encode(v).decode("utf-8"))
                else:
                    try:
                        json.dumps(v)  # test rápido
                        out_row.append(v)
                    except Exception:
                        out_row.append(str(v))
            rows.append(out_row)
        return rows, cols

# ---------- Helpers gráficos ----------
PLOTS_DIR = os.path.join("static", "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

def render_chart_from_df(df: pd.DataFrame, spec: dict) -> str:
    """
    Genera un gráfico (line, bar, heatmap, corr) desde un DataFrame
    y retorna la ruta pública bajo /static/plots/...
    """
    import numpy as np
    from matplotlib.ticker import PercentFormatter

    spec = (spec or {})
    chart = spec.get("chart", "line")
    title = spec.get("title") or ""
    x = spec.get("x")
    ys = spec.get("ys") or []
    style = spec.get("style") or {}
    width = style.get("width", 900)
    height = style.get("height", 500)

    # Nuevas opciones (opcionales) para controlar el eje Y y el orden del X
    y_format = spec.get("y_format")       # "percent" | None
    y_min = spec.get("y_min")             # num | None
    y_max = spec.get("y_max")             # num | None
    sort_x = spec.get("sort_x", True)     # por defecto ordena el eje X

    # Coerción a numérico para todas las series Y
    for y in ys:
        if y in df.columns:
            df[y] = pd.to_numeric(df[y], errors="coerce")

    # Si el X es fecha/tiempo o string de fecha, intenta parsear y ordenar
    if x:
        if np.issubdtype(df[x].dtype, np.number) is False:
            # intenta parsear fechas sin romper si falla
            try:
                df[x] = pd.to_datetime(df[x], errors="ignore")
            except Exception:
                pass
        if sort_x:
            df = df.sort_values(by=x)

    fig, ax = plt.subplots(figsize=(width/100.0, height/100.0))

    if chart in ("line", "bar"):
        if not (x and ys):
            raise ValueError("Para line/bar especifica 'x' y 'ys'")
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

    # —— NUEVO: formateo del eje Y como porcentaje y límites 0–100 ——
    if y_format == "percent":
        # Si tus KPIs vienen 0–1, conviértelos a 0–100 automáticamente
        if ys and df[ys].max(numeric_only=True).max() <= 1.0:
            for y in ys:
                df[y] = df[y] * 100.0
        ax.yaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
        # Si no se pasan límites, fuerza 0–100 para que el eje quede limpio
        if y_min is None and y_max is None:
            ax.set_ylim(0, 100)

    # Límites manuales si se pasaron
    if y_min is not None or y_max is not None:
        ax.set_ylim(bottom=y_min if y_min is not None else ax.get_ylim()[0],
                    top=y_max if y_max is not None else ax.get_ylim()[1])

    if title:
        ax.set_title(title)

    fname = f"{uuid.uuid4().hex}.png"
    fpath = os.path.join(PLOTS_DIR, fname)
    plt.tight_layout()
    plt.savefig(fpath, dpi=120)
    plt.close(fig)

    return f"/static/plots/{fname}"


# ---------- Core assistant step ----------
def run_assistant_cycle(user_text: str, thread_id: Optional[str]) -> dict:
    """
    Crea/usa un thread, envía el mensaje y resuelve tool calls (sql_query y viz_render),
    devolviendo el último texto + recursos. Incluye:
      - timeout y reintentos
      - guardrails de tablas permitidas
      - instrucciones para forzar uso de sql_query y no mostrar SQL
      - reintento forzado si el asistente no usa tools en preguntas de KPIs/turnos/fechas
      - reintento forzado si el asistente devuelve SQL como texto o usa tablas inválidas
    """
    import logging, time, json, re
    logging.basicConfig(level=logging.INFO)

    # Siempre inicializa para evitar NameError en retornos/errores
    images_out: List[str] = []
    captions_out: List[str] = []
    last_text = ""

    # Parámetros de control del ciclo
    MAX_WAIT_SECONDS = 45
    POLL_INTERVAL_SEC = 0.5
    TOOL_SUBMIT_RETRIES = 2

    # Tablas permitidas (normalizadas a minúsculas, incluir esquema)
    ALLOWED_TABLES = {
        "dbo.productionlineintervals",
        "dbo.productionlines",
        "dbo.workshiftexecutions",
        "dbo.workshifttemplates",
        "ind.workshiftexecutionsummaries",
    }

    # Palabras clave que indican preguntas que DEBEN ir a SQL
    KPI_KEYWORDS = [
        "oee", "disponibilidad", "desempeño", "desempeno", "calidad",
        "turno", "ayer", "hoy", "fecha", "rango", "intervalo",
        "actual", "ahora", "último", "ultimo", "snapshot", "estado"
    ]

    # Flag para saber si el asistente realmente usó tools
    tool_used = False

    # --- Helper: manejador del ciclo de un run (poll + tools) ----------------
    def handle_run(thread_id: str, run_id: str) -> bool:
        """Sondea el run y atiende tool calls hasta completar o fallar. Devuelve True si se usó alguna tool."""
        nonlocal tool_used, images_out, captions_out
        start_time = time.time()

        while True:
            r = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
            status = r.status or "unknown"

            if status in ("completed", "failed", "expired", "cancelled", "incomplete"):
                break

            # Timeout para evitar ciclos infinitos
            if time.time() - start_time > MAX_WAIT_SECONDS:
                logging.warning("⏳ Timeout esperando respuesta del asistente.")
                try:
                    client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run_id)
                except Exception:
                    pass
                break

            if status == "requires_action":
                tool_outputs = []
                for tool in r.required_action.submit_tool_outputs.tool_calls:
                    name = tool.function.name
                    tool_used = True  # <<-- ¡Se usó una herramienta!

                    try:
                        args = json.loads(tool.function.arguments or "{}")
                    except Exception:
                        args = {}

                    try:
                        if name == "sql_query":
                            select_sql = (args.get("select_sql") or "").strip()

                            # Seguridad básica: SOLO SELECT
                            if not select_sql.lower().startswith("select"):
                                raise ValueError("Solo se permiten consultas SELECT.")

                            # Guardrail: NO tablas inventadas. Extrae esquema.tabla (ej. dbo.Tabla, ind.Tabla)
                            tables_found = {
                                f"{schema.lower()}.{table.lower()}"
                                for (schema, table) in re.findall(
                                    r"(?:\b\[?([A-Za-z0-9_]+)\]?)\.\[?([A-Za-z0-9_]+)\]?",
                                    select_sql,
                                    flags=re.IGNORECASE
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
                            tool_outputs.append({
                                "tool_call_id": tool.id,
                                "output": json.dumps(
                                    {"columns": columns, "rows": rows},
                                    ensure_ascii=False, default=str
                                )
                            })

                        elif name == "viz_render":
                            rows = args.get("rows")
                            columns = args.get("columns")
                            select_sql = args.get("select_sql")
                            spec = args.get("spec", {}) or {}

                            import pandas as pd
                            if rows and columns:
                                df = pd.DataFrame(rows, columns=columns)
                            elif select_sql:
                                rws, cols = run_sql(select_sql)
                                df = pd.DataFrame(rws, columns=cols)
                            else:
                                raise ValueError("Proporciona 'rows/columns' o 'select_sql'.")

                            img_url = render_chart_from_df(df, spec)
                            images_out.append(img_url)
                            captions_out.append(spec.get("title") or "Gráfico")

                            tool_outputs.append({
                                "tool_call_id": tool.id,
                                "output": json.dumps({"image_url": img_url}, ensure_ascii=False)
                            })

                        else:
                            tool_outputs.append({
                                "tool_call_id": tool.id,
                                "output": json.dumps({"error": f"Función no reconocida: {name}"}, ensure_ascii=False)
                            })

                    except Exception as ex:
                        tool_outputs.append({
                            "tool_call_id": tool.id,
                            "output": json.dumps({"error": str(ex)}, ensure_ascii=False)
                        })

                # Enviar outputs con pequeños reintentos defensivos
                last_err = None
                for _ in range(1 + TOOL_SUBMIT_RETRIES):
                    try:
                        client.beta.threads.runs.submit_tool_outputs(
                            thread_id=thread_id, run_id=run_id, tool_outputs=tool_outputs
                        )
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        time.sleep(0.4)
                if last_err:
                    logging.error(f"Error enviando tool_outputs: {last_err}")

            time.sleep(POLL_INTERVAL_SEC)

        return tool_used

    # ------------------------ Cuerpo principal -------------------------------
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
            content=user_text
        )

        # 3) Instrucciones base (saludo mínimo + uso de tools + reglas SQL por turno sin horarios fijos)
        msg = user_text.strip().lower()
        greeting_set = {
            "hola", "holi", "buenos días", "buenas", "buenas tardes", "buenas noches",
            "qué tal", "que tal", "hi", "hello", "hey"
        }
        is_pure_greeting = msg in greeting_set or msg.rstrip("!.?") in greeting_set

        extra_instructions = (
            "Responde en español. "
            "Si el mensaje del usuario es SOLO un saludo, responde con un saludo breve y pregunta en qué puedes ayudar. "
            "NO muestres consultas SQL en la respuesta final (salvo que el usuario lo pida explícitamente). "
            "Cuando la pregunta requiera datos de la base, DEBES llamar a la función sql_query con UNA sola SELECT. "
            "Consulta los documentos adjuntos (schema/cookbook) y CONFÍA en ellos. "
            "Tablas disponibles (con esquema): dbo.ProductionLineIntervals, dbo.ProductionLines, "
            "dbo.WorkShiftExecutions, dbo.WorkShiftTemplates, ind.WorkShiftExecutionSummaries. "
            "No pidas confirmación de nombres de columnas: úsalos tal cual. "
            "Si una consulta falla por nombre inválido, corrígelo tú mismo según el esquema y reintenta. "
            "REGLAS POR TURNO (OBLIGATORIO si el usuario dice 'turno' o da una fecha): "
            "1) Obtén el turno desde dbo.WorkShiftExecutions (StartDate/EndDate en hora local). "
            "2) Obtén el nombre del turno desde dbo.WorkShiftTemplates (por WorkShiftTemplateId). "
            "3) Si el usuario solicita el resumen/resultado del turno, usa ind.WorkShiftExecutionSummaries "
            "(filtra por WorkShiftExecutionId) para OEE, Availability, Performance y Quality. "
            "4) Si el usuario solicita detalle minuto a minuto dentro del turno, usa dbo.ProductionLineIntervals "
            "limitado al rango [StartDate, EndDate) del turno. "
            "Para **tiempo real / actual**, **PROHIBIDO** usar `ind.WorkShiftExecutionSummaries`. Usa SIEMPRE `dbo.ProductionLineIntervals` y trae **un solo registro** con: `ORDER BY pli.IntervalBegin DESC, pli.CreatedAt DESC` para desempatar. "
            "Tras ejecutar sql_query, resume OEE, Disponibilidad, Desempeño y Calidad en % (2 decimales) y "
            "menciona el nombre del turno (Primer/Segundo/Tercero) con la hora local de referencia. "
            "Usa viz_render sólo si el usuario pide comparaciones o tendencias."
        )

                # Detección explícita de consultas de tiempo real
        # Detección explícita de consultas de tiempo real
        is_realtime = any(k in msg for k in ["actual", "ahora", "último", "ultimo", "snapshot", "estado actual", "oee actual"]) \
              and not any(k in msg for k in ["turno", "ayer", "semana", "mes"])

        if is_realtime:
            extra_instructions += (
                " En esta petición de TIEMPO REAL debes llamar a sql_query sobre dbo.ProductionLineIntervals "
                "con una única SELECT: SELECT TOP(1) ... FROM dbo.ProductionLineIntervals AS pli "
                "ORDER BY pli.IntervalBegin DESC, pli.CreatedAt DESC. "
                "Mapea KPIs: OEE→pli.OEE, Disponibilidad→pli.OEEAvailability, Desempeño→pli.OEEPerformance, "
                "Calidad→pli.OEEQuality."
            )

        # saludo breve si el usuario solo saludó
        if is_pure_greeting:
            extra_instructions += " Puedes incluir un solo saludo breve en este turno."

        # 4) Primer run
        run = client.beta.threads.runs.create(
            thread_id=t_id,
            assistant_id=ASSISTANT_ID,
            instructions=extra_instructions
        )
        handle_run(t_id, run.id)

        # 5) Leer último mensaje de asistente
        try:
            msgs = client.beta.threads.messages.list(thread_id=t_id, order="desc", limit=10)
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
            logging.error(f"Error leyendo mensajes del hilo: {e}")

        # 6) Paracaídas A: si NO usó tools y la pregunta amerita SQL, forzar segundo run
        msg_low = (user_text or "").lower()
        asks_for_kpis = any(k in msg_low for k in KPI_KEYWORDS)
        if (not tool_used) and asks_for_kpis:
            forced_instructions = (
                "Debes responder ejecutando SIEMPRE una consulta con la función sql_query. "
                "Si es por turno/fecha: "
                " - Identifica el turno en dbo.WorkShiftExecutions (hora local) y su nombre en dbo.WorkShiftTemplates. "
                " - Para el resumen del turno, consulta ind.WorkShiftExecutionSummaries por WorkShiftExecutionId. "
                " - Para detalle minuto a minuto, consulta dbo.ProductionLineIntervals limitado a [StartDate, EndDate). "
                "Si el usuario pide ACTUAL/AHORA, usa EXCLUSIVAMENTE dbo.ProductionLineIntervals con TOP(1) y ORDER BY IntervalBegin DESC, CreatedAt DESC (no uses Summaries). "
                "Entrega OEE, Availability, Performance y Quality en % (2 decimales) y el nombre del turno si aplica. "
                "NO muestres la consulta en el mensaje final."
            )
            run2 = client.beta.threads.runs.create(
                thread_id=t_id,
                assistant_id=ASSISTANT_ID,
                instructions=forced_instructions
            )
            handle_run(t_id, run2.id)

            # releer mensaje después del run forzado
            try:
                msgs = client.beta.threads.messages.list(thread_id=t_id, order="desc", limit=10)
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
                logging.error(f"Error leyendo mensajes del hilo (run2 forzado por no usar tools): {e}")

        # 7) Paracaídas B: si devolvió SQL literal o error de objeto inválido, forzar otro run
        text_low = (last_text or "").lower()
        looks_like_sql = "```sql" in text_low or ("select " in text_low and " from " in text_low)
        mentions_invalid_object = ("invalid object name" in text_low) or ("no existe" in text_low and "tabla" in text_low)

        if looks_like_sql or mentions_invalid_object:
            allowed = ", ".join(sorted(ALLOWED_TABLES))
            forced_instructions_2 = (
                "NO devuelvas consultas SQL como texto. "
                "EJECUTA la consulta mediante la función sql_query con UNA sola sentencia SELECT. "
                "Usa exclusivamente tablas de la lista permitida: " + allowed + ". "
                "Para preguntas por turno: identifica el turno con dbo.WorkShiftExecutions, "
                "obtén su nombre con dbo.WorkShiftTemplates, y trae el resumen desde ind.WorkShiftExecutionSummaries; "
                "para detalle dentro del turno usa dbo.ProductionLineIntervals en [StartDate, EndDate); si es ACTUAL/AHORA usa TOP(1) en dbo.ProductionLineIntervals ordenado por IntervalBegin DESC, CreatedAt DESC. "
                "Finalmente responde con KPIs en % (2 decimales) y menciona el nombre del turno."
            )

            run3 = client.beta.threads.runs.create(
                thread_id=t_id,
                assistant_id=ASSISTANT_ID,
                instructions=forced_instructions_2
            )
            handle_run(t_id, run3.id)

            # volver a leer
            try:
                msgs = client.beta.threads.messages.list(thread_id=t_id, order="desc", limit=10)
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
                logging.error(f"Error leyendo mensajes del hilo (run3 por SQL literal/error): {e}")

        if not last_text:
            last_text = "No se recibió respuesta del asistente."

        return {
            "thread_id": t_id,
            "message": last_text,
            "images": images_out,
            "captions": captions_out
        }

    except Exception as e:
        logging.exception("Error en run_assistant_cycle")
        return {
            "thread_id": thread_id or "",
            "message": f"⚠️ Ocurrió un error al procesar tu solicitud: {e}",
            "images": images_out,
            "captions": captions_out
        }



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
