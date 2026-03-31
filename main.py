import os
import io
import uuid
import json
import time
import base64
import re
from typing import List, Optional
from datetime import datetime, date, timedelta


import pyodbc
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # backend sin pantalla
import matplotlib.pyplot as plt

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from openai import AzureOpenAI

# Reportes (PDF/Word)
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

from docx import Document
from docx.shared import Pt
import tempfile

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
    f"DATABASE={{{SQL_DB}}};"
    f"UID={{{SQL_USER}}};"
    f"PWD={{{SQL_PASS}}};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connect Timeout=30;"
)

# Ruta absoluta al logo de DUMA (para PDF y Word)
_LOGO_PATH = os.path.join("static", "images", "LOGO DUMA.png")


# ---------- Cliente Azure ----------
client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
)

import os
import json
from typing import Any, Optional

AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "").strip()

def aoai_text(system_prompt: str, user_prompt: str, temperature: float = 0.2, max_tokens: int = 900) -> str:
    """
    Llama a Azure OpenAI (Chat Completions) y regresa texto.
    Requiere AZURE_OPENAI_DEPLOYMENT definido en .env
    """
    if not AZURE_OPENAI_DEPLOYMENT:
        return "⚠️ Falta AZURE_OPENAI_DEPLOYMENT en el .env (nombre del deployment del modelo)."

    try:
        resp = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            temperature=temperature,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        return f"⚠️ Error llamando a Azure OpenAI: {e}"
    

CONTROL_VARS_AI_SYSTEM = """\
Eres Duma, un asistente experto para analítica de piso de producción. Tu tarea es generar un informe EJECUTIVO y de ALTO NIVEL para la dirección.

### Estructura Mandataria (Markdown):

### Resumen ejecutivo
(Escribe UN párrafo fluido y profesional que resuma el estado de la planta hoy. NUNCA uses listas aquí.)

### Hallazgos clave
- (Dato de desviación con impacto operativo...)
- (Incidencia técnica o anomalía de sensor...)

### Interpretación operacional
(Análisis técnico breve de las posibles causas raíz. Usa un tono de Director de Operaciones.)

### Acciones recomendadas
- (Acción concreta 1...)
- (Acción concreta 2...)

### Próximos pasos
- (Validación necesaria para el siguiente turno...)

### Reglas Críticas de Formato:
1. Usa EXACTAMENTE los encabezados con `### `.
2. Deja SIEMPRE una línea en blanco antes y después de cada encabezado.
3. El Resumen Ejecutivo debe ser TEXTO CONTINUO (Párrafo).
4. CADA HALLAZGO Y ACCIÓN DEBE IR EN UNA LÍNEA NUEVA con `- `.
5. NO uses fragmentos de oraciones cortadas como puntos de lista.
6. Tono: Formal, sobrio y directo.
7. **Contexto de Turnos**: Los turnos inician a las 07:00, 15:30 y 23:00.
8. **Regla de Ceros**: Si detectas valores en 0 (producción, velocidad, OEE) exactamente en estos horarios de inicio, interprétalos como un REINICIO (o "borrón y cuenta nueva") del contador acumulado para el nuevo turno, NO como una falla operacional ni parada de línea.
"""

def format_duration_es(minutes: float) -> str:
    """Convierte minutos a formato 'X horas y Y minutos' (español)."""
    if minutes is None or minutes <= 0:
        return "0 minutos"
    mins = int(round(float(minutes)))
    h = mins // 60
    m = mins % 60
    
    parts = []
    if h > 0:
        parts.append(f"{h} {'hora' if h == 1 else 'horas'}")
    if m > 0:
        parts.append(f"{m} {'minuto' if m == 1 else 'minutos'}")
    
    if not parts:
        return "0 minutos"
    if len(parts) == 1:
        return parts[0]
    return " y ".join(parts)

def ai_control_variables_day(day: str, summary: list[dict], executive_summary: str) -> str:
    payload = {
        "day": day,
        "executive_summary_backend": executive_summary,
        "metrics_by_variable": summary[:50],
        "notes": [
            "out_pct es porcentaje de lecturas fuera de rango.",
            "out_points/points es conteo de lecturas fuera de rango.",
        ],
    }
    user_prompt = (
        "Estos son los resultados del backend.\n"
        "Genera el análisis ejecutivo y recomendaciones.\n\n"
        f"JSON:\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )
    return aoai_text(CONTROL_VARS_AI_SYSTEM, user_prompt, temperature=0.25, max_tokens=1100)


# -----------------------------------------------------------------------------
# IA (análisis ejecutivo) para OEE (tiempo real / por día-turno)
# -----------------------------------------------------------------------------

OEE_AI_SYSTEM = """
Eres Duma, un consultor experto en productividad industrial. Genera un análisis ejecutivo del OEE para la gerencia.

### Resumen ejecutivo
(Párrafo fluido analizando el desempeño global y urgencia. SIN listas.)

### Análisis de Paros y Producción
(Analiza el impacto de los paros programados vs no programados en el OEE. Compara la Producción Real vs la Esperada y la eficiencia de la Velocidad Actual vs la Velocidad Esperada.)

### KPI limitante
(Identifica DISPONIBILIDAD, DESEMPEÑO o CALIDAD como el cuello de botella actual.)

### Acciones recomendadas
- (Acción paliativa o correctiva 1...)

### Riesgo si no se actúa
- (Impacto en costos o entregas 1...)

### Reglas Críticas:
1. Tono Senior/Director.
2. Resumen Ejecutivo siempre en PÁRRAFO.
3. Listas con `- ` para acciones y riesgos.
4. Doble salto de línea entre secciones.
5. **Formato de Tiempo**: En tus explicaciones y resúmenes, reporta SIEMPRE las duraciones usando el formato "X horas y Y minutos".
6. **Interpretación de Gaps**: Si la Producción Real es inferior a la Esperada (o la Velocidad Real a la Esperada), explica la posible causa operativa basada en los paros reportados.
8. **Contexto de Turnos**: Turnos inician a las 07:00, 15:30 y 23:00.
9. **Regla de Ceros**: Valores en 0 en estos horarios coinciden con el cambio de turno y deben interpretarse como un reinicio de acumulados, NUNCA como una falla o detención.
10. **Fecha Operativa del Tercer Turno**: El Tercer Turno (23:00→07:00) cruza la medianoche. Su StartDate es el día D (23:00) y su EndDate es el día D+1 (07:00). La "Fecha Operativa" del turno es siempre el día D (el día en que comenzó). Si los datos muestran que el Tercer Turno tiene StartDate en un día y EndDate en el siguiente, es completamente normal y NO indica un error.
""".strip()


def ai_oee_realtime(snapshot: dict) -> str:
    """Genera análisis ejecutivo para OEE en tiempo real (un snapshot)."""
    user_prompt = (
        "Analiza el siguiente SNAPSHOT de OEE en tiempo real y escribe el análisis con la estructura indicada.\n\n"
        "SNAPSHOT (JSON):\n"
        f"{json.dumps(snapshot, ensure_ascii=False, indent=2)}\n\n"
        "Reglas adicionales:\n"
        "- Identifica el KPI limitante (Availability/Performance/Quality el más bajo).\n"
        "- Si StatusCode indica paro, sugiere acciones acordes (mantenimiento, operación, planeación).\n"
        "- No inventes valores que no estén en el JSON."
    )
    return aoai_text(OEE_AI_SYSTEM, user_prompt, temperature=0.2, max_tokens=700)


def ai_oee_day_turn(day: str, rows: list[dict], shift_name: str | None = None) -> str:
    """Genera análisis ejecutivo para OEE por día/turno(s)."""
    user_prompt = (
        "Analiza el siguiente resumen de OEE por turno para un día.\n"
        "Devuelve un análisis ejecutivo con la estructura indicada.\n\n"
        f"DIA: {day}\n"
        f"TURNO_SOLICITADO: {shift_name or 'Todos'}\n\n"
        "ROWS (JSON array):\n"
        f"{json.dumps(rows, ensure_ascii=False, indent=2)}\n\n"
        "Reglas adicionales:\n"
        "- Ordena mentalmente turnos 1→2→3 si vienen varios.\n"
        "- KPI limitante por turno y KPI limitante del día (peor caso).\n"
        "- No inventes valores que no estén en el JSON."
    )
    return aoai_text(OEE_AI_SYSTEM, user_prompt, temperature=0.2, max_tokens=900)



# ---------- App FastAPI ----------
ROOT_PATH = os.getenv("ROOT_PATH", "")
app = FastAPI(title="Duma Planta Backend", version="1.0.3", root_path=ROOT_PATH)

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
    print("\n====== EJECUTANDO EN SQL SERVER ======")
    print(select_sql)
    print("======================================")

    with pyodbc.connect(CONN_STR) as conn:
        cur = conn.cursor()
        cur.execute(select_sql)
        rows_raw = cur.fetchall()
        cols = [c[0] for c in cur.description]

        # Convertir tipos a algo serializable
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

        # 🔍 DEBUG: ver cuántas filas regresó y un ejemplo
        print(f"--> Filas devueltas: {len(rows)}")
        if rows:
            print(f"--> Primera fila: {rows[0]}")
        else:
            print("--> SIN filas (resultado vacío)")

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

    hue = spec.get("hue")
    
    # Limpieza de columnas (quitar espacios en blanco que pyodbc a veces deja)
    df.columns = [c.strip() for c in df.columns]

    # --- VERBOSE DEBUG ---
    print(f"\n[DEBUG CHART] Generating {chart} chart: '{title}'")
    print(f"[DEBUG CHART] spec: {json.dumps(spec)}")
    print(f"[DEBUG CHART] columns: {df.columns.tolist()}")

    # --- AGGRESSIVE SMART HUE DETECTION ---
    if not hue and x in df.columns:
        # Si hay duplicados en X, o si es OEE y tenemos columna de Turno, forzamos grouping
        has_duplicates = df[x].duplicated().any()
        is_oee = any("OEE" in str(y).upper() for y in ys)
        
        if has_duplicates or is_oee:
            for potential in ["Turno", "WorkShiftName", "WorkShift", "Shift", "Linea"]:
                if potential in df.columns and potential != x:
                    hue = potential
                    print(f"[DEBUG CHART] Auto-detected hue: '{hue}' (duplicates={has_duplicates}, oee={is_oee})")
                    break

    if chart in ("line", "bar"):
        if not (x and ys):
            raise ValueError("Para line/bar especifica 'x' y 'ys'")

        if hue and hue in df.columns:
            print(f"[DEBUG CHART] Using Hue grouping by: '{hue}'")
            # Una serie por cada valor único del hue, ordenados si son turnos
            unique_hues = df[hue].dropna().unique()
            
            # Ordenar los turnos lógicamente si es posible
            if all(str(t) in ["Primer Turno", "Segundo Turno", "Tercer Turno"] for t in unique_hues):
                order = {"Primer Turno": 1, "Segundo Turno": 2, "Tercer Turno": 3}
                unique_hues = sorted(unique_hues, key=lambda tx: order.get(str(tx), 9))
            else:
                unique_hues = sorted(unique_hues)

            # Build complete X axis for proper gap handling
            all_x = sorted(df[x].dropna().unique())

            for category in unique_hues:
                df_cat = df[df[hue] == category].sort_values(by=x)
                if df_cat.empty:
                    continue
                for y in ys:
                    label = f"{category}" if len(ys) == 1 else f"{category} - {y}"
                    if chart == "line":
                        ax.plot(df_cat[x], df_cat[y], label=label, marker="o")
                    else:
                        ax.bar(df_cat[x], df_cat[y], label=label, alpha=0.7)
        else:
            print(f"[DEBUG CHART] No hue grouping applied (Single series). Hue: {hue}, Columns: {df.columns.tolist()}")
            for y in ys:
                if chart == "line":
                    ax.plot(df[x], df[y], label=y, marker="o")
                else:
                    ax.bar(df[x], df[y], label=y, alpha=0.8)

        ax.set_xlabel(x or "")
        ax.set_ylabel(", ".join(ys))
        ax.legend(loc="best", fontsize="small")
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

    # —— Formateo del eje Y como porcentaje con límites dinámicos ——
    if y_format == "percent":
        # Si tus KPIs vienen 0–1, conviértelos a 0–100 automáticamente
        if ys and df[ys].max(numeric_only=True).max() <= 1.0:
            for y in ys:
                df[y] = df[y] * 100.0
        ax.yaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
        # Límite inferior 0, superior dinámico según los datos (mínimo 100)
        if y_min is None and y_max is None:
            data_max = df[ys].max(numeric_only=True).max()
            upper = max(105, data_max * 1.05) if not pd.isna(data_max) else 105
            ax.set_ylim(0, upper)

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

    return f"static/plots/{fname}"


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
        "oee", "disponibilidad", "desempeño", "desempeno", "producto conforme",
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
                            mode = args.get("mode")
                            day = args.get("day")
                            from_day = args.get("from_day")
                            to_day = args.get("to_day")
                            shift_name = args.get("shift_name")

                            if mode not in ("realtime", "hist_turno_dia", "hist_turno_rango"):
                                raise ValueError("Parámetro 'mode' inválido para sql_query.")


                            # ------------------------------------------------------------------
                            # 1) REALTIME (RT.1)
                            # ------------------------------------------------------------------
                            if mode == "realtime":
                                select_sql = """
DECLARE @linePattern NVARCHAR(100) = NULL;

SELECT TOP (1)
    pl.Name                           AS LineName,
    pli.IntervalBegin                 AS SnapshotAtLocal,

    -- KPIs
    ROUND(pli.OEE,2)                  AS OEE,
    ROUND(pli.OEEAvailability,2)      AS Availability,
    ROUND(pli.OEEPerformance,2)       AS Performance,
    ROUND(pli.OEEQuality,2)           AS [Producto Conforme],

    -- Conteo de eventos de paros y duraciones (AGRUPADOS)
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = pli.ProductionLineId
              AND IntervalBegin >= DATEADD(MINUTE, -60, pli.IntervalBegin)
              AND IntervalBegin <= pli.IntervalBegin
        ) sub WHERE IntervalProductionLineStatus = 'US' AND (PrevStatus <> 'US' OR PrevStatus IS NULL)
    ) AS ParosNoProgramadosCont,
    DATEDIFF(MINUTE, 0, pli.UnscheduledStopageTime)      AS UnscheduledStopageMin,
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = pli.ProductionLineId
              AND IntervalBegin >= DATEADD(MINUTE, -60, pli.IntervalBegin)
              AND IntervalBegin <= pli.IntervalBegin
        ) sub WHERE IntervalProductionLineStatus = 'SS' AND (PrevStatus <> 'SS' OR PrevStatus IS NULL)
    ) AS ParosProgramadosCont,
    DATEDIFF(MINUTE, 0, pli.ScheduledStopageTime)        AS ScheduledStopageMin,

    -- Estado de la línea (con nombres completos)
    CASE pli.IntervalProductionLineStatus
        WHEN 'US' THEN N'Paro No Programado'
        WHEN 'SS' THEN N'Paro Programado'
        WHEN 'LP' THEN N'Baja Producción'
        WHEN 'AV' THEN N'Disponible'
        ELSE pli.IntervalProductionLineStatus
    END                                              AS StatusCode,

    -- Tiempos adicionales
    CASE 
        WHEN TRY_CONVERT(time, pli.TimeSinceLastStatusChange) IS NOT NULL THEN
            DATEDIFF(MINUTE, 0, TRY_CONVERT(time, pli.TimeSinceLastStatusChange))
        ELSE TRY_CONVERT(int, RIGHT(pli.TimeSinceLastStatusChange, 2))
    END                                              AS StatusTimeMin,

    CASE 
        WHEN TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin) IS NOT NULL THEN
            DATEDIFF(MINUTE, 0, TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin))
        ELSE TRY_CONVERT(int, RIGHT(pli.TimeSinceLastWorkshiftBegin, 2))
    END                                              AS NaturalTimeMin,

    DATEDIFF(MINUTE, 0, pli.EffectiveAvailableTime)      AS ProductiveTimeMin,

    -- Velocidades
    pli.CurrentRate                   AS CurrentRate,
    pli.ExpectedRate                  AS ExpectedRate,

    -- Producción
    pli.CurrentShiftProduction        AS CurrentShiftProduction,
    pli.ExpectedShiftProduction       AS ExpectedShiftProduction,
    pli.CurrentProduction             AS CurrentProduction,
    pli.ExpectedDayProduction         AS ExpectedDayProduction

FROM dbo.ProductionLineIntervals AS pli
INNER JOIN dbo.ProductionLines AS pl
    ON pli.ProductionLineId = pl.ProductionLineId

WHERE
    (@linePattern IS NULL
        OR pl.Name LIKE N'%' + @linePattern + N'%')

ORDER BY pli.IntervalBegin DESC, pli.CreatedAt DESC;
"""

                            # ------------------------------------------------------------------
                            # 2) HISTÓRICO POR TURNO / DÍA (H1.1)
                            # ------------------------------------------------------------------
                            elif mode == "hist_turno_dia":
                                if day:
                                    day_sql = f"CONVERT(date, '{day}')"
                                else:
                                    day_sql = "CAST(GETDATE()-1 AS date)"

                                shift_filter = ""
                                if shift_name:
                                    safe_shift = str(shift_name).replace("'", "''")
                                    shift_filter = f"\n    AND wst.Name = N'{safe_shift}'"

                                select_sql = f"""
DECLARE @day DATE = {day_sql};

SELECT
    wst.Name AS Turno,
    -- ✅ Fecha técnica (Fecha Operativa): 
    CASE
        WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
        ELSE CAST(wse.StartDate AS date)
    END AS Fecha,

    wses.Oee                       AS OEE,
    wses.Availability              AS Disponibilidad,
    wses.Performance               AS Desempeno,
    wses.Quality                   AS [Producto Conforme],

    -- Conteo de eventos y duraciones (AGRUPADOS)
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = wses.ProductionLineId
              AND IntervalBegin >= wse.StartDate AND IntervalBegin < wse.EndDate
        ) sub WHERE IntervalProductionLineStatus = 'US' AND (PrevStatus <> 'US' OR PrevStatus IS NULL)
    ) AS ParosNoProgramadosCont,
    wses.UnscheduledStopageMin     AS TiempoNoProdNoProgramadoMin,
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = wses.ProductionLineId
              AND IntervalBegin >= wse.StartDate AND IntervalBegin < wse.EndDate
        ) sub WHERE IntervalProductionLineStatus = 'SS' AND (PrevStatus <> 'SS' OR PrevStatus IS NULL)
    ) AS ParosProgramadosCont,
    wses.ScheduledStopageMin       AS TiempoNoProdProgramadoMin,

    wses.WorkshiftDurationMin      AS DuracionTurnoMin,
    wses.AvailableTimeMin          AS TiempoDisponibleMin,
    wses.ProductiveTimeMin         AS TiempoProductivoMin,
    wses.ExpectedProductionSummary AS ProduccionEstimadaKg,
    wses.CurrentProductionSummary  AS ProduccionRealKg,
    wses.AvgExpectedVelocity       AS VelocidadPromedioEstimadaKgHr,
    wses.AvgCurrentVelocity        AS VelocidadPromedioRealKgHr
FROM ind.WorkShiftExecutionSummaries AS wses
INNER JOIN dbo.WorkShiftExecutions AS wse
    ON wses.WorkShiftExecutionId = wse.WorkShiftExecutionId
INNER JOIN dbo.WorkShiftTemplates AS wst
    ON wse.WorkShiftTemplateId = wst.WorkShiftTemplateId
WHERE
    wse.Status = 'closed'
    AND wse.Active = 1
    AND wses.Active = 1
    AND wst.Active = 1
    -- ✅ Filtro por Fecha Operativa
    AND (
        CASE
            WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date)
        END
    ) = @day
    {shift_filter}
ORDER BY
    Fecha,
    CASE wst.Name
        WHEN N'Primer Turno' THEN 1
        WHEN N'Segundo Turno' THEN 2
        WHEN N'Tercer Turno' THEN 3
        ELSE 9
    END;
"""

                            # ------------------------------------------------------------------
                            # 3) HISTÓRICO POR RANGO (H1.2)
                            # ------------------------------------------------------------------
                            elif mode == "hist_turno_rango":
                                from_sql = f"CONVERT(date, '{from_day}')" if from_day else "DATEADD(day, -7, CAST(GETDATE() AS date))"
                                to_sql = f"CONVERT(date, '{to_day}')" if to_day else "CAST(GETDATE() AS date)"
                                
                                shift_filter = ""
                                if shift_name:
                                    safe_shift = str(shift_name).replace("'", "''")
                                    shift_filter = f"\n    AND wst.Name = N'{safe_shift}'"

                                select_sql = f"""
DECLARE @fromDay DATE = {from_sql};
DECLARE @toDay DATE = {to_sql};

SELECT
    CASE
        WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
        ELSE CAST(wse.StartDate AS date)
    END                                   AS Fecha,
    wst.Name                              AS Turno,
    wses.Oee                              AS OEE,
    wses.Availability                     AS Disponibilidad,
    wses.Performance                      AS Desempeno,
    wses.Quality                          AS [Producto Conforme],
    -- Conteo de eventos de paros y duraciones (AGRUPADOS)
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = wses.ProductionLineId
              AND IntervalBegin >= wse.StartDate AND IntervalBegin < wse.EndDate
        ) sub WHERE IntervalProductionLineStatus = 'US' AND (PrevStatus <> 'US' OR PrevStatus IS NULL)
    ) AS ParosNoProgramadosCont,
    wses.UnscheduledStopageMin     AS TiempoNoProdNoProgramadoMin,
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = wses.ProductionLineId
              AND IntervalBegin >= wse.StartDate AND IntervalBegin < wse.EndDate
        ) sub WHERE IntervalProductionLineStatus = 'SS' AND (PrevStatus <> 'SS' OR PrevStatus IS NULL)
    ) AS ParosProgramadosCont,
    wses.ScheduledStopageMin       AS TiempoNoProdProgramadoMin
FROM ind.WorkShiftExecutionSummaries AS wses
INNER JOIN dbo.WorkShiftExecutions      AS wse
    ON wses.WorkShiftExecutionId = wse.WorkShiftExecutionId
INNER JOIN dbo.WorkShiftTemplates       AS wst
    ON wse.WorkShiftTemplateId = wst.WorkShiftTemplateId
WHERE
    wse.Status = 'closed'
    AND wse.Active = 1
    AND wses.Active = 1
    AND wst.Active = 1
    AND (
        CASE
            WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date)
        END
    ) >= @fromDay
    AND (
        CASE
            WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date)
        END
    ) <= @toDay
    {shift_filter}
ORDER BY Fecha, Turno;
"""

                            # 🔍 DEBUG: ver qué SQL se está ejecutando
                            print("\n========== SQL GENERADO POR BACKEND ==========")
                            print(select_sql)
                            print("==============================================\n")

                            rows, columns = run_sql(select_sql)
                            tool_outputs.append({
                                "tool_call_id": tool.id,
                                "output": json.dumps(
                                    {"columns": columns, "rows": rows},
                                    ensure_ascii=False,
                                    default=str
                                )
                            })

                        elif name == "get_control_variables":
                            day = args.get("day")
                            if not day:
                                day = date.today().isoformat()
                            
                            try:
                                # Usar lógica existente en main.py para variables críticas
                                from main import load_critical_reads_for_day, summarize_critical_day, plot_critical_timeseries_day, CRITICAL_VARS
                                import os, re

                                df_day = load_critical_reads_for_day(day)
                                summary_df = summarize_critical_day(df_day)
                                summary = summary_df.to_dict(orient="records")

                                # Generar o encontrar los plots para que Duma los pueda mostrar
                                out_dir = os.path.join("static", "plots")
                                os.makedirs(out_dir, exist_ok=True)
                                plots = []
                                
                                if not df_day.empty:
                                    var_ids = sorted(df_day["ProductionLineControlVariableId"].dropna().astype(str).str.lower().unique().tolist())
                                    for vid in var_ids:
                                        meta = next((CRITICAL_VARS[k] for k in CRITICAL_VARS if k.lower() == vid), None)
                                        safe_name = (meta.get("device", "var") + "_" + meta.get("name", "var")) if meta else vid
                                        safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", safe_name.strip().lower()).strip("_")
                                        filename = f"{day}_dia_{safe_name}.html"
                                        out_path = os.path.join(out_dir, filename)
                                        
                                        # Generar si no existe o forzar actualización
                                        plot_critical_timeseries_day(df_day, vid, out_path)
                                        plots.append({
                                            "var_id": vid,
                                            "title": f"{meta.get('name','Variable')} — {meta.get('device','')}".strip(" —") if meta else vid,
                                            "url": f"static/plots/{filename}"
                                        })

                                tool_outputs.append({
                                    "tool_call_id": tool.id,
                                    "output": json.dumps({"day": day, "summary": summary, "plots": plots}, ensure_ascii=False)
                                })
                            except Exception as e:
                                tool_outputs.append({
                                    "tool_call_id": tool.id,
                                    "output": json.dumps({"error": str(e)}, ensure_ascii=False)
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

                            print(f"DEBUG viz_render tool called with spec: {json.dumps(spec, indent=2)}")
                            img_url = render_chart_from_df(df, spec)
                            print(f"DEBUG viz_render DataFrame columns: {df.columns.tolist()}")
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

        # 2) Mensajes hacia el thread: primero una marca de fecha del backend, luego el mensaje real

        # Fecha actual del backend en formato YYYY-MM-DD
        system_date = date.today().isoformat()  # Ejemplo: "2025-12-05"
        system_date_msg = f"[system_date={system_date}]"

        # Enviar mensaje invisible/técnico con la fecha del backend
        client.beta.threads.messages.create(
            thread_id=t_id,
            role="user",
            content=system_date_msg
        )

        # Enviar luego el mensaje real del usuario
        client.beta.threads.messages.create(
            thread_id=t_id,
            role="user",
            content=user_text
        )


        # 3) Instrucciones base (Carga desde archivo + saludo mínimo + reglas SQL por turno)
        system_prompt_content = ""
        try:
            # ✅ Redirigido al nuevo prompt ejecutivo para evitar bloqueos
            with open("DUMA_EXECUTIVE_PROMPT.txt", "r", encoding="utf-8") as f:
                system_prompt_content = f.read()
        except Exception as e:
            logging.error(f"No se pudo leer DUMA_EXECUTIVE_PROMPT.txt: {e}")

        msg = user_text.strip().lower()
        greeting_set = {
            "hola", "holi", "buenos días", "buenas", "buenas tardes", "buenas noches",
            "qué tal", "que tal", "hi", "hello", "hey"
        }
        is_pure_greeting = msg in greeting_set or msg.rstrip("!.?") in greeting_set

        extra_instructions = (
            f"{system_prompt_content}\n\n"
            "CATÁLOGO DE SENSORES (VARIABLES DE CONTROL):\n"
            f"{json.dumps(CRITICAL_VARS, indent=2, ensure_ascii=False)}\n\n"
            "INSTRUCCIONES ADICIONALES DE SESIÓN:\n"
            "Responde en español. "
            "Si el mensaje del usuario es SOLO un saludo, responde con un saludo breve y pregunta en qué puedes ayudar. "
            "NO muestres consultas SQL en la respuesta final. "
            "Para OEE, disponibilidad, desempeño y calidad, usa sql_query. "
            "Para rangos de fechas (ej. 'la semana pasada', 'últimos 7 días'), usa mode='hist_turno_rango' con from_day y to_day. "
            "**REGLA CRÍTICA DE GRÁFICAS:** Si usas una herramienta que genera una gráfica (como `viz_render` o `get_control_variables`), **PROHIBIDO** usar la sintaxis de imagen markdown `![]()`. "
            "NUNCA escribas enlaces que empiecen por `sandbox:/static/plots/...`. "
            "Simplemente menciona en tu respuesta que has generado la gráfica (ej: 'Aquí tienes la gráfica del OEE...'). "
            "El sistema detectará automáticamente la imagen y mostrará un botón de 'Ver gráfica' debajo de tu mensaje. No intentes generarlo tú mismo."
            "Usa rutas relativas directas (ej: static/plots/archivo.png o .html) solo dentro de las llamadas a herramientas, nunca en el texto final. "
            "IMPORTANTE: Para gráficas de OEE por turno/día (serie de tiempo), usa `chart: 'line'`, `x: 'Fecha'`, `ys: ['OEE']` y ESENCIAL usar `hue: 'Turno'`. Sin el parámetro `hue`, la gráfica será ilegible y confusa. "
            "Usa siempre los nombres de columna `Fecha` y `Turno` tal cual aparecen en el cookbook. "
            "Para comparaciones de barras entre turnos, usa `chart: 'bar'`, `x: 'Turno'`, `ys: ['OEE']`. "
            "Consulta los documentos adjuntos (schema/cookbook) y CONFÍA en ellos. "
            "Si una consulta falla por nombre inválido, corrígelo tú mismo según el esquema y reintenta. "
            "Para TIEMPO REAL / ACTUAL de OEE debes usar RT.1. "
            "Para cualquier pregunta de TURNOS o FECHAS de OEE, usa H1.x. "
            "Usa viz_render sólo si el usuario pide comparaciones, tendencias o gráficas."
        )


                # Detección explícita de consultas de tiempo real
        # Detección explícita de consultas de tiempo real
        is_realtime = any(k in msg for k in ["actual", "ahora", "último", "ultimo", "snapshot", "estado actual", "oee actual"]) \
              and not any(k in msg for k in ["turno", "ayer", "semana", "mes"])

        if is_realtime:
            extra_instructions += (
                " En esta petición de TIEMPO REAL debes usar la RECETA RT.1 del archivo duma_cookbook.txt "
                "para consultar dbo.ProductionLineIntervals (último snapshot de la línea). "
                "No inventes otra consulta: usa RT.1 tal cual está definida en el cookbook. "
                "Después interpreta los campos según el system prompt (estatus, tiempos, velocidades, producción, OEE y sus componentes)."
                " Los campos importantes de ese registro significan lo siguiente:\n"
                "   - TimeSinceLastStatusChange: duración que la línea lleva en el estatus actual.\n"
                "   - TimeSinceLastWorkshiftBegin: tiempo natural transcurrido desde que inició el turno.\n"
                "   - EffectiveAvailableTime: TIEMPO PRODUCTIVO (minutos u horas según la columna).\n"
                "   - ScheduledStopageTime: tiempo NO productivo PROGRAMADO.\n"
                "   - UnscheduledStopageTime: tiempo NO productivo NO programado.\n"
                "   - CurrentRate: velocidad actual (kg/h).\n"
                "   - ExpectedRate: velocidad esperada (kg/h).\n"
                "   - CurrentShiftProduction: producción real del turno actual (kg).\n"
                "   - ExpectedShiftProduction: producción estimada del turno a la hora actual (kg).\n"
                "   - CurrentProduction: producción actual del día (kg).\n"
                "   - ExpectedDayProduction: producción planificada del día (kg).\n"
                "   - IntervalProductionLineStatus: estado actual de la línea.\n"
                "   - OEE: indicador OEE global.\n"
                "   - OEEAvailability: disponibilidad.\n"
                "   - OEEPerformance: desempeño.\n"
                "   - OEEQuality: Producto Conforme.\n"
                " Cuando el usuario pregunte por 'tiempo productivo', responde usando EffectiveAvailableTime.\n"
                " Cuando pregunte por 'tiempo no productivo programado', usa ScheduledStopageTime.\n"
                " Cuando pregunte por 'tiempo no productivo no programado', usa UnscheduledStopageTime.\n"
                " Si pide 'tiempo no productivo' en general, puedes explicar que es la suma de los tiempos "
                "no productivos programados y no programados, e indicar ambos valores por separado.\n"
                " Si el usuario pregunta 'qué es' un indicador (por ejemplo: 'qué es tiempo productivo'), "
                "explica su definición usando estas descripciones sin llamar a sql_query.\n"
                " Si el usuario pregunta 'cuánto es' un indicador (por ejemplo: 'cuál es el tiempo productivo'), "
                "llama a sql_query con la SELECT indicada, toma el valor del último registro y devuelve el "
                "resultado de forma clara (incluyendo la unidad de medida si está disponible).\n"
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
                "Debes responder ejecutando SIEMPRE una consulta con la función sql_query.\n"
                "Si la pregunta es de TIEMPO REAL / ACTUAL / AHORA, usa la receta RT.1 del archivo duma_cookbook.txt "
                "sobre dbo.ProductionLineIntervals para obtener el último snapshot.\n"
                "Si la pregunta es por TURNOS o FECHAS (día específico, rango de fechas, ayer, último turno, etc.), "
                "usa EXCLUSIVAMENTE las recetas H1.x del duma_cookbook.txt basadas en "
                "dbo.WorkShiftExecutions + dbo.WorkShiftTemplates + ind.WorkShiftExecutionSummaries "
                "(por ejemplo H1.1 para un solo día por turno, H1.2 para rangos de fechas).\n"
                "No inventes nuevas consultas SQL: copia la receta que corresponda, ajusta solo las fechas o filtros necesarios, "
                "y pásala a sql_query.\n"
                "En la respuesta, entrega OEE, disponibilidad, desempeño y producto conforme en % (En la base de datos ya están en porcentaje, no multipliques por 100), "
                "producción estimada vs real, velocidades promedio estimada y real (si están en la receta), "
                "y tiempos productivos vs no productivos.\n"
                "NO muestres la consulta SQL en el mensaje final."
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

        # 8) Scanner de imágenes post-proceso: mueve links de plots a la lista de imágenes para que el frontend los embeba
        if last_text:
            import re
            # Buscamos cualquier link que contenga /static/plots/... (incluyendo sandbox: o dominios)
            # Ahora detectamos .html, .png, .jpg, .jpeg
            plot_pattern = r"(?:(?:https?://[^\s)\]]+)|sandbox:)?(?:/)?static/plots/[^\s)\]]+\.(?:html|png|jpg|jpeg)"
            found_plots = re.findall(plot_pattern, last_text, re.IGNORECASE)
            
            for p_url in found_plots:
                # Normalizamos a ruta relativa del servidor
                if "static/plots/" in p_url:
                    parts = p_url.split("static/plots/")
                    # Limpiamos el nombre del archivo de caracteres de cierre de markdown o paréntesis
                    filename = parts[-1].split("?")[0].split("#")[0].strip(")] \t\r\n")
                    clean_url = f"static/plots/{filename}"
                    
                    if clean_url not in images_out:
                        images_out.append(clean_url)
                        # Intentar extraer caption si estaba en formato markdown ![caption](url)
                        cap_match = re.search(r"!\[([^\]]*)\]\(" + re.escape(p_url) + r"\)", last_text)
                        if cap_match and cap_match.group(1):
                            captions_out.append(cap_match.group(1))
                        else:
                            is_oee = "oee" in filename.lower()
                            captions_out.append("Gráfico de OEE" if is_oee else "Gráfico de Sensor")
            
            # Limpieza robusta del texto final
            last_text = re.sub(r"!\[[^\]]*\]\(" + plot_pattern + r"\)", "", last_text, flags=re.IGNORECASE)
            last_text = re.sub(r"\[[^\]]*\]\(" + plot_pattern + r"\)", "", last_text, flags=re.IGNORECASE) 
            last_text = re.sub(plot_pattern, "", last_text, flags=re.IGNORECASE)
            # Eliminar restos de sintaxis markdown vacía
            last_text = last_text.replace("()", "").replace("[]", "").replace("![]", "").strip()
            # Limpiar posibles dobles saltos de línea generados por la eliminación
            last_text = re.sub(r"\n\s*\n", "\n\n", last_text)

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

# ---------------------------------------------------------
#  Página web del chat (sirve static/index.html)
#   - GET /        -> index
#   - GET /Bafar   -> index
#   - GET /Bafar/  -> index
# ---------------------------------------------------------
# =========================
# Control Variables module (Critical variables: Parquet + Plotly)
# =========================
from typing import Literal

try:
    import duckdb  # type: ignore
except Exception:
    duckdb = None  # optional; will raise if used without installed

import pandas as pd
import plotly.graph_objects as go
from azure.storage.blob import BlobServiceClient
import tempfile

ShiftName = Literal["Primer", "Segundo", "Tercer"]

CRITICAL_VARS = {
    "3AB4E612-5987-432C-8EF0-28EE3D74C313": {"name": "Temperatura del agua", "device": "Chiller", "min": 0.00, "max": 4.00, "crit_min": -1.00, "crit_max": 5.00},
    "9057486C-3A01-417D-B5E0-33F848EB19FB": {"name": "Alertas", "device": "Detector de metales", "min": -1.00, "max": 1.00, "crit_min": -2.00, "crit_max": 1.00},
    "11A4996C-FA1B-47D9-9A60-125D66F41F84": {"name": "Temperatura interna", "device": "IQF", "min": -42.00, "max": -20.00, "crit_min": -40.00, "crit_max": -18.00},
    "F71768ED-3006-4880-A2FD-9F62344870CC": {"name": "Tiempo de permanencia del producto", "device": "IQF", "min": 31.00, "max": 90.00, "crit_min": 30.00, "crit_max": 95.00},
    "AB2D10BC-B497-4049-AD39-554C2E4BCC24": {"name": "Temperatura del producto", "device": "Mezclador", "min": -4.50, "max": 4.00, "crit_min": -5.00, "crit_max": 5.00},
    "5EF87231-BD89-41F1-B0D6-C5371B237684": {"name": "Temperatura del producto", "device": "Molino", "min": -18.00, "max": -10.00, "crit_min": -20.00, "crit_max": -8.00},
    "D592EFE2-94FF-4DBF-95C8-C1C01FE37D4F": {"name": "Temperatura del agua", "device": "Volteador", "min": 0.00, "max": 25.00, "crit_min": -1.00, "crit_max": 26.00},
    "7AA64D76-1AE9-41DA-85AA-F53A9B5F0162": {"name": "Tiempo de hidratación", "device": "Volteador", "min": -0.50, "max": 15.00, "crit_min": -1.00, "crit_max": 20.00},
}
CRITICAL_VAR_IDS = set(k.strip().lower() for k in CRITICAL_VARS.keys())

def _normalize_shift(shift: str) -> ShiftName:
    s = (shift or "").strip().lower()
    if s.startswith("primer"):
        return "Primer"
    if s.startswith("segundo"):
        return "Segundo"
    if s.startswith("tercer"):
        return "Tercer"
    raise ValueError("shift inválido. Usa: Primer | Segundo | Tercer")

def _get_blob_service_client() -> BlobServiceClient:
    account_url = os.environ["ADLS_ACCOUNT_URL"].strip()
    key = os.environ["ADLS_ACCOUNT_KEY"].strip()
    return BlobServiceClient(account_url=account_url, credential=key)

def download_turn_parquet(day: str, shift: ShiftName) -> str:
    """Descarga el parquet correspondiente a un día y turno. Retorna path local."""
    container_name = os.environ["ADLS_CONTAINER"].strip()
    base_prefix = os.environ.get("ADLS_BASE_PREFIX", "control-variable-reads").strip().strip("/")

    blob_service = _get_blob_service_client()
    container_client = blob_service.get_container_client(container_name)

    day_prefix = f"{base_prefix}/{day}/"

    # Patrones aceptados (por si en ADLS no se llama exactamente "{shift}_Turno_...")
    shift_l = shift.lower()
    acceptable_prefixes = [
        f"{shift}_Turno_".lower(),
        f"{shift}_".lower(),
        f"{shift}-".lower(),
    ]

    blobs = container_client.list_blobs(name_starts_with=day_prefix)

    target_blob = None
    for blob in blobs:
        name_only = blob.name.split("/")[-1].lstrip()  # <-- IMPORTANTÍSIMO
        name_l = name_only.lower()

        if not name_l.endswith(".parquet"):
            continue

        # Match flexible
        if any(name_l.startswith(p) for p in acceptable_prefixes) or (shift_l in name_l):
            target_blob = blob.name  # guarda el nombre REAL del blob
            break

    if not target_blob:
        raise FileNotFoundError(f"No se encontró archivo parquet para {shift} turno en {day} (prefijo: {day_prefix})")

    tmp_dir = tempfile.mkdtemp()

    # Para el nombre local, quita espacio inicial si lo trae
    local_filename = os.path.basename(target_blob).lstrip()
    local_path = os.path.join(tmp_dir, local_filename)

    with open(local_path, "wb") as f:
        blob_client = container_client.get_blob_client(target_blob)
        f.write(blob_client.download_blob().readall())

    return local_path


def load_critical_reads_for_shift(day: str, shift: ShiftName) -> pd.DataFrame:
    """Descarga el parquet de un día/turno y regresa SOLO lecturas de variables críticas."""
    if duckdb is None:
        raise RuntimeError("duckdb no está instalado. Agrega duckdb a requirements.txt")

    parquet_path = download_turn_parquet(day, shift)

    crit_ids = list(CRITICAL_VAR_IDS)
    in_list = ", ".join([f"'{x}'" for x in crit_ids])

    con = duckdb.connect()
    df = con.execute(f"""
        SELECT
            lower(CAST(ProductionLineControlVariableId AS VARCHAR)) AS ProductionLineControlVariableId,
            CAST(LocalTime AS TIMESTAMP) AS LocalTime,
            CAST(Value AS DOUBLE) AS Value,
            CAST(CriticalMinValue AS DOUBLE) AS CriticalMinValue,
            CAST(CriticalMaxValue AS DOUBLE) AS CriticalMaxValue
        FROM read_parquet('{parquet_path}')
        WHERE lower(CAST(ProductionLineControlVariableId AS VARCHAR)) IN ({in_list})
        ORDER BY ProductionLineControlVariableId, LocalTime
    """).df()
    df["Shift"] = shift
    return df

def load_critical_reads_for_day(day: str) -> pd.DataFrame:
    frames = []
    missing = []

    for sh in ["Primer", "Segundo", "Tercer"]:
        try:
            frames.append(load_critical_reads_for_shift(day, sh))  # type: ignore[arg-type]
        except FileNotFoundError:
            missing.append(sh)
            continue

    if not frames:
        # No devolvemos vacío silencioso: esto es justo lo que te está pasando
        raise FileNotFoundError(f"No hubo parquets para el día {day}. Turnos faltantes: {', '.join(missing)}")

    df = pd.concat(frames, ignore_index=True)
    df["LocalTime"] = pd.to_datetime(df["LocalTime"], errors="coerce")
    return df


def plot_critical_timeseries_day(df_day: pd.DataFrame, var_id: str, out_html_path: str) -> str:
    """Grafica una variable con los 3 turnos en un solo gráfico y guarda HTML."""
    vid = var_id.strip().lower()
    d = df_day[df_day["ProductionLineControlVariableId"].astype(str).str.lower() == vid].copy()
    if d.empty:
        raise ValueError(f"No hay datos para var_id={var_id} en este día")

    d["LocalTime"] = pd.to_datetime(d["LocalTime"], errors="coerce")
    d["Value"] = pd.to_numeric(d["Value"], errors="coerce")
    d["CriticalMinValue"] = pd.to_numeric(d["CriticalMinValue"], errors="coerce")
    d["CriticalMaxValue"] = pd.to_numeric(d["CriticalMaxValue"], errors="coerce")
    d = d.sort_values("LocalTime")

    crit_min = float(d["CriticalMinValue"].median())
    crit_max = float(d["CriticalMaxValue"].median())
    d["IsCriticalOut"] = (d["Value"] < crit_min) | (d["Value"] > crit_max)

    meta = None
    for kk, m in CRITICAL_VARS.items():
        if kk.strip().lower() == vid:
            meta = m
            break

    title = f"{meta.get('name','Variable')} — {meta.get('device','')}" if meta else "Serie de tiempo"

    fig = go.Figure()

    # Banda crítica
    fig.add_trace(go.Scatter(
        x=d["LocalTime"], y=[crit_max]*len(d),
        mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"
    ))
    fig.add_trace(go.Scatter(
        x=d["LocalTime"], y=[crit_min]*len(d),
        mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(52, 152, 219, 0.2)",
        name="Rango operativo", hoverinfo="skip"
    ))

    # Serie principal
    fig.add_trace(go.Scatter(
        x=d["LocalTime"], y=d["Value"],
        mode="lines",
        name="Valor"
    ))

    out = d[d["IsCriticalOut"]]
    if not out.empty:
        fig.add_trace(go.Scatter(
            x=out["LocalTime"], y=out["Value"],
            mode="markers",
            name="Lecturas fuera de rango",
            marker=dict(size=6)
        ))

    fig.update_layout(
        yaxis_title="Valor",
        template="plotly_dark",
        hovermode="x unified",
        margin=dict(l=55, r=25, t=40, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=11)
        )
    )

    os.makedirs(os.path.dirname(out_html_path), exist_ok=True)
    fig.write_html(out_html_path, include_plotlyjs="cdn")
    return out_html_path

def plot_critical_timeseries_day_png(
    df_day: pd.DataFrame,
    var_id: str,
    out_png_path: str
) -> str:
    """Versión PNG (matplotlib) para reportes PDF/DOCX.
    Dibuja:
      - Serie Value
      - Banda crítica (min..max)
      - Puntos fuera de crítico
    """
    d = df_day[df_day["ProductionLineControlVariableId"].astype(str).str.lower() == var_id.strip().lower()].copy()
    if d.empty:
        return ""

    d["LocalTime"] = pd.to_datetime(d["LocalTime"], errors="coerce")
    for c in ["Value", "CriticalMinValue", "CriticalMaxValue"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.sort_values("LocalTime")
    d["IsOut"] = (d["Value"] < d["CriticalMinValue"]) | (d["Value"] > d["CriticalMaxValue"])

    crit_min = float(d["CriticalMinValue"].median())
    crit_max = float(d["CriticalMaxValue"].median())

    meta = next((CRITICAL_VARS[k] for k in CRITICAL_VARS if k.strip().lower() == var_id.strip().lower()), None)
    title = f"{meta.get('name','Variable')} — {meta.get('device','')}" if meta else str(var_id)

    # Colores estéticos
    COLOR_IN = "#2ecc71"   # Verde esmeralda
    COLOR_OUT = "#e74c3c"  # Alizarin (Rojo)
    COLOR_LINE = "#2c3e50" # Midnight blue para la línea
    COLOR_BAND = "#3498db" # Belize hole (Azul) para la banda

    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 3.8), dpi=160)

    # Línea principal
    ax.plot(d["LocalTime"], d["Value"], color=COLOR_LINE, linewidth=1.0, alpha=0.7, label="Tendencia")

    # Banda crítica
    ax.fill_between(d["LocalTime"], crit_min, crit_max, alpha=0.1, color=COLOR_BAND, label="Rango operativo")
    ax.axhline(crit_min, color=COLOR_BAND, linestyle="--", linewidth=0.8, alpha=0.5)
    ax.axhline(crit_max, color=COLOR_BAND, linestyle="--", linewidth=0.8, alpha=0.5)

    # Puntos dentro del rango (Verde)
    in_range = d[~d["IsOut"] & d["LocalTime"].notna() & d["Value"].notna()]
    if not in_range.empty:
        ax.scatter(in_range["LocalTime"], in_range["Value"], s=10, color=COLOR_IN, alpha=0.8, label="En rango", zorder=3)

    # Puntos fuera de rango (Rojo)
    out_range = d[d["IsOut"] & d["LocalTime"].notna() & d["Value"].notna()]
    if not out_range.empty:
        ax.scatter(out_range["LocalTime"], out_range["Value"], s=12, color=COLOR_OUT, alpha=0.9, label="Fuera de rango", zorder=4)

    ax.set_title(title, fontsize=12, fontweight='bold', pad=45)
    ax.set_xlabel("Hora local", fontsize=9)
    ax.set_ylabel("Valor", fontsize=9)
    ax.grid(True, alpha=0.15)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.02), fontsize=8, ncol=3, frameon=True, framealpha=0.9)

    # Ajustar formato de fecha en el eje X para que se vea limpio
    import matplotlib.dates as mdates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    
    os.makedirs(os.path.dirname(out_png_path), exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_png_path, bbox_inches="tight")
    plt.close(fig)
    return out_png_path

def summarize_critical_day(df_day: pd.DataFrame) -> pd.DataFrame:
    """Resumen por variable para TODO el día.
    Devuelve: puntos, puntos fuera, %, promedio, min, max (ordenado por % fuera desc).
    """
    if df_day is None or df_day.empty:
        return pd.DataFrame(columns=[
            "var_id","name","device","points","out_points","out_pct","avg_value","min_value","max_value"
        ])

    d = df_day.copy()
    d["var_id"] = d["ProductionLineControlVariableId"].astype(str).str.lower()
    d["Value"] = pd.to_numeric(d["Value"], errors="coerce")

    # Asegurar columna booleana para fuera de crítico
    if "IsOut" not in d.columns:
        d["CriticalMinValue"] = pd.to_numeric(d.get("CriticalMinValue"), errors="coerce")
        d["CriticalMaxValue"] = pd.to_numeric(d.get("CriticalMaxValue"), errors="coerce")
        d["IsOut"] = (d["Value"] < d["CriticalMinValue"]) | (d["Value"] > d["CriticalMaxValue"])

    g = (d.groupby("var_id", dropna=False)
         .agg(
             points=("Value","count"),
             out_points=("IsOut","sum"),
             avg_value=("Value","mean"),
             min_value=("Value","min"),
             max_value=("Value","max"),
         )
         .reset_index())

    g["out_pct"] = (g["out_points"] / g["points"] * 100.0).round(2)

    # Enriquecer con catálogo
    names, devices = [], []
    for vid in g["var_id"].tolist():
        meta = next((CRITICAL_VARS[k] for k in CRITICAL_VARS if k.strip().lower() == str(vid).lower()), None)
        names.append(meta.get("name") if meta else str(vid))
        devices.append(meta.get("device") if meta else "")
    g["name"] = names
    g["device"] = devices

    # Redondeo amigable
    g["avg_value"] = g["avg_value"].round(3)

    # Orden (más fuera primero)
    g = g.sort_values(["out_pct","out_points"], ascending=[False,False]).reset_index(drop=True)

    return g[["var_id","name","device","points","out_points","out_pct","avg_value","min_value","max_value"]]

def normalize_day_str(day: str) -> str:
    day = (day or "").strip()
    if not day:
        return day

    # Ya viene ISO: 2026-01-06
    if re.match(r"^\d{4}-\d{2}-\d{2}$", day):
        return day

    # Viene DD/MM/YYYY: 01/06/2026
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", day)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    # Último intento: parse flexible
    try:
        return pd.to_datetime(day, dayfirst=True).date().isoformat()
    except Exception:
        return day



# =========================
# Endpoints OEE (sin IA)
# =========================

def _sql_oee_realtime(line_pattern: str | None = None) -> str:
    """
    Último snapshot (RT.1). line_pattern opcional.
    """
    lp = "NULL" if not line_pattern else "N'" + str(line_pattern).replace("'", "''") + "'"
    return f"""
DECLARE @linePattern NVARCHAR(100) = {lp};

SELECT TOP (1)
    pl.Name                           AS LineName,
    pli.IntervalBegin                 AS SnapshotAtLocal,

    ROUND(pli.OEE,2)                  AS OEE,
    ROUND(pli.OEEAvailability,2)      AS Availability,
    ROUND(pli.OEEPerformance,2)       AS Performance,
    ROUND(pli.OEEQuality,2)           AS [Producto Conforme],

    -- Conteo de eventos de paros y duraciones (AGRUPADOS)
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = pli.ProductionLineId
              AND IntervalBegin >= DATEADD(MINUTE, -(
                  CASE 
                      WHEN TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin) IS NOT NULL 
                      THEN DATEDIFF(MINUTE, 0, TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin))
                      ELSE TRY_CONVERT(int, RIGHT(pli.TimeSinceLastWorkshiftBegin, 2))
                  END
              ), pli.IntervalBegin)
              AND IntervalBegin <= pli.IntervalBegin
        ) sub WHERE IntervalProductionLineStatus = 'US' AND (PrevStatus <> 'US' OR PrevStatus IS NULL)
    ) AS ParosNoProgramadosCont,
    DATEDIFF(MINUTE, 0, pli.UnscheduledStopageTime)      AS UnscheduledStopageMin,
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = pli.ProductionLineId
              AND IntervalBegin >= DATEADD(MINUTE, -(
                  CASE 
                      WHEN TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin) IS NOT NULL 
                      THEN DATEDIFF(MINUTE, 0, TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin))
                      ELSE TRY_CONVERT(int, RIGHT(pli.TimeSinceLastWorkshiftBegin, 2))
                  END
              ), pli.IntervalBegin)
              AND IntervalBegin <= pli.IntervalBegin
        ) sub WHERE IntervalProductionLineStatus = 'SS' AND (PrevStatus <> 'SS' OR PrevStatus IS NULL)
    ) AS ParosProgramadosCont,
    DATEDIFF(MINUTE, 0, pli.ScheduledStopageTime)        AS ScheduledStopageMin,

    -- Estado de la línea (con nombres completos)
    CASE pli.IntervalProductionLineStatus
        WHEN 'US' THEN N'Paro No Programado'
        WHEN 'SS' THEN N'Paro Programado'
        WHEN 'LP' THEN N'Baja Producción'
        WHEN 'AV' THEN N'Disponible'
        ELSE pli.IntervalProductionLineStatus
    END                                              AS StatusCode,

    CASE 
        WHEN TRY_CONVERT(time, pli.TimeSinceLastStatusChange) IS NOT NULL THEN
            DATEDIFF(MINUTE, 0, TRY_CONVERT(time, pli.TimeSinceLastStatusChange))
        ELSE TRY_CONVERT(int, RIGHT(pli.TimeSinceLastStatusChange, 2))
    END                                              AS StatusTimeMin,

    CASE 
        WHEN TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin) IS NOT NULL THEN
            DATEDIFF(MINUTE, 0, TRY_CONVERT(time, pli.TimeSinceLastWorkshiftBegin))
        ELSE TRY_CONVERT(int, RIGHT(pli.TimeSinceLastWorkshiftBegin, 2))
    END                                              AS NaturalTimeMin,

    DATEDIFF(MINUTE, 0, pli.EffectiveAvailableTime)      AS ProductiveTimeMin,

    pli.CurrentRate                   AS CurrentRate,
    pli.ExpectedRate                  AS ExpectedRate,

    pli.CurrentShiftProduction        AS CurrentShiftProduction,
    pli.ExpectedShiftProduction       AS ExpectedShiftProduction,
    pli.CurrentProduction             AS CurrentProduction,
    pli.ExpectedDayProduction         AS ExpectedDayProduction

FROM dbo.ProductionLineIntervals AS pli
INNER JOIN dbo.ProductionLines AS pl
    ON pli.ProductionLineId = pl.ProductionLineId
WHERE
    (@linePattern IS NULL OR pl.Name LIKE N'%' + @linePattern + N'%')
ORDER BY
    pli.IntervalBegin DESC,
    pli.CreatedAt DESC;
"""

def _sql_oee_day_turn(day: str, shift_name: str | None = None) -> str:
    """
    Resumen por turno para un día (Usa Lógica de Fecha Operativa).
    """
    # Escapar comillas simples SIN backslashes dentro del f-string
    day_safe = str(day).replace("'", "''")
    day_sql = f"CONVERT(date, '{day_safe}')"

    shift_filter = ""
    if shift_name:
        safe_shift = str(shift_name).replace("'", "''")
        shift_filter = f"\n    AND wst.Name = N'{safe_shift}'"

    return f"""
DECLARE @day DATE = {day_sql};

SELECT
    wst.Name AS Turno,

    -- Fecha operativa real (ShiftBusinessDate)
    CASE
        WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
        ELSE CAST(wse.StartDate AS date)
    END                            AS Fecha,

    wses.Oee                       AS OEE,
    wses.Availability              AS Disponibilidad,
    wses.Performance               AS Desempeno,
    wses.Quality                   AS [Producto Conforme],

    -- Conteo de eventos y duraciones (AGRUPADOS)
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = wses.ProductionLineId
              AND IntervalBegin >= wse.StartDate AND IntervalBegin < wse.EndDate
        ) sub WHERE IntervalProductionLineStatus = 'US' AND (PrevStatus <> 'US' OR PrevStatus IS NULL)
    ) AS ParosNoProgramadosCont,
    wses.UnscheduledStopageMin     AS TiempoNoProdNoProgramadoMin,
    (
        SELECT COUNT(*) FROM (
            SELECT IntervalProductionLineStatus, LAG(IntervalProductionLineStatus) OVER (ORDER BY IntervalBegin) as PrevStatus
            FROM dbo.ProductionLineIntervals
            WHERE ProductionLineId = wses.ProductionLineId
              AND IntervalBegin >= wse.StartDate AND IntervalBegin < wse.EndDate
        ) sub WHERE IntervalProductionLineStatus = 'SS' AND (PrevStatus <> 'SS' OR PrevStatus IS NULL)
    ) AS ParosProgramadosCont,
    wses.ScheduledStopageMin       AS TiempoNoProdProgramadoMin,

    wses.WorkshiftDurationMin      AS DuracionTurnoMin,
    wses.AvailableTimeMin          AS TiempoDisponibleMin,
    wses.ProductiveTimeMin         AS TiempoProductivoMin,
    wses.ExpectedProductionSummary AS ProduccionEstimadaKg,
    wses.CurrentProductionSummary  AS ProduccionRealKg,
    wses.AvgExpectedVelocity       AS VelocidadPromedioEstimadaKgHr,
    wses.AvgCurrentVelocity        AS VelocidadPromedioRealKgHr
FROM ind.WorkShiftExecutionSummaries AS wses
INNER JOIN dbo.WorkShiftExecutions AS wse
    ON wses.WorkShiftExecutionId = wse.WorkShiftExecutionId
INNER JOIN dbo.WorkShiftTemplates AS wst
    ON wse.WorkShiftTemplateId = wst.WorkShiftTemplateId
WHERE
    wse.Status = 'closed'
    AND wse.Active = 1
    AND wses.Active = 1
    AND wst.Active = 1

    -- Filtro por Fecha Operativa
    AND (
        CASE
            WHEN wst.EndTime < wst.StartTime THEN DATEADD(day, -1, CAST(wse.EndDate AS date))
            ELSE CAST(wse.StartDate AS date)
        END
    ) = @day
    {shift_filter}
ORDER BY
    Fecha,
    CASE wst.Name
        WHEN N'Primer Turno' THEN 1
        WHEN N'Segundo Turno' THEN 2
        WHEN N'Tercer Turno' THEN 3
        ELSE 9
    END;
"""


    return {"rows": rows_formatted, "columns": cols, "snapshot": snap_formatted, "ai_analysis": ai}

def plot_oee_realtime_snapshot(snap_dict: dict) -> List[dict]:
    """Genera gráficas Plotly para el snapshot de tiempo real (Turno Actual)."""
    import plotly.graph_objects as go
    
    if not snap_dict:
        return []
        
    out_dir = os.path.join("static", "plots")
    os.makedirs(out_dir, exist_ok=True)
    
    # Helper para asegurar valores numéricos
    def to_f(v):
        try:
            return float(v) if v is not None else 0.0
        except (ValueError, TypeError):
            return 0.0

    plots = []
    # Usaremos una marca de tiempo para evitar caché
    ts = int(time.time() * 1000)
    
    # --- 1. Eficiencia (OEE %) ---
    oee_val = to_f(snap_dict.get("OEE"))
    fig_oee = go.Figure(data=[
        go.Bar(
            name='OEE (%)', 
            x=["Turno Actual"], 
            y=[oee_val], 
            marker_color='#1abc9c', 
            text=[f"{oee_val:.1f}%"], 
            textposition='outside',
            width=0.4
        )
    ])
    fig_oee.update_layout(
        title="Eficiencia Global (OEE %) - Snapshot", 
        template="plotly_dark", 
        margin=dict(l=40, r=40, t=60, b=40), 
        yaxis=dict(range=[0, max(110, oee_val + 10)], ticksuffix="%")
    )
    oee_fname = f"oee_rt_kpi_{ts}.html"
    fig_oee.write_html(os.path.join(out_dir, oee_fname))
    plots.append({"title": "OEE (%)", "url": f"static/plots/{oee_fname}"})

    # --- 2. Producción (Kg) ---
    prod_real = to_f(snap_dict.get("CurrentShiftProduction"))
    prod_expected = to_f(snap_dict.get("ExpectedShiftProduction"))
    fig_prod = go.Figure(data=[
        go.Bar(name='Real', x=["Producción"], y=[prod_real], marker_color='#1abc9c'),
        go.Bar(name='Esperada', x=["Producción"], y=[prod_expected], marker_color='#6366f1')
    ])
    fig_prod.update_layout(
        title="Producción del Turno (Kg) - Snapshot", 
        template="plotly_dark", barmode='group',
        margin=dict(l=40, r=40, t=60, b=40)
    )
    prod_fname = f"oee_rt_prod_{ts}.html"
    fig_prod.write_html(os.path.join(out_dir, prod_fname))
    plots.append({"title": "Producción (Kg)", "url": f"static/plots/{prod_fname}"})

    # --- 3. Velocidad (Kg/h) ---
    vel_real = to_f(snap_dict.get("CurrentRate"))
    vel_expected = to_f(snap_dict.get("ExpectedRate"))
    fig_vel = go.Figure(data=[
        go.Bar(name='Real', x=["Velocidad"], y=[vel_real], marker_color='#1abc9c'),
        go.Bar(name='Esperada', x=["Velocidad"], y=[vel_expected], marker_color='#6366f1')
    ])
    fig_vel.update_layout(
        title="Velocidad Promedio (Kg/h) - Snapshot", 
        template="plotly_dark", barmode='group',
        margin=dict(l=40, r=40, t=60, b=40)
    )
    vel_fname = f"oee_rt_vel_{ts}.html"
    fig_vel.write_html(os.path.join(out_dir, vel_fname))
    plots.append({"title": "Velocidad (Kg/h)", "url": f"static/plots/{vel_fname}"})

    # --- 4. Paros (Duración min) ---
    dur_unsched = to_f(snap_dict.get("UnscheduledStopageMin"))
    dur_sched = to_f(snap_dict.get("ScheduledStopageMin"))
    fig_stops = go.Figure(data=[
        go.Bar(name='No Programado', x=["Paros"], y=[dur_unsched], marker_color='#ef4444'),
        go.Bar(name='Programado', x=["Paros"], y=[dur_sched], marker_color='#f59e0b')
    ])
    fig_stops.update_layout(
        title="Distribución de Paros (Minutos) - Snapshot", 
        template="plotly_dark", barmode='group',
        margin=dict(l=40, r=40, t=60, b=40)
    )
    stops_fname = f"oee_rt_stops_{ts}.html"
    fig_stops.write_html(os.path.join(out_dir, stops_fname))
    plots.append({"title": "Tiempos de Paro (Min)", "url": f"static/plots/{stops_fname}"})

    # --- 5. Frecuencia de Paros (Eventos) ---
    cnt_unsched = to_f(snap_dict.get("ParosNoProgramadosCont"))
    cnt_sched = to_f(snap_dict.get("ParosProgramadosCont"))
    fig_freq = go.Figure(data=[
        go.Bar(name='No Programado', x=["Eventos"], y=[cnt_unsched], marker_color='#ef4444'),
        go.Bar(name='Programado', x=["Eventos"], y=[cnt_sched], marker_color='#f59e0b')
    ])
    fig_freq.update_layout(
        title="Frecuencia de Paros (Eventos) - Snapshot", 
        template="plotly_dark", barmode='group',
        margin=dict(l=40, r=40, t=60, b=40)
    )

    freq_fname = f"oee_rt_freq_{ts}.html"
    fig_freq.write_html(os.path.join(out_dir, freq_fname))
    plots.append({"title": "Frecuencia de Paros", "url": f"static/plots/{freq_fname}"})
    
    return plots

@app.get("/api/oee/realtime/")
async def api_oee_realtime():
    """OEE en tiempo real (último snapshot)."""
    rows, cols = run_sql(_sql_oee_realtime())
    if not rows:
        return {"rows": [], "columns": cols, "snapshot": None, "ai_analysis": "", "plots": []}

    # Registro original para las gráficas (antes de formatear tiempos)
    raw_snap = dict(zip(cols, rows[0]))

    # Identificamos columnas de duración para formatear
    duration_cols = [
        "StatusTimeMin", "NaturalTimeMin", "ProductiveTimeMin", 
        "ScheduledStopageMin", "UnscheduledStopageMin"
    ]

    # Formateamos todas las filas para la tabla
    rows_formatted = []
    for r in rows:
        r_dict = dict(zip(cols, r))
        for col in duration_cols:
            if col in r_dict:
                r_dict[col] = format_duration_es(r_dict[col])
        rows_formatted.append([r_dict.get(c) for c in cols])

    # Snapshot = primer registro formateado para la IA y los KPIs
    snap_formatted = dict(zip(cols, rows_formatted[0]))

    # Gráficas
    plots = plot_oee_realtime_snapshot(raw_snap)

    # IA (si está configurada)
    ai = ai_oee_realtime(snap_formatted)

    return {"rows": rows_formatted, "columns": cols, "snapshot": snap_formatted, "ai_analysis": ai, "plots": plots}

def plot_oee_historical_comparison(day: str, rows_dicts: List[dict]) -> List[dict]:
    """Genera gráficas Plotly para comparar métricas por turno."""
    import plotly.graph_objects as go
    
    if not rows_dicts:
        return []
        
    out_dir = os.path.join("static", "plots")
    os.makedirs(out_dir, exist_ok=True)
    
    plots = []
    
    # Ordenar por turno
    shift_order = {"Primer Turno": 1, "Segundo Turno": 2, "Tercer Turno": 3}
    data = sorted(rows_dicts, key=lambda x: shift_order.get(x.get("Turno"), 9))
    shifts = [r.get("Turno") for r in data]
    
def plot_oee_historical_comparison(day: str, rows_dicts: List[dict]) -> List[dict]:
    """Genera gráficas Plotly para comparar métricas por turno."""
    import plotly.graph_objects as go
    
    if not rows_dicts:
        return []
        
    out_dir = os.path.join("static", "plots")
    os.makedirs(out_dir, exist_ok=True)
    
    plots = []
    
    # Helper para asegurar valores numéricos
    def to_f(v):
        try:
            return float(v) if v is not None else 0.0
        except (ValueError, TypeError):
            return 0.0

    # Ordenar por turno
    shift_order = {"Primer Turno": 1, "Segundo Turno": 2, "Tercer Turno": 3}
    data = sorted(rows_dicts, key=lambda x: shift_order.get(x.get("Turno"), 9))
    shifts = [r.get("Turno") for r in data]
    
    # --- 1. Eficiencia (OEE %) ---
    oee_values = [to_f(r.get("OEE")) for r in data]
    fig_oee = go.Figure(data=[
        go.Bar(
            name='OEE (%)', 
            x=shifts, 
            y=oee_values, 
            marker_color='#1abc9c', 
            text=[f"{v:.1f}%" for v in oee_values], 
            textposition='outside',
            width=0.5
        )
    ])
    fig_oee.update_layout(
        title="Eficiencia Global (OEE %) por Turno", 
        template="plotly_dark", 
        margin=dict(l=40, r=40, t=60, b=40), 
        yaxis=dict(range=[0, max(105, max(oee_values or [0]) + 10)], ticksuffix="%")
    )
    oee_fname = f"oee_kpi_{day}.html"
    oee_png = f"oee_kpi_{day}.png"
    fig_oee.write_html(os.path.join(out_dir, oee_fname))
    try:
        fig_oee.write_image(os.path.join(out_dir, oee_png), engine="kaleido")
    except Exception: pass
    plots.append({"title": "Indicador OEE (%)", "url": f"static/plots/{oee_fname}", "path": os.path.join(out_dir, oee_png)})

    # --- 2. Producción ---
    real_prod = [to_f(r.get("ProduccionRealKg")) for r in data]
    est_prod = [to_f(r.get("ProduccionEstimadaKg")) for r in data]
    fig_prod = go.Figure(data=[
        go.Bar(name='Real (Kg)', x=shifts, y=real_prod, marker_color='#1abc9c'),
        go.Bar(name='Esperada (Kg)', x=shifts, y=est_prod, marker_color='#6366f1')
    ])
    fig_prod.update_layout(title="Producción Real vs Esperada por Turno", barmode='group', template="plotly_dark", margin=dict(l=40, r=40, t=60, b=40))
    prod_fname = f"oee_prod_{day}.html"
    prod_png = f"oee_prod_{day}.png"
    fig_prod.write_html(os.path.join(out_dir, prod_fname))
    try:
        fig_prod.write_image(os.path.join(out_dir, prod_png), engine="kaleido")
    except Exception: pass
    plots.append({"title": "Comparativa de Producción", "url": f"static/plots/{prod_fname}", "path": os.path.join(out_dir, prod_png)})
    
    # --- 3. Velocidad ---
    real_vel = [to_f(r.get("VelocidadPromedioRealKgHr")) for r in data]
    est_vel = [to_f(r.get("VelocidadPromedioEstimadaKgHr")) for r in data]
    fig_vel = go.Figure(data=[
        go.Bar(name='Real (Kg/h)', x=shifts, y=real_vel, marker_color='#1abc9c'),
        go.Bar(name='Esperada (Kg/h)', x=shifts, y=est_vel, marker_color='#6366f1')
    ])
    fig_vel.update_layout(title="Velocidad Real vs Esperada por Turno", barmode='group', template="plotly_dark", margin=dict(l=40, r=40, t=60, b=40))
    vel_fname = f"oee_vel_{day}.html"
    vel_png = f"oee_vel_{day}.png"
    fig_vel.write_html(os.path.join(out_dir, vel_fname))
    try:
        fig_vel.write_image(os.path.join(out_dir, vel_png), engine="kaleido")
    except Exception: pass
    plots.append({"title": "Comparativa de Velocidad", "url": f"static/plots/{vel_fname}", "path": os.path.join(out_dir, vel_png)})

    # --- 4. Paros ---
    un_stop = [to_f(r.get("TiempoNoProdNoProgramadoMin")) for r in data]
    sch_stop = [to_f(r.get("TiempoNoProdProgramadoMin")) for r in data]
    fig_stop = go.Figure(data=[
        go.Bar(name='No Programado (Min)', x=shifts, y=un_stop, marker_color='#ef4444'),
        go.Bar(name='Programado (Min)', x=shifts, y=sch_stop, marker_color='#f59e0b')
    ])
    fig_stop.update_layout(title="Tiempo de Paro por Turno (Minutos)", barmode='stack', template="plotly_dark", margin=dict(l=40, r=40, t=60, b=40))
    stop_fname = f"oee_stops_{day}.html"
    stop_png = f"oee_stops_{day}.png"
    fig_stop.write_html(os.path.join(out_dir, stop_fname))
    try:
        fig_stop.write_image(os.path.join(out_dir, stop_png), engine="kaleido")
    except Exception: pass
    plots.append({"title": "Distribución de Paros", "url": f"static/plots/{stop_fname}", "path": os.path.join(out_dir, stop_png)})
    
    # --- 5. Frecuencia de Paros ---
    un_stop_cnt = [to_f(r.get("ParosNoProgramadosCont")) for r in data]
    sch_stop_cnt = [to_f(r.get("ParosProgramadosCont")) for r in data]
    fig_stop_cnt = go.Figure(data=[
        go.Bar(name='Eventos No Programados', x=shifts, y=un_stop_cnt, marker_color='#ef4444'),
        go.Bar(name='Eventos Programados', x=shifts, y=sch_stop_cnt, marker_color='#f59e0b')
    ])
    fig_stop_cnt.update_layout(title="Frecuencia de Paros por Turno (Eventos)", barmode='group', template="plotly_dark", margin=dict(l=40, r=40, t=60, b=40))

    stop_cnt_fname = f"oee_stop_counts_{day}.html"
    stop_cnt_png = f"oee_stop_counts_{day}.png"
    fig_stop_cnt.write_html(os.path.join(out_dir, stop_cnt_fname))
    try:
        fig_stop_cnt.write_image(os.path.join(out_dir, stop_cnt_png), engine="kaleido")
    except Exception: pass
    plots.append({"title": "Frecuencia de Paros", "url": f"static/plots/{stop_cnt_fname}", "path": os.path.join(out_dir, stop_cnt_png)})
    
    return plots


@app.post("/api/oee/day-turn/")
async def api_oee_day_turn(payload: dict):
    """
    OEE por día/turno. Body: { "day": "YYYY-MM-DD", "shift_name"?: "Primer Turno"|"Segundo Turno"|"Tercer Turno" }
    """
    day = (payload.get("day") or "").strip()
    shift_name = payload.get("shift_name")
    if not day:
        raise HTTPException(status_code=400, detail="Falta 'day' (YYYY-MM-DD).")
    
    rows, cols = run_sql(_sql_oee_day_turn(day, shift_name))
    if not rows:
        return {"day": day, "shift_name": shift_name, "rows": [], "columns": cols, "ai_analysis": "No se encontraron datos.", "plots": []}

    # Dicts para IA y para generar gráficas (antes de formatear tiempos)
    rows_dicts_raw = [dict(zip(cols, r)) for r in rows]

    # Generar gráficas si hay datos
    plots = plot_oee_historical_comparison(day, rows_dicts_raw)

    # Formatear duraciones para el reporte final (tablas e IA)
    duration_cols = [
        "DuracionTurnoMin", "TiempoDisponibleMin", "TiempoProductivoMin", 
        "TiempoNoProdProgramadoMin", "TiempoNoProdNoProgramadoMin"
    ]
    
    rows_dicts_formatted = []
    for r in rows_dicts_raw:
        new_row = dict(r)
        for col in duration_cols:
            if col in new_row:
                new_row[col] = format_duration_es(new_row[col])
        rows_dicts_formatted.append(new_row)

    # IA recibe los datos ya formateados para que hable en "horas y minutos"
    ai = ai_oee_day_turn(day, rows_dicts_formatted, shift_name)

    # Para el frontend: convertimos de vuelta a lista de listas usando los dicts formateados
    rows_final = []
    for r_dict in rows_dicts_formatted:
        rows_final.append([r_dict.get(c) for c in cols])

    return {
        "day": day, 
        "shift_name": shift_name, 
        "rows": rows_final, 
        "columns": cols, 
        "ai_analysis": ai,
        "plots": plots
    }


@app.post("/api/cv/day/")
async def api_control_variables_day(payload: dict):
    """Devuelve plots + resumen para TODO el día (3 turnos) de variables críticas."""
    day = normalize_day_str(payload.get("day") or "")
    if not day:
        raise HTTPException(status_code=400, detail="Falta 'day' (YYYY-MM-DD o DD/MM/YYYY).")

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", day):
        raise HTTPException(status_code=400, detail="Formato de 'day' inválido. Usa YYYY-MM-DD o DD/MM/YYYY.")

    try:
        df_day = load_critical_reads_for_day(day)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    out_dir = os.path.join("static", "plots")
    os.makedirs(out_dir, exist_ok=True)

    plots = []
    if not df_day.empty:
        var_ids = sorted(
            df_day["ProductionLineControlVariableId"]
            .dropna()
            .astype(str)
            .str.lower()
            .unique()
            .tolist()
        )

        for vid in var_ids:
            meta = next((CRITICAL_VARS[k] for k in CRITICAL_VARS if k.lower() == vid), None)
            safe_name = (meta.get("device", "var") + "_" + meta.get("name", "var")) if meta else vid
            safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", safe_name.strip().lower()).strip("_")

            filename = f"{day}_dia_{safe_name}.html"
            out_path = os.path.join(out_dir, filename)

            plot_critical_timeseries_day(df_day, vid, out_path)

            plots.append({
                "var_id": vid,
                "title": f"{meta.get('name','Variable')} — {meta.get('device','')}".strip(" —") if meta else vid,
                "url": f"static/plots/{filename}"
            })

    summary_df = summarize_critical_day(df_day)
    summary = summary_df.to_dict(orient="records")

    exec_lines = []
    if summary:
        worst = summary[0]
        exec_lines.append(f"Resumen ejecutivo ({day}):")
        exec_lines.append(f"- Variables críticas analizadas: {len(summary)}")
        exec_lines.append(
            f"- Mayor % fuera de crítico: {worst.get('name','')} — {worst.get('device','')} ({worst.get('out_pct',0)}%)"
        )
        for i, r in enumerate(summary[:3], start=1):
            exec_lines.append(
                f"  {i}) {r.get('name','')} — {r.get('device','')}: {r.get('out_pct',0)}% fuera "
                f"({r.get('out_points',0)}/{r.get('points',0)} pts)"
            )

    executive_summary = "\n".join(exec_lines)

    ai_analysis = ai_control_variables_day(day=day, summary=summary, executive_summary=executive_summary)

    return {
        "day": day,
        "plots": plots,
        "summary": summary,
        "executive_summary": executive_summary,
        "ai_analysis": ai_analysis
    }




@app.get("/", response_class=HTMLResponse)
async def root():
    return FileResponse("static/index.html")


@app.get("/Bafar", response_class=HTMLResponse)
@app.get("/Bafar/", response_class=HTMLResponse)
async def bafar_page():
    return FileResponse("static/index.html")


@app.post("/chat/")
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


# =========================================================
# Reportes descargables (PDF / Word)
# =========================================================

def _report_filename(prefix: str, ext: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", prefix).strip("_")
    return f"{safe}.{ext}"


def _build_pdf_bytes(
    title: str,
    subtitle: str,
    sections: List[dict],
    table_title: str,
    table_rows: List[dict],
    logo_path: str | None = None
) -> bytes:
    """Genera PDF (bytes) con estilo moderno "Duma Teal":
    - Título + subtítulo + logo
    - Secciones con texto tipo markdown simple (##/###, viñetas, párrafos)
    - Tabla con encabezado estileada
    - (Opcional) imágenes por sección (paths a PNG/JPG)
    """
    import io
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
    from reportlab.lib.units import inch
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfgen import canvas
    from datetime import datetime

    # --- PALETA DE COLORES (Matches Frontend) ---
    COLOR_BRAND = colors.HexColor("#1abc9c")       # Teal brillante
    COLOR_BRAND_DARK = colors.HexColor("#16a085")  # Teal oscuro
    COLOR_TEXT = colors.HexColor("#0f172a")        # Navy dark (texto principal)
    COLOR_TEXT_MUTED = colors.HexColor("#64748b")  # Gray blue (texto secundario)
    COLOR_BG_LIGHT = colors.HexColor("#f0fdfa")    # Light Teal BG (para filas tabla)
    COLOR_ACCENT = colors.HexColor("#0f172a")      # Headings
    
    def _safe(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def _strip_md(s: str) -> str:
        s = re.sub(r"\*\*(.+?)\*\*", r"\1", s or "")
        s = re.sub(r"`([^`]+)`", r"\1", s)
        s = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", s)
        return s

    def _md_to_flowables(md: str, styles) -> List:
        out = []
        if not md: return out

        # Normalizar saltos de línea
        md = md.replace("\r\n", "\n").replace("\r", "\n")

        # SEPARACIÓN INTELIGENTE (Lista solo si viene después de puntuación)
        md = re.sub(r'([^\n])\s*###', r'\1\n\n###', md) # Header pegado
        md = re.sub(r'([.?!:])\s*(\d+[\.\)]\s+)', r'\1\n\2', md) # Número tras punto
        md = re.sub(r'([.?!:])\s*([-*]\s+)', r'\1\n\2', md) # Bullet tras punto

        # Split headers conocidos que traen contenido en la misma línea
        COMMON_HEADERS = [
            "Resumen ejecutivo", "Hallazgos clave", "Interpretación operacional", 
            "Acciones recomendadas", "Próximos pasos", "KPI limitante", "Riesgo si no se actúa"
        ]
        for h in COMMON_HEADERS:
            md = re.sub(rf'(###\s+{re.escape(h)})\.?(\s+[^ \n])', r'\1\n\n\2', md, flags=re.IGNORECASE)

        lines = [ln.rstrip() for ln in md.split("\n")]
        buf = []

        def flush_paragraph():
            nonlocal buf
            if buf:
                txtp = " ".join([b.strip() for b in buf]).strip()
                txtp = _safe(_strip_md(txtp))
                if txtp and txtp != ".":
                    out.append(Paragraph(txtp, styles["Body"]))
                    out.append(Spacer(1, 8))
                buf = []

        for ln in lines:
            l = ln.strip()
            if not l:
                flush_paragraph()
                continue
            
            # Headers
            if l.startswith("### "):
                flush_paragraph()
                raw = l[4:].strip()
                
                # Intentar detectar si el header tiene contenido pegado
                found_h = next((h for h in COMMON_HEADERS if raw.lower().startswith(h.lower())), None)
                if found_h and len(raw) > len(found_h) + 10:
                    title_text = raw[:len(found_h)].strip()
                    body_text = raw[len(found_h):].strip()
                    out.append(Paragraph(_safe(_strip_md(title_text)), styles["H3"]))
                    out.append(Spacer(1, 4))
                    if body_text:
                        out.append(Paragraph(_safe(_strip_md(body_text)), styles["Body"]))
                        out.append(Spacer(1, 8))
                else:
                    out.append(Paragraph(_safe(_strip_md(raw)), styles["H3"]))
                    out.append(Spacer(1, 4))
                continue

            if l.startswith("## "):
                flush_paragraph()
                out.append(Paragraph(_safe(_strip_md(l[3:])), styles["H2"]))
                out.append(Spacer(1, 8))
                continue
            if l.startswith("# "):
                flush_paragraph()
                out.append(Paragraph(_safe(_strip_md(l[2:])), styles["H1"]))
                out.append(Spacer(1, 10))
                continue
            
            # Bullets
            if l.startswith("- ") or l.startswith("* "):
                flush_paragraph()
                content = l[2:].strip()
                out.append(Paragraph("• " + _safe(_strip_md(content)), styles["Bullet"]))
                continue

            # Numeradas
            match_num = re.match(r"^(\d+[\.\)])\s+(.*)", l)
            if match_num:
                flush_paragraph()
                out.append(Paragraph(f"{match_num.group(1)} { _safe(_strip_md(match_num.group(2)))}", styles["Bullet"]))
                continue

            # Ignorar puntos solos que a veces mete la IA
            if l == ".": continue

            buf.append(l)

        flush_paragraph()
        return out

    # -------- Page Template (Header/Footer) --------
    def on_page(canvas, doc):
        canvas.saveState()
        w, h = doc.pagesize
        
        # Header Bar (Teal gradient simulation)
        canvas.setFillColor(COLOR_BG_LIGHT)
        canvas.rect(0, h - 0.5*inch, w, 0.5*inch, fill=1, stroke=0)
        
        # Footer Line
        canvas.setStrokeColor(COLOR_BRAND)
        canvas.setLineWidth(1)
        canvas.line(0.7*inch, 0.75*inch, w - 0.7*inch, 0.75*inch)
        
        # Footer Text
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(COLOR_TEXT_MUTED)
        page_num = f"Página {doc.page}"
        date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        canvas.drawString(0.7*inch, 0.5*inch, f"Duma Analytics — {date_str}")
        canvas.drawRightString(w - 0.7*inch, 0.5*inch, page_num)
        
        canvas.restoreState()

    # -------- Styles --------
    ss = getSampleStyleSheet()
    styles = {}
    styles["Title"] = ParagraphStyle("DumaTitle", parent=ss["Title"], fontName="Helvetica-Bold", fontSize=24, leading=28, textColor=COLOR_BRAND, spaceAfter=8)
    styles["Sub"] = ParagraphStyle("DumaSub", parent=ss["Normal"], fontName="Helvetica", fontSize=12, leading=14, textColor=COLOR_TEXT_MUTED, spaceAfter=24)
    styles["H1"] = ParagraphStyle("DumaH1", parent=ss["Heading1"], fontName="Helvetica-Bold", fontSize=16, leading=20, textColor=COLOR_ACCENT, spaceAfter=10, spaceBefore=4)
    styles["H2"] = ParagraphStyle("DumaH2", parent=ss["Heading2"], fontName="Helvetica-Bold", fontSize=13, leading=16, textColor=COLOR_BRAND_DARK, spaceAfter=8, spaceBefore=4)
    styles["H3"] = ParagraphStyle("DumaH3", parent=ss["Heading3"], fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=COLOR_BRAND, spaceAfter=6)
    styles["Body"] = ParagraphStyle("DumaBody", parent=ss["BodyText"], fontName="Helvetica", fontSize=10, leading=14, textColor=COLOR_TEXT)
    styles["Bullet"] = ParagraphStyle("DumaBullet", parent=ss["BodyText"], fontName="Helvetica", fontSize=10, leading=14, textColor=COLOR_TEXT, leftIndent=14, bulletIndent=6, spaceAfter=4)

    buffer = io.BytesIO()
    
    # Decide orientation: Landscape if table has > 5 columns
    use_landscape = False
    if table_rows and len(table_rows[0].keys()) > 5:
        use_landscape = True
        
    page_size = landscape(letter) if use_landscape else letter
    
    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        leftMargin=0.7*inch, rightMargin=0.7*inch,
        topMargin=0.8*inch, bottomMargin=1*inch
    )

    story = []

    # --- Cover Section ---
    if logo_path and os.path.exists(logo_path):
        try:
            ir = ImageReader(logo_path)
            w, h = ir.getSize()
            max_w = 2.0*inch  # Bigger logo
            max_h = 1.0*inch
            scale = min(max_w / float(w), max_h / float(h))
            img = Image(logo_path, width=w*scale, height=h*scale)
            img.hAlign = "LEFT"
            story.append(img)
            story.append(Spacer(1, 12))
        except Exception:
            pass

    story.append(Paragraph(_safe(title), styles["Title"]))
    if subtitle:
        story.append(Paragraph(_safe(subtitle), styles["Sub"]))
        
    story.append(Spacer(1, 12))

    # --- Normalize Sections ---
    norm_sections = []
    for sec in (sections or []):
        if isinstance(sec, dict):
            norm_sections.append(sec)
        elif isinstance(sec, (list, tuple)) and len(sec) >= 2:
            norm_sections.append({"title": str(sec[0] or ""), "text": str(sec[1] or "")})
        elif isinstance(sec, str):
            norm_sections.append({"title": "", "text": sec})
        else:
            norm_sections.append({"title": "", "text": str(sec)})

    # --- Content ---
    for sec in norm_sections:
        sec_title = (sec.get("title") or "").strip()
        sec_text = (sec.get("text") or sec.get("content") or "").strip()
        sec_images = sec.get("images") or []

        if sec_title:
            story.append(Paragraph(_safe(_strip_md(sec_title)), styles["H2"]))
            
        story.extend(_md_to_flowables(sec_text, styles))

        for img_path in sec_images:
            if not img_path or not os.path.exists(img_path): continue
            try:
                ir = ImageReader(img_path)
                w, h = ir.getSize()
                # Adjust max width based on orientation
                avail_w_inch = 9.0 if use_landscape else 7.0
                available_w = avail_w_inch*inch
                available_h = 5.0*inch
                scale = min(available_w/float(w), available_h/float(h))
                story.append(Image(img_path, width=w*scale, height=h*scale))
                story.append(Spacer(1, 12))
            except Exception:
                continue
        
        story.append(Spacer(1, 12))

    # --- Table ---
    if table_rows:
        story.append(PageBreak()) # Start table on new page if complex? Optional.
        story.append(Paragraph(_safe(table_title or "Detalle de Datos"), styles["H2"]))
        story.append(Spacer(1, 8))

        cols = list(table_rows[0].keys())
        raw_data = [cols] + [[r.get(c, "") for c in cols] for r in table_rows]

        # --- Dynamic Fit Logic ---
        # 1. Calculate available width
        page_w, page_h = page_size
        # margins defined in SimpleDocTemplate: left=0.7*inch, right=0.7*inch
        avail_width = page_w - (1.4 * inch)
        
        num_cols = len(cols)
        if num_cols > 0:
            col_width = avail_width / num_cols
            col_widths = [col_width] * num_cols
        else:
            col_widths = None

        # 2. Define ParagraphStyles for table content (Header vs Body) to allow wrapping
        tbl_header_style = ParagraphStyle(
            "TblHead", 
            parent=styles["Body"], 
            fontName="Helvetica-Bold", 
            fontSize=7, 
            leading=8, 
            alignment=1, # Center
            textColor=colors.white
        )
        tbl_body_style = ParagraphStyle(
            "TblBody", 
            parent=styles["Body"], 
            fontName="Helvetica", 
            fontSize=7, 
            leading=8, 
            alignment=1, # Center
            textColor=colors.black
        )

        # 3. Wrap content in Paragraphs
        final_data = []
        
        # Header row
        header_row = []
        for c in cols:
            header_row.append(Paragraph(_safe(str(c)), tbl_header_style))
        final_data.append(header_row)
        
        # Body rows
        for row in raw_data[1:]:
            processed_row = []
            for cell in row:
                # Convert None or non-string to string
                txt = str(cell) if cell is not None else ""
                processed_row.append(Paragraph(_safe(txt), tbl_body_style))
            final_data.append(processed_row)

        # 4. Create Table with explicit widths
        tbl = Table(final_data, colWidths=col_widths, repeatRows=1)
        
        # Zebra striping styling
        tbl_style = [
            ("BACKGROUND", (0,0), (-1,0), COLOR_BRAND),         # Header BG Teal
            # Text color is handled by Paragraph style, but we keep this for safety
            ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
            ("TOPPADDING", (0,0), (-1,-1), 4),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
            ("LEFTPADDING", (0,0), (-1,-1), 2),
            ("RIGHTPADDING", (0,0), (-1,-1), 2),
            ("GRID", (0,0), (-1,-1), 0.5, colors.lightgrey),
        ]
        
        # Add Zebrastripes
        for i in range(1, len(final_data)):
            if i % 2 == 0:
                bg = COLOR_BG_LIGHT # Light Teal for even rows
            else:
                bg = colors.white
            tbl_style.append(("BACKGROUND", (0, i), (-1, i), bg))

        tbl.setStyle(TableStyle(tbl_style))
        story.append(tbl)
    
    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    return buffer.getvalue()


def _as_file_response(content: bytes, filename: str, media_type: str):
    tmp_path = os.path.join("static", "reports")
    os.makedirs(tmp_path, exist_ok=True)
    full = os.path.join(tmp_path, filename)
    with open(full, "wb") as f:
        f.write(content)
    return FileResponse(full, media_type=media_type, filename=filename)


@app.post("/api/report/cv/day/")
async def report_control_variables_day(payload: dict):
    """Descarga reporte (PDF/DOCX) de Variables de Control para un día completo."""
    day = normalize_day_str(payload.get("day") or "")
    fmt = (payload.get("format") or "pdf").lower()
    provided_summary = payload.get("summary")
    provided_ai = payload.get("ai_analysis")

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", day):
        raise HTTPException(status_code=400, detail="Formato de 'day' inválido. Usa YYYY-MM-DD.")

    try:
        # Siempre cargamos df_day si queremos gráficas (y para el resumen si no viene)
        df_day = load_critical_reads_for_day(day)
        
        if provided_summary is not None:
            summary_rows = provided_summary
        else:
            summary_df = summarize_critical_day(df_day)
            summary_rows = summary_df.to_dict(orient="records")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No hay datos para el día {day}.")
    except Exception as e:
        print(f"Error generando reporte PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

    # ---------- Resumen ejecutivo backend (corto) ----------
    exec_lines = []
    if summary_rows:
        worst = summary_rows[0]
        exec_lines.append(f"- Variables analizadas: {len(summary_rows)}")
        exec_lines.append(
            f"- Mayor % fuera de crítico: {worst.get('name','')} — {worst.get('device','')} ({worst.get('out_pct',0)}%)"
        )
        for i, r in enumerate(summary_rows[:3], start=1):
            exec_lines.append(
                f"- Top {i}: {r.get('name','')} — {r.get('device','')}: {r.get('out_pct',0)}% "
                f"({r.get('out_points',0)}/{r.get('points',0)} pts)"
            )
    executive_summary = "\n".join(exec_lines)

    # ---------- IA (SIEMPRE AL FINAL) ----------
    ai_text = provided_ai if provided_ai is not None else ""
    if provided_ai is None:
        try:
            # OJO: tu función ai_control_variables_day requiere (day, summary, executive_summary)
            ai_text = ai_control_variables_day(day=day, summary=summary_rows, executive_summary=executive_summary)
        except Exception:
            ai_text = ""

    # ---------- PNGs para el reporte ----------
    png_dir = os.path.join("static", "report_imgs")
    os.makedirs(png_dir, exist_ok=True)

    images = []
    if df_day is not None and not df_day.empty:
        var_ids = sorted(df_day["ProductionLineControlVariableId"].dropna().astype(str).str.lower().unique().tolist())
        for vid in var_ids:
            meta = next((CRITICAL_VARS[k] for k in CRITICAL_VARS if k.strip().lower() == vid), None)
            safe_name = (meta.get("device","var") + "_" + meta.get("name","var")) if meta else vid
            safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", safe_name.strip().lower()).strip("_")
            out_png = os.path.join(png_dir, f"{day}_control_{safe_name}.png")
            p = plot_critical_timeseries_day_png(df_day, vid, out_png)
            if p and os.path.exists(p):
                images.append({
                    "title": f"{meta.get('name','Variable')} — {meta.get('device','')}".strip(" —") if meta else vid,
                    "path": p
                })

    # ---------- Tabla (nombres ejecutivos) ----------
    table = []
    for r in summary_rows:
        table.append({
            "Equipo": r.get("device",""),
            "Variable": r.get("name",""),
            "Lecturas": r.get("points",0),
            "Fuera de crítico": r.get("out_points",0),
            "% fuera": r.get("out_pct",0),
            "Promedio": r.get("avg_value",""),
            "Mín": r.get("min_value",""),
            "Máx": r.get("max_value",""),
        })

    title = "Reporte — Variables de Control"
    subtitle = f"Día: {day}"

    sections = []
    sections.append({"title": "Resumen ejecutivo", "text": executive_summary or "- (Sin datos)"})

    if images:
        # Aquí van las gráficas ANTES de la IA (como quieres en el documento)
        sections.append({
            "title": "Gráficas (PNG)",
            "text": "Lecturas por variable (día completo).",
            "images": [x["path"] for x in images]
        })

    # IA SIEMPRE AL FINAL
    if ai_text:
        sections.append({"title": "Análisis mediante IA (Duma)", "text": ai_text})

    fmt = (fmt or "pdf").lower()

    if fmt in ("docx", "word"):
        content = _build_docx_bytes(title, subtitle, sections, "Métricas por variable", table, logo_path=_LOGO_PATH)
        filename = f"variables_control_{day}.docx"
        return Response(
            content=content,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    content = _build_pdf_bytes(title, subtitle, sections, "Métricas por variable", table, logo_path=_LOGO_PATH)
    filename = f"variables_control_{day}.pdf"
    return Response(
        content=content,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


def _generate_oee_rt_pngs(snap_dict: dict) -> List[str]:
    """Genera versiones estáticas (PNG) de las gráficas de tiempo real para el PDF."""
    import plotly.graph_objects as go
    import os, uuid
    
    if not snap_dict: return []
    
    out_dir = os.path.join("static", "plots")
    os.makedirs(out_dir, exist_ok=True)
    
    def to_f(v):
        try: return float(v) if v is not None else 0.0
        except: return 0.0

    image_paths = []
    uid = uuid.uuid4().hex[:8]

    # 1. OEE %
    oee_val = to_f(snap_dict.get("OEE"))
    fig = go.Figure(go.Bar(x=["Turno Actual"], y=[oee_val], marker_color='#1abc9c', text=[f"{oee_val:.1f}%"], textposition='outside'))
    fig.update_layout(template="plotly_dark", title="Eficiencia OEE (%)", margin=dict(l=20, r=20, t=40, b=20), height=300)
    p1 = os.path.join(out_dir, f"oee_rt_kpi_{uid}.png")
    fig.write_image(p1, engine="kaleido")
    image_paths.append(p1)

    # 2. Producción
    prod_real = to_f(snap_dict.get("CurrentShiftProduction"))
    prod_exp = to_f(snap_dict.get("ExpectedShiftProduction"))
    fig = go.Figure([go.Bar(name='Real', x=["Prod"], y=[prod_real], marker_color='#1abc9c'), go.Bar(name='Esperado', x=["Prod"], y=[prod_exp], marker_color='#34495e')])
    fig.update_layout(template="plotly_dark", barmode='group', title="Producción Turno (Kg)", margin=dict(l=20, r=20, t=40, b=20), height=300)
    p2 = os.path.join(out_dir, f"oee_rt_prod_{uid}.png")
    fig.write_image(p2, engine="kaleido")
    image_paths.append(p2)

    # 3. Velocidad
    vel_real = to_f(snap_dict.get("CurrentRate"))
    vel_exp = to_f(snap_dict.get("ExpectedRate"))
    fig = go.Figure([go.Bar(name='Real', x=["Velocidad"], y=[vel_real], marker_color='#1abc9c'), go.Bar(name='Esperado', x=["Velocidad"], y=[vel_exp], marker_color='#34495e')])
    fig.update_layout(template="plotly_dark", barmode='group', title="Velocidad (Kg/h)", margin=dict(l=20, r=20, t=40, b=20), height=300)
    p3 = os.path.join(out_dir, f"oee_rt_vel_{uid}.png")
    fig.write_image(p3, engine="kaleido")
    image_paths.append(p3)

    # 4. Paros
    dur_us = to_f(snap_dict.get("UnscheduledStopageMin"))
    dur_ss = to_f(snap_dict.get("ScheduledStopageMin"))
    fig = go.Figure([go.Bar(name='No Prog', x=["Mins"], y=[dur_us], marker_color='#e74c3c'), go.Bar(name='Prog', x=["Mins"], y=[dur_ss], marker_color='#f1c40f')])
    fig.update_layout(template="plotly_dark", barmode='group', title="Tiempos Paros (Min)", margin=dict(l=20, r=20, t=40, b=20), height=300)
    p4 = os.path.join(out_dir, f"oee_rt_stops_{uid}.png")
    fig.write_image(p4, engine="kaleido")
    image_paths.append(p4)

    # 5. Frecuencia
    cnt_us = to_f(snap_dict.get("ParosNoProgramadosCont"))
    cnt_ss = to_f(snap_dict.get("ParosProgramadosCont"))
    fig = go.Figure([go.Bar(name='No Prog', x=["Eventos"], y=[cnt_us], marker_color='#e74c3c'), go.Bar(name='Prog', x=["Eventos"], y=[cnt_ss], marker_color='#f1c40f')])
    fig.update_layout(template="plotly_dark", barmode='group', title="Frecuencia Paros", margin=dict(l=20, r=20, t=40, b=20), height=300)
    p5 = os.path.join(out_dir, f"oee_rt_freq_{uid}.png")
    fig.write_image(p5, engine="kaleido")
    image_paths.append(p5)

    return image_paths


@app.post("/api/report/oee/realtime/")
async def report_oee_realtime(payload: dict):
    """Descarga reporte (PDF/DOCX) de OEE en tiempo real (último snapshot)."""
    fmt = (payload.get("format") or "pdf").lower()
    provided_rows = payload.get("rows")
    provided_cols = payload.get("columns")
    provided_ai = payload.get("ai_analysis")

    if provided_rows and provided_cols:
        rows = provided_rows
        cols = provided_cols
    else:
        # Fallback si no hay datos en el body
        data = await api_oee_realtime()
        rows = data.get("rows") or []
        cols = data.get("columns") or []

    if not rows or not cols:
        raise HTTPException(status_code=404, detail="No hay datos de OEE en tiempo real.")

    # Usamos el primer snapshot para el reporte
    row = dict(zip(cols, rows[0]))

    def fmt_num(x, suffix=""):
        try:
            val = float(x)
            return f"{val:.2f}{suffix}"
        except:
            return str(x)

    # Tabla ampliada de métricas
    table = [
        {"Métrica": "OEE (%)", "Valor": fmt_num(row.get("OEE"), "%")},
        {"Métrica": "Disponibilidad (%)", "Valor": fmt_num(row.get("Availability"), "%")},
        {"Métrica": "Desempeño (%)", "Valor": fmt_num(row.get("Performance"), "%")},
        {"Métrica": "Producto Conforme (%)", "Valor": fmt_num(row.get("Producto Conforme"), "%")},
        {"Métrica": "Estatus Actual", "Valor": row.get("StatusCode") or "N/A"},
        {"Métrica": "Snapshot (Local)", "Valor": row.get("SnapshotAtLocal") or "N/A"},
        {"Métrica": "Línea", "Valor": row.get("LineName") or "N/A"},
        {"Métrica": "Producción Real (Kg)", "Valor": fmt_num(row.get("CurrentShiftProduction"))},
        {"Métrica": "Producción Esperada (Kg)", "Valor": fmt_num(row.get("ExpectedShiftProduction"))},
        {"Métrica": "Velocidad Real (Kg/h)", "Valor": fmt_num(row.get("CurrentRate"))},
        {"Métrica": "Velocidad Esperada (Kg/h)", "Valor": fmt_num(row.get("ExpectedRate"))},
        {"Métrica": "Paros No Programados (Eventos)", "Valor": row.get("ParosNoProgramadosCont") or 0},
        {"Métrica": "Duración Paros No Prog.", "Valor": row.get("UnscheduledStopageMin") or "0 minutos"},
        {"Métrica": "Paros Programados (Eventos)", "Valor": row.get("ParosProgramadosCont") or 0},
        {"Métrica": "Duración Paros Prog.", "Valor": row.get("ScheduledStopageMin") or "0 minutos"},
    ]

    # Gráficas PNG (Backend)
    image_paths = []
    try:
        from main import _sql_oee_realtime, run_sql
        rows_raw, cols_raw = run_sql(_sql_oee_realtime())
        if rows_raw:
            raw_snap = dict(zip(cols_raw, rows_raw[0]))
            image_paths = _generate_oee_rt_pngs(raw_snap)
    except Exception as e:
        print(f"Error generando PNGs para tiempo real: {e}")

    # IA (Texto)
    ai_text = provided_ai if provided_ai is not None else ""
    if provided_ai is None:
        try:
            ai_text = ai_oee_realtime(row)
        except Exception:
            ai_text = ""

    title = "Reporte Ejecutivo — OEE Tiempo Real"
    subtitle = f"Snapshot extraído a las {row.get('SnapshotAtLocal') or 'N/A'}"

    sections = [
        {"title": "Resumen Operativo", "text": "Estado actual de la línea de producción basado en el último snapshot de telemetría."}
    ]
    
    if image_paths:
        sections.append({
            "title": "Análisis Visual de Desempeño",
            "text": "Comparativa de eficiencia, producción y velocidad del turno actual:",
            "images": image_paths
        })

    if ai_text:
        sections.append({"title": "Análisis y Recomendaciones (IA)", "text": ai_text})

    content = _build_pdf_bytes(title, subtitle, sections, "Indicadores", table, logo_path=_LOGO_PATH)
    filename = "reporte_oee_realtime.pdf"
    return Response(content=content, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

from fastapi import Response, HTTPException
import re

@app.post("/api/report/oee/day/")
async def report_oee_day(payload: dict):
    """Descarga el análisis (PDF/Word) para OEE por día/turno."""
    day = normalize_day_str(payload.get("day") or "")
    shift_name = payload.get("shift_name")
    fmt = (payload.get("format") or "pdf").lower()
    provided_rows = payload.get("rows")
    provided_cols = payload.get("columns")
    provided_ai = payload.get("ai_analysis")

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", day):
        raise HTTPException(status_code=400, detail="Formato de 'day' inválido. Usa YYYY-MM-DD.")

    if provided_rows and provided_cols:
        rows = provided_rows
        cols = provided_cols
    else:
        # El front usa shift_name (ver index.html), así que mantenemos ese nombre.
        api_payload = {"day": day}
        if shift_name and str(shift_name).strip() and shift_name not in ("(Todos)", "(todos)", "todos", "(all)", "(All)"):
            api_payload["shift_name"] = shift_name

        try:
            data = await api_oee_day_turn(api_payload)
            cols = data.get("columns") or []
            rows = data.get("rows") or []
            if not provided_ai:
                provided_ai = data.get("ai_analysis")
        except HTTPException as he:
            raise he
        except Exception as e:
            print(f"Error reporte OEE Day: {e}")
            raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

    if not cols or not rows:
        raise HTTPException(status_code=404, detail="No hay datos para esa fecha/turno.")

    # rows viene como lista de listas => lo convertimos a lista de dicts
    table_rows = []
    for r in rows:
        # r puede venir como tuple o list
        r_list = list(r) if isinstance(r, (tuple, list)) else [r]
        row_dict = {c: (r_list[i] if i < len(r_list) else "") for i, c in enumerate(cols)}
        table_rows.append(row_dict)

    title = "Reporte — OEE por día/turno"
    subtitle = f"Fecha: {day}" + (f" — Turno: {shift_name}" if shift_name else "")

    # El análisis ya viene en data["ai_analysis"] (markdown) desde /api/oee/day-turn
    ai_text = provided_ai or ""

    # SECCIÓN: Gráficas de Desempeño
    # Obtenemos las imágenes (PNG) regenerándolas si es necesario
    # Para asegurar que tenemos las rutas de los PNG locales, llamamos a api_oee_day_turn si no tenemos los plots
    plots_meta = []
    try:
        # Llamamos de nuevo pero sin esperar que el front nos dé los plots
        # (Así nos aseguramos de que los PNG existen en disco en este momento)
        oee_data = await api_oee_day_turn({"day": day, "shift_name": shift_name})
        plots_meta = oee_data.get("plots") or []
    except Exception: pass

    sections = [
        {"title": "Resumen", "text": "Indicadores calculados por turno para la fecha seleccionada."}
    ]
    
    if plots_meta:
        sections.append({
            "title": "Gráficas de Desempeño",
            "text": "Comparativa visual de eficiencia, producción, velocidad y paros.",
            "images": [p["path"] for p in plots_meta if p.get("path")]
        })

    if ai_text.strip():
        sections.append({"title": "Análisis y recomendaciones (IA)", "text": ai_text})

    fmt = (fmt or "pdf").lower()
    if fmt in ("docx", "word"):
        content = _build_docx_bytes(title, subtitle, sections, "Resultado", table_rows, logo_path=_LOGO_PATH)
        return _as_file_response(
            content,
            _report_filename(f"oee_day_{day}" + (f"_{shift_name}" if shift_name else ""), "docx"),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

    content = _build_pdf_bytes(title, subtitle, sections, "Resultado", table_rows, logo_path=_LOGO_PATH)
    return _as_file_response(
        content,
        _report_filename(f"oee_day_{day}" + (f"_{shift_name}" if shift_name else ""), "pdf"),
        "application/pdf",
    )





# Para correr local:
# uvicorn main:app --host 0.0.0.0 --port 8000 --env-file .env

