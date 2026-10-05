# -*- coding: utf-8 -*-
"""El PDF del modulo Usabilidad, dibujado a mano.

POR QUE A MANO Y NO UNA CAPTURA DEL TABLERO. Era lo primero que se intento:
fotografiar la pantalla con html2canvas y pegarla. No sale. Medido panel por
panel, el contenido se captura bien por separado pero cualquier `.card` completa
devuelve CERO pixeles -lleva `backdrop-filter`, que esa libreria no soporta, y
apagarlo tampoco basta-, y la exportacion propia de Chart.js devuelve el lienzo
en blanco. Pero aunque hubiera salido, un PDF hecho de capturas es texto que no
se puede seleccionar, borroso al imprimir, y se rompe en silencio el dia que
alguien toque un estilo: el reporte saldria en blanco y nadie se enteraria hasta
abrirlo.

Dibujado aqui queda vectorial, el texto se copia, imprime nitido y no depende de
que un navegador logre retratarse a si mismo.

LOS COLORES LOS MANDA LA PANTALLA, no se eligen aqui. Si este archivo tuviera su
propia paleta, el dia que cambiara la del tablero el papel diria otra cosa.
"""
import io

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import Flowable, SimpleDocTemplate, Spacer

# Lo unico que se define aqui es lo que no depende de los datos.
C_MARCA = colors.HexColor("#0f766e")
C_MARCA2 = colors.HexColor("#1abc9c")
C_TEXTO = colors.HexColor("#0f172a")
C_SUAVE = colors.HexColor("#64748b")
C_LINEA = colors.HexColor("#e2e8f0")
C_FONDO = colors.HexColor("#f8fafc")
C_GRIS = colors.HexColor("#94a3b8")

MARGEN = 36
ANCHO_PAG, ALTO_PAG = letter
ANCHO = ANCHO_PAG - 2 * MARGEN
ALTO_CAB = 76


def _c(valor, porDefecto=C_GRIS):
    """Un color que viene del navegador (#rrggbb) como color de ReportLab."""
    try:
        s = str(valor or "").strip()
        if s.startswith("#") and len(s) in (4, 7):
            return colors.HexColor(s)
    except Exception:
        pass
    return porDefecto


def _texto(c, x, y, s, fuente="Helvetica", tam=9, color=C_TEXTO, al="i"):
    c.setFont(fuente, tam)
    c.setFillColor(color)
    s = str(s)
    if al == "c":
        c.drawCentredString(x, y, s)
    elif al == "d":
        c.drawRightString(x, y, s)
    else:
        c.drawString(x, y, s)


def _recorta(s, fuente, tam, ancho):
    """Corta con puntos suspensivos lo que no quepa, en vez de dejar que se monte
    encima de lo de al lado."""
    s = str(s)
    if stringWidth(s, fuente, tam) <= ancho:
        return s
    while s and stringWidth(s + "…", fuente, tam) > ancho:
        s = s[:-1]
    return s + "…"


def _caja(c, x, y, w, h, relleno=None, borde=None, r=10, grosor=0.7):
    if relleno is not None:
        c.setFillColor(relleno)
    if borde is not None:
        c.setStrokeColor(borde)
        c.setLineWidth(grosor)
    c.roundRect(x, y, w, h, r,
                stroke=1 if borde is not None else 0,
                fill=1 if relleno is not None else 0)


class Bloque(Flowable):
    """Un trozo de alto conocido que se pinta con el canvas. Mas simple que
    pelearse con tablas anidadas para cosas que son dibujo, no texto."""

    def __init__(self, alto, pintar):
        Flowable.__init__(self)
        self.alto = alto
        self.pintar = pintar

    def wrap(self, aw, ah):
        self._aw = aw
        return aw, self.alto

    def draw(self):
        self.pintar(self.canv, self._aw, self.alto)


# ---------------------------------------------------------------------------
# Los bloques, en el mismo orden que el tablero
# ---------------------------------------------------------------------------

def veredicto(v):
    titulo = str(v.get("titulo") or "")
    detalle = str(v.get("detalle") or "")
    tono = _c(v.get("tono"), C_MARCA2)
    alto = 58

    def pintar(c, w, h):
        _caja(c, 0, 0, w, h, relleno=C_FONDO, borde=C_LINEA, r=12)
        c.setFillColor(tono)
        c.circle(26, h / 2, 11, stroke=0, fill=1)
        _texto(c, 52, h / 2 + 4, _recorta(titulo, "Helvetica-Bold", 13, w - 70),
               "Helvetica-Bold", 13, C_TEXTO)
        _texto(c, 52, h / 2 - 12, _recorta(detalle, "Helvetica", 9, w - 70),
               "Helvetica", 9, C_SUAVE)

    return Bloque(alto, pintar)


def tarjetas(lista):
    lista = lista or []
    if not lista:
        return None
    alto = 80

    def pintar(c, w, h):
        n = len(lista)
        hueco = 11
        aw = (w - hueco * (n - 1)) / float(n)
        for i, t in enumerate(lista):
            x = i * (aw + hueco)
            _caja(c, x, 0, aw, h, relleno=colors.white, borde=C_LINEA, r=11)
            # La barrita de color del tablero, a la izquierda.
            c.setFillColor(_c(t.get("color")))
            c.roundRect(x + 1.5, 7, 3.5, h - 14, 1.75, stroke=0, fill=1)
            _texto(c, x + 14, h - 20,
                   _recorta(str(t.get("label") or "").upper(),
                            "Helvetica-Bold", 7.5, aw - 24),
                   "Helvetica-Bold", 7.5, C_SUAVE)
            _texto(c, x + 14, h - 48, str(t.get("value") or ""),
                   "Helvetica-Bold", 25, C_TEXTO)
            _texto(c, x + 14, 13,
                   _recorta(str(t.get("status") or ""), "Helvetica", 8, aw - 24),
                   "Helvetica", 8, C_SUAVE)

    return Bloque(alto, pintar)


def cabecera_panel(c, w, h, titulo, sub):
    """La cabecera comun de cada panel: titulo y subtitulo."""
    _texto(c, 0, h - 13, _recorta(titulo, "Helvetica-Bold", 12, w),
           "Helvetica-Bold", 12, C_MARCA)
    if sub:
        _texto(c, 0, h - 26, _recorta(sub, "Helvetica", 8.5, w),
               "Helvetica", 8.5, C_SUAVE)
    return h - 38


def gente(filas, titulo, sub, unidad, unidad1, unidad_anon, primera=True):
    filas = filas or []
    fila_h = 38
    cab = 38 if primera else 6
    alto = cab + len(filas) * fila_h

    def pintar(c, w, h):
        y = h
        if primera:
            y = cabecera_panel(c, w, h, titulo, sub)
        else:
            y = h - 6
        for f in filas:
            y -= fila_h
            _caja(c, 0, y + 4, w, fila_h - 6, relleno=colors.white,
                  borde=C_LINEA, r=9)
            cy = y + 4 + (fila_h - 6) / 2.0
            c.setFillColor(_c(f.get("color")))
            c.circle(21, cy, 12, stroke=0, fill=1)
            ini = str(f.get("iniciales") or "")
            if ini:
                _texto(c, 21, cy - 3.2, ini, "Helvetica-Bold", 9,
                       colors.white, al="c")
            # Numero y unidad, a la derecha
            val = f.get("veces")
            val = "" if val == "" else str(val)
            anon = (f.get("veces") == "")
            uni = unidad_anon if anon else (unidad1 if str(val) == "1" else unidad)
            if anon:
                val = str(f.get("accesos") or "")
            ancho_uni = stringWidth(uni, "Helvetica", 7)
            _texto(c, w - 12, cy - 1, val, "Helvetica-Bold", 14, C_TEXTO, al="d")
            _texto(c, w - 12, cy - 12, uni, "Helvetica", 7, C_SUAVE, al="d")
            libre = w - 40 - max(ancho_uni, 40) - 26
            _texto(c, 40, cy + 2,
                   _recorta(f.get("nombre"), "Helvetica-Bold", 9.5, libre),
                   "Helvetica-Bold", 9.5, C_TEXTO)
            _texto(c, 40, cy - 9,
                   _recorta("en %s %s" % (f.get("dias") or 1,
                                          "día" if (f.get("dias") or 1) == 1 else "días"),
                            "Helvetica", 8, libre),
                   "Helvetica", 8, C_SUAVE)

    return Bloque(alto, pintar)


def dona(partes, total, etiqueta, titulo, sub):
    """Las visitas repartidas entre las personas. Se dibuja con cunas y un
    circulo blanco encima: mas corto que armar un grafico de la libreria y con
    control exacto de los colores, que vienen de la pantalla."""
    partes = [p for p in (partes or []) if (p.get("valor") or 0) > 0]
    # El alto depende de cuanta leyenda haya: con una altura fija, siete personas
    # desbordaban el bloque y la ultima linea se montaba sobre el panel de abajo.
    filas_leyenda = (len(partes) + 1) // 2
    alto = 38 + 92 + 74 + 18 + filas_leyenda * 14 + 10

    def pintar(c, w, h):
        y0 = cabecera_panel(c, w, h, titulo, sub)
        if not partes:
            return
        cx, cy, R, r = w / 2.0, y0 - 92, 74, 46
        suma = float(sum(p.get("valor") or 0 for p in partes)) or 1.0
        ang = 90.0
        for p in partes:
            ext = -360.0 * (p.get("valor") or 0) / suma
            c.setFillColor(_c(p.get("color")))
            c.setStrokeColor(colors.white)
            c.setLineWidth(1)
            c.wedge(cx - R, cy - R, cx + R, cy + R, ang, ext, stroke=1, fill=1)
            ang += ext
        c.setFillColor(colors.white)
        c.circle(cx, cy, r, stroke=0, fill=1)
        _texto(c, cx, cy + 2, str(total), "Helvetica-Bold", 21, C_TEXTO, al="c")
        _texto(c, cx, cy - 13, etiqueta, "Helvetica", 8, C_SUAVE, al="c")

        # El numero dentro de cada cuna, como en el tablero.
        import math
        ang = 90.0
        rm = (R + r) / 2.0
        for p in partes:
            ext = -360.0 * (p.get("valor") or 0) / suma
            if abs(ext) >= 16:
                a = math.radians(ang + ext / 2.0)
                _texto(c, cx + rm * math.cos(a), cy + rm * math.sin(a) - 3,
                       str(p.get("valor")), "Helvetica-Bold", 8.5,
                       colors.white, al="c")
            ang += ext

        # Leyenda en dos columnas
        ly = cy - R - 16
        col = 0
        for p in partes:
            x = 6 + col * (w / 2.0)
            c.setFillColor(_c(p.get("color")))
            c.roundRect(x, ly - 6, 8, 8, 2, stroke=0, fill=1)
            _texto(c, x + 13, ly - 5.5,
                   _recorta(p.get("nombre"), "Helvetica", 8, w / 2.0 - 30),
                   "Helvetica", 8, C_TEXTO)
            col += 1
            if col == 2:
                col = 0
                ly -= 14

    return Bloque(alto, pintar)


def horas(valores, turnos, nota, titulo, sub, etiqueta_visitas):
    valores = (valores or [0] * 24)[:24]
    turnos = turnos or []
    alto = 150 + (78 if turnos else 0) + (16 if nota else 0)

    def pintar(c, w, h):
        y0 = cabecera_panel(c, w, h, titulo, sub)
        base = y0 - 108
        tope = max(valores) or 1
        hmax = 86
        bw = (w - 24) / 24.0

        c.setStrokeColor(C_LINEA)
        c.setLineWidth(0.5)
        c.line(0, base, w, base)

        for i, v in enumerate(valores):
            x = 12 + i * bw
            if v:
                # El color de la hora es el de su turno, igual que en pantalla.
                col = C_GRIS
                for t in turnos:
                    d, ha = t.get("desde"), t.get("hasta")
                    if d is None:
                        continue
                    dentro = (d <= i < ha) if d < ha else (i >= d or i < ha)
                    if dentro:
                        col = _c(t.get("color"))
                        break
                bh = hmax * (float(v) / tope)
                c.setFillColor(col)
                c.roundRect(x + bw * 0.16, base, bw * 0.68, bh, 2, stroke=0, fill=1)
                _texto(c, x + bw / 2.0, base + bh + 4, str(v),
                       "Helvetica-Bold", 7.5, C_TEXTO, al="c")
            _texto(c, x + bw / 2.0, base - 11, "%02d" % i,
                   "Helvetica", 6.5, C_SUAVE, al="c")

        if turnos:
            ty = base - 24
            hueco = 11
            aw = (w - hueco * (len(turnos) - 1)) / float(len(turnos))
            for i, t in enumerate(turnos):
                x = i * (aw + hueco)
                _caja(c, x, ty - 54, aw, 54, relleno=colors.white,
                      borde=C_LINEA, r=9)
                _texto(c, x + 12, ty - 18,
                       _recorta(t.get("etiqueta"), "Helvetica-Bold", 8.5, aw - 24),
                       "Helvetica-Bold", 8.5, C_TEXTO)
                pct = t.get("pct")
                grande = ("%d%%" % pct) if pct is not None else str(t.get("sesiones") or 0)
                _texto(c, x + 12, ty - 40, grande, "Helvetica-Bold", 17,
                       _c(t.get("color")) if pct is not None else C_SUAVE)
                _texto(c, x + 12, ty - 50,
                       "%s %s" % (t.get("sesiones") or 0, etiqueta_visitas),
                       "Helvetica", 7.5, C_SUAVE)
            if nota:
                _texto(c, 0, ty - 68, _recorta(nota, "Helvetica", 7.5, w),
                       "Helvetica", 7.5, C_SUAVE)

    return Bloque(alto, pintar)


def areas(lista, titulo, sub, unidad, unidad1):
    lista = lista or []
    por_fila = 6 if len(lista) > 4 else max(len(lista), 1)
    filas = (len(lista) + por_fila - 1) // por_fila
    alto = 38 + filas * 58

    def pintar(c, w, h):
        y0 = cabecera_panel(c, w, h, titulo, sub)
        hueco = 9
        aw = (w - hueco * (por_fila - 1)) / float(por_fila)
        for i, a in enumerate(lista):
            fx = (i % por_fila) * (aw + hueco)
            fy = y0 - 52 - (i // por_fila) * 58
            _caja(c, fx, fy, aw, 50, relleno=colors.white, borde=C_LINEA, r=9)
            _texto(c, fx + 9, fy + 34,
                   _recorta(a.get("area"), "Helvetica-Bold", 7.5, aw - 18),
                   "Helvetica-Bold", 7.5, C_SUAVE)
            _texto(c, fx + 9, fy + 14, "%d%%" % (a.get("pct") or 0),
                   "Helvetica-Bold", 16, C_TEXTO)
            u = a.get("usos") or 0
            _texto(c, fx + 9, fy + 5, "%s %s" % (u, unidad1 if u == 1 else unidad),
                   "Helvetica", 7, C_SUAVE)

    return Bloque(alto, pintar)


def pantallas(lista, tope, primera, titulo, sub):
    lista = lista or []
    fila_h = 26
    cab = 38 if primera else 4
    alto = cab + len(lista) * fila_h

    def pintar(c, w, h):
        y = cabecera_panel(c, w, h, titulo, sub) if primera else h - 4
        for p in lista:
            y -= fila_h
            nom = str(p.get("nombre") or "")
            fuente = "Helvetica" if p.get("nombrada") else "Courier"
            _texto(c, 0, y + 13, _recorta(nom, fuente, 9, w - 50), fuente, 9,
                   C_TEXTO if p.get("nombrada") else C_SUAVE)
            _texto(c, w, y + 13, str(p.get("usos") or 0), "Helvetica-Bold", 9,
                   C_TEXTO, al="d")
            c.setFillColor(C_LINEA)
            c.roundRect(0, y + 3, w, 5, 2.5, stroke=0, fill=1)
            frac = min(1.0, (p.get("usos") or 0) / float(tope or 1))
            c.setFillColor(C_MARCA2 if p.get("nombrada") else C_GRIS)
            c.roundRect(0, y + 3, max(6, w * frac), 5, 2.5, stroke=0, fill=1)

    return Bloque(alto, pintar)


def aviso(texto):
    alto = 30

    def pintar(c, w, h):
        _caja(c, 0, 0, w, h, relleno=colors.HexColor("#fef3c7"),
              borde=colors.HexColor("#fcd34d"), r=8)
        _texto(c, 12, h / 2 - 3, _recorta(texto, "Helvetica", 8, w - 24),
               "Helvetica", 8, colors.HexColor("#92400e"))

    return Bloque(alto, pintar)


# ---------------------------------------------------------------------------

def construir(vista, periodo, logo=None, etiquetas=None):
    """El documento entero, en el mismo orden que el tablero."""
    import os

    e = {
        "titulo": "Reporte — Uso de Sidón Industrial",
        "quien_tit": "Quién usó la plataforma",
        "quien_sub": "Cuántas veces entró cada persona en las fechas elegidas",
        "veces": "veces que entró", "vez": "vez que entró",
        "accesos": "accesos sin identificar",
        "dona_tit": "Quién entra más",
        "dona_sub": "Cómo se reparten las visitas entre las personas",
        "visitas": "visitas",
        "horas_tit": "A qué hora trabajan",
        "horas_sub": "Visitas que arrancan en cada hora del día",
        "pant_tit": "Qué pantallas usan",
        "pant_sub": "Las pantallas que la gente abre, de más a menos usada",
        "usos": "usos", "uso": "uso",
    }
    e.update(etiquetas or {})

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=MARGEN, rightMargin=MARGEN,
        topMargin=ALTO_CAB + 16, bottomMargin=46,
        title=e["titulo"], author="Duma Analytics")

    def pagina(c, d):
        c.saveState()
        c.setFillColor(C_MARCA)
        c.rect(0, ALTO_PAG - ALTO_CAB, ANCHO_PAG, ALTO_CAB, stroke=0, fill=1)
        tx = MARGEN
        if logo and os.path.exists(logo):
            try:
                from reportlab.lib.utils import ImageReader
                ir = ImageReader(logo)
                iw, ih = ir.getSize()
                sc = min(58.0 / iw, 50.0 / ih)
                c.drawImage(logo, MARGEN, ALTO_PAG - ALTO_CAB + 13,
                            width=iw * sc, height=ih * sc, mask="auto")
                tx = MARGEN + 58 + 12
                c.setStrokeColor(C_MARCA2)
                c.setLineWidth(0.8)
                c.line(tx - 8, ALTO_PAG - ALTO_CAB + 10, tx - 8, ALTO_PAG - 12)
            except Exception:
                pass
        _texto(c, tx, ALTO_PAG - 36, e["titulo"], "Helvetica-Bold", 15, colors.white)
        _texto(c, tx, ALTO_PAG - 54, "Periodo: " + periodo, "Helvetica", 9,
               colors.HexColor("#a7f3d0"))
        c.setStrokeColor(C_MARCA2)
        c.setLineWidth(0.7)
        c.line(MARGEN, 40, ANCHO_PAG - MARGEN, 40)
        from datetime import datetime
        _texto(c, MARGEN, 30,
               "Duma Analytics  |  Generado: " + datetime.now().strftime("%Y-%m-%d %H:%M"),
               "Helvetica", 7.5, C_SUAVE)
        _texto(c, ANCHO_PAG - MARGEN, 30, "Página %d" % d.page,
               "Helvetica", 7.5, C_SUAVE, al="d")
        c.restoreState()

    hist = []
    if vista.get("veredicto", {}).get("titulo"):
        hist.append(veredicto(vista["veredicto"]))
        hist.append(Spacer(1, 12))
    t = tarjetas(vista.get("tarjetas"))
    if t:
        hist.append(t)
        hist.append(Spacer(1, 18))

    # Quien uso la plataforma. Se parte en tandas para que un bloque no quede a
    # caballo entre dos paginas: un Flowable no se puede cortar por la mitad.
    g = list(vista.get("gente") or [])
    for i in range(0, len(g), 9):
        hist.append(gente(g[i:i + 9], e["quien_tit"], e["quien_sub"],
                          e["veces"], e["vez"], e["accesos"], primera=(i == 0)))
    if vista.get("gente_nota"):
        hist.append(Spacer(1, 8))
        hist.append(aviso(vista["gente_nota"]))
    hist.append(Spacer(1, 18))

    partes = [{"nombre": str(x.get("nombre") or "").split("@")[0],
               "valor": x.get("veces") or 0, "color": x.get("color")}
              for x in g if x.get("veces") not in ("", None)]
    total = sum(p["valor"] for p in partes)
    if total:
        hist.append(dona(partes, total, e["visitas"], e["dona_tit"], e["dona_sub"]))
        hist.append(Spacer(1, 14))

    hist.append(horas(vista.get("por_hora"), vista.get("turnos"),
                      vista.get("turnos_nota"), e["horas_tit"], e["horas_sub"],
                      e["visitas"]))
    hist.append(Spacer(1, 14))

    if vista.get("areas"):
        hist.append(areas(vista["areas"], e["pant_tit"], e["pant_sub"],
                          e["usos"], e["uso"]))
        hist.append(Spacer(1, 6))
    pant = list(vista.get("pantallas") or [])
    tope = max([p.get("usos") or 0 for p in pant] or [1])
    for i in range(0, len(pant), 12):
        hist.append(pantallas(pant[i:i + 12], tope, primera=(i == 0 and not vista.get("areas")),
                              titulo=e["pant_tit"], sub=e["pant_sub"]))

    if vista.get("al_dia"):
        hist.append(Spacer(1, 12))
        hist.append(aviso(vista["al_dia"]))

    doc.build(hist, onFirstPage=pagina, onLaterPages=pagina)
    return buf.getvalue()
