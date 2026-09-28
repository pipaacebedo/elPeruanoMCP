"""Servidor MCP para busquedas en El Peruano https://busquedas.elperuano.pe

Regla de diseno: las tools NUNCA escriben archivos ni guardan datos.
Solo leen el HTML del sitio y devuelven metadatos + URLs publicas de los
PDFs (/api/archivo/file/<hash>/*/<nombre>.pdf). El usuario/cliente es
quien descarga los archivos.
"""
from __future__ import annotations

import asyncio
import html as htmllib
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

import httpx
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

try:
    import pypdf

    _PYPDF_OK = True
except Exception:
    _PYPDF_OK = False
BASE_URL = os.getenv("EPERUANO_BASE_URL", "https://busquedas.elperuano.pe").rstrip("/")
TIMEOUT = float(os.getenv("EPERUANO_TIMEOUT", "60"))
CACHE_TTL = float(os.getenv("EPERUANO_CACHE_TTL", "300"))
CACHE_BYTES = int(os.getenv("EPERUANO_CACHE_BYTES", str(32 * 1024 * 1024)))
CACHE_MAX_ENTRY = int(os.getenv("EPERUANO_CACHE_MAX_ENTRY", str(3 * 1024 * 1024)))
MAX_PAGINAS = int(os.getenv("EPERUANO_MAX_PAGINAS", "20"))
MAX_CONCURRENCY = int(os.getenv("EPERUANO_MAX_CONCURRENCY", "3"))
INFLIGHT_CAP = int(os.getenv("EPERUANO_INFLIGHT_CAP", "40"))
REPO_URL = os.getenv("EPERUANO_REPO_URL", "https://github.com/usuario/elPeruanoMCP").rstrip("/")
PAGE_SIZE = 20

TIPOS_PUBLICACION = {
    "NL": "Normas Legales",
    "BO": "Boletin Oficial",
    "EX": "Edicion Extraordinaria",
    "PC": "Procesos Constitucionales",
    "DJ": "Declaracion Jurada",
    "JU": "Jurisprudencia",
    "SE": "Separata Especial",
    "CA": "Sentencia en Casacion",
    "IN": "Indice Quincenal",
    "TU": "T.U.P.A.",
}
SIN_INDIVIDUAL = {"PC", "DJ", "JU", "CA", "IN", "TU"}
OP_RE = re.compile(r"^\d{7}-\d$")
CUADERNILLO_RE = re.compile(r"^(NL|BO|EX|PC|DJ|JU|SE|CA|IN|TU)(\d{8})$")
_INSTRUCCIONES = (
    "Diario Oficial El Peruano (busquedas.elperuano.pe).\n"
    "\n"
    "buscar_dispositivos: texto libre (frases exactas entre comillas\n"
    "dobles), cuadernillo (NL, BO, EX, PC, DJ, JU, SE, CA, IN, TU), fecha\n"
    "puntual o rango, sector como post-filtro de la pagina obtenida.\n"
    "coincidencias_texto cuenta coincidencias de texto del sitio (no\n"
    "dispositivos unicos); dispositivos_unicos los individuales devueltos.\n"
    "El sitio no garantiza un AND estricto entre palabras.\n"
    "\n"
    "Si trae cuadernillos_con_coincidencias en vez de data, las\n"
    "coincidencias estan en cuadernillos sin pagina individual\n"
    "(PC/DJ/JU/CA/IN/TU) y a veces NL/BO a nivel de cuadernillo: el texto\n"
    "integral esta en el PDF (descargar_cuadernillo / "
    "obtener_cuadernillo_texto).\n"
    "\n"
    "Para documentos largos usa obtener_dispositivo_texto por rangos de\n"
    "parrafos. descargar_pdf y descargar_cuadernillo devuelven la URL\n"
    "publica del PDF (no lo almacenan ni descargan); en instancias con\n"
    "pypdf, obtener_cuadernillo_texto lo lee por paginas.\n"
    "\n"
    "Las busquedas sin cuadernillo pueden tardar 10-40 s; con cuadernillo\n"
    "son mas rapidas. Consultas identicas repetidas responden por cache."
)

mcp = FastMCP(
    "ElPeruanoMCP",
    instructions=_INSTRUCCIONES,
)

_CLIENT: Optional[httpx.AsyncClient] = None
_CACHE: dict[str, tuple[float, str]] = {}
_CACHE_BYTES = 0
_SEM: Optional[asyncio.Semaphore] = None
_INFLIGHT: dict[str, asyncio.Task] = {}
_INFLIGHT_UPSTREAM = 0
_SEARCH_CACHE: dict[str, tuple[float, dict]] = {}
_SEARCH_CACHE_MAX = 64

def _sem() -> asyncio.Semaphore:
    global _SEM
    if _SEM is None:
        _SEM = asyncio.Semaphore(max(1, MAX_CONCURRENCY))
    return _SEM

def _client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/152.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
            },
        )
    return _CLIENT

async def _get_search_page(client: httpx.AsyncClient, params: Optional[dict]) -> str:
    """Descarga la pagina de busqueda con streaming y CORTE en </main>.

    El SSR del sitio incrusta el texto completo de las coincidencias fuera
    de <main> (paginas de hasta ~190 MB); todo lo que parseamos vive en
    <main> (~40-140 KB). Al cortar: memoria, ancho de banda y latencia
    bajan 3-4 ordenes de magnitud. El <head> (og:image) queda incluido.
    """
    buf = bytearray()
    async with client.stream("GET", "/", params=params) as r:
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}")
        async for chunk in r.aiter_bytes(65536):
            buf.extend(chunk)
            if b"</main>" in buf:
                break
            if len(buf) > 2 * 1024 * 1024:
                break
    return buf.decode("utf-8", "replace")

async def _do_fetch(path: str, params: Optional[dict]) -> str:
    """GET con reintentos (backoff inmediato, 2s, 5s) y semaforo upstream.
    Para la ruta de busqueda usa streaming con corte en </main>."""
    client = _client()
    last_err: str = ""
    delays = (0.0, 2.0, 5.0)
    for wait in delays:
        if wait:
            await asyncio.sleep(wait)
        try:
            async with _sem():
                if path == "/":
                    return await _get_search_page(client, params)
                r = await client.get(path, params=params)
        except httpx.TransportError as e:
            last_err = f"red: {e}"
            continue
        except RuntimeError as e:
            m = re.search(r"HTTP (\d{3})", str(e))
            code = int(m.group(1)) if m else 0
            if code >= 500 or (path == "/" and code == 404):
                last_err = str(e)
                continue
            raise
        if r.status_code >= 500:
            last_err = f"HTTP {r.status_code}"
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}")
        return r.text
    raise RuntimeError(f"Tras 3 intentos: {last_err}")

def _cache_put(key: str, body: str) -> None:
    """Inserta en cache con presupuesto por bytes y evict FIFO."""
    global _CACHE_BYTES
    if CACHE_TTL <= 0:
        return
    size = len(body)
    if size > CACHE_MAX_ENTRY:
        return
    while _CACHE and _CACHE_BYTES + size > CACHE_BYTES:
        viejo = next(iter(_CACHE))
        _, viejo_body = _CACHE.pop(viejo)
        _CACHE_BYTES -= len(viejo_body)
    if _CACHE_BYTES + size > CACHE_BYTES:
        return
    _CACHE[key] = (time.time(), body)
    _CACHE_BYTES += size

def _cache_get(key: str) -> Optional[str]:
    hit = _CACHE.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    if hit:
        _CACHE.pop(key, None)
    return None

async def _fetch(path: str, params: Optional[dict] = None) -> str:
    """GET con cache TTL (presupuesto por bytes), single-flight para fetches
    identicos en vuelo, tope de trabajo upstream y reintentos ante 5xx/red."""
    global _INFLIGHT_UPSTREAM
    key = path + ("?" + "&".join(f"{k}={v}" for k, v in sorted((params or {}).items())) if params else "")
    cached = _cache_get(key)
    if cached is not None:
        return cached
    tarea = _INFLIGHT.get(key)
    if tarea is not None:
        return await asyncio.shield(tarea)
    if _INFLIGHT_UPSTREAM >= max(1, INFLIGHT_CAP):
        raise RuntimeError(
            "429: servidor ocupado, reintenta en 30 segundos (demasiadas consultas en vuelo)"
        )
    _INFLIGHT_UPSTREAM += 1
    t0 = time.time()
    tarea = asyncio.ensure_future(_do_fetch(path, params))
    _INFLIGHT[key] = tarea
    try:
        body = await tarea
    finally:
        _INFLIGHT.pop(key, None)
        _INFLIGHT_UPSTREAM -= 1
    _cache_put(key, body)
    return body

async def _head_bytes(url: str) -> dict:
    """Probe del PDF sin bajar el cuerpo (aborta la conexion al leer
    headers). 'bytes' solo aparece si el sitio entrega content-length.

    Devuelve {"status": int, "bytes": int|None}.
    """
    st, ln = 0, None
    try:
        async with _sem():
            async with _client().stream("GET", url, headers={"Range": "bytes=0-0"}) as r:
                st = r.status_code
                if st == 206:
                    m = re.search(r"/(\d+)$", r.headers.get("content-range", ""))
                    ln = int(m.group(1)) if m else None
                elif st == 200:
                    cl = r.headers.get("content-length")
                    ln = int(cl) if cl else None
    except httpx.TransportError:
        pass
    except ValueError:
        pass
    return {"status": st, "bytes": ln}

def _clean(t: str) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", t or ""))).strip()

def _norm_fecha(raw: Optional[str]) -> Optional[str]:
    """Normaliza a YYYYMMDD: acepta YYYYMMDD, AAAA-MM-DD, DD/MM/AAAA.
    Devuelve None si el formato o la fecha son inválidos."""
    from datetime import date

    if not raw:
        return None
    raw = raw.strip()
    if re.match(r"^\d{8}$", raw):
        y, mo, d = int(raw[:4]), int(raw[4:6]), int(raw[6:8])
    else:
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        else:
            m = re.match(r"^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})$", raw)
            if not m:
                return None
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        date(y, mo, d)
    except ValueError:
        return None
    return f"{y:04d}{mo:02d}{d:02d}"

def _pdf_from_thumb(thumb_url: str) -> Optional[str]:
    """De thumbnail/<hash>/*/<base>_thumb.jpg -> file/<hash>/*/<base>.pdf.

    <base> es la OP (ej 2558398-1) o el cuadernillo (ej NL20260927; con
    sufijo de primera pagina: NL20260927_4436, que se recorta).
    """
    path = thumb_url[len(BASE_URL):] if thumb_url.startswith(BASE_URL) else thumb_url
    mm = re.match(r"(.*?)/thumbnail/([A-Za-z0-9_-]+)/\*/([^/]+?)_thumb\.jpg$", path)
    if not mm:
        return None
    prefijo, h, base = mm.group(1), mm.group(2), mm.group(3)
    mc = re.match(r"^([A-Z]{2}\d{8})_[\w-]{1,6}$", base)
    if mc:
        base = mc.group(1)
    return f"{BASE_URL}{prefijo}/file/{h}/*/{base}.pdf"

_RE_TOTAL = re.compile(r"(\d+)\s*(?:<!--\s*-->\s*){1,2}[a-zA-Z ]*?encontrad[oa]s?")
_CARD_SPLIT_RE = re.compile(
    r'(?s)<div class="rounded-xl border bg-card text-card-foreground shadow flex h-full flex-col'
)
_RE_CARD = re.compile(
    r'(?s)<p class="text-sm font-semibold text-primary">(?P<sector>[^<]*)</p>'
    r'.{0,800}?href="/dispositivo/(?P<tipo>[A-Z]{2})/(?P<op>\d{7}-\d+)"'
    r'.{0,600}?<p class="text-xs text-muted-foreground">(?P<tipoDisp>[^<]*)</p>'
    r'(?:<p class="text-xs font-medium text-muted-foreground">(?P<numero>[^<]*)</p>)?'
    r'.{0,2500}?<span>(?P<span1>[^<]*)</span>\s*<span>(?P<span2>[^<]*)</span>'
)
_RE_SNIPPET = re.compile(r'(?s)<a class="[^"]*(?:line-clamp-2|line-clamp-3)[^"]*"[^>]*>(.*?)</a>')
_RE_LINK_SIMPLE = re.compile(r'href="/dispositivo/([A-Z]{2})/(\d+-\d+)"')
_CUA_CARD = re.compile(
    r'(?s)<(?:p|div) class="text-sm font-bold[^"]*">(?P<title>[^<]+)</(?:p|div)>'
    r'.{0,1200}?archivo/thumbnail/(?P<hash>[A-Za-z0-9_-]+)/\*/[A-Z]{2}\d{8}[\w-]*_thumb\.jpg'
)
_CUA_SPANS = re.compile(
    r'(?s)<span>(?P<pag>\d+)\s*p[aá]ginas?</span>\s*<span>(?P<fecha>[^<]*)</span>'
)
_THUMB_RE = re.compile(
    r'archivo/thumbnail/([A-Za-z0-9_-]+)/\*/([A-Z]{2})(\d{8})[A-Za-z0-9_-]*_thumb\.jpg'
)
_RE_PDF_FILE = re.compile(r'archivo/file/([A-Za-z0-9_-]+)/\*/([A-Za-z0-9_.-]+)\.pdf')
_RE_OG_URL = re.compile(r'og:url" content="([^"]+)"')
_RE_OG_IMAGE = re.compile(r'og:image" content="([^"]+)"')

def _parse_total(body: str) -> Optional[int]:
    m = _RE_TOTAL.search(body)
    return int(m.group(1)) if m else None

def _parse_cards(body: str) -> list[dict]:
    """Tarjetas de dispositivo (cuadernillos con pagina individual)."""
    out = []
    for chunk in _CARD_SPLIT_RE.split(body)[1:]:
        m = _RE_CARD.search(chunk)
        if not m:
            continue
        lnk = _RE_LINK_SIMPLE.search(chunk)
        if not lnk:
            continue
        sn = _RE_SNIPPET.search(chunk)
        out.append(
            {
                "sector": _clean(m.group("sector")),
                "cuadernillo": lnk.group(1),
                "op": lnk.group(2),
                "tipo_dispositivo": _clean(m.group("tipoDisp") or ""),
                "numero": _clean(m.group("numero") or ""),
                "fecha": _clean(m.group("span2") or m.group("span1") or ""),
                "titulo": _clean(sn.group(1)) if sn else "",
                "url": f"{BASE_URL}/dispositivo/{lnk.group(1)}/{lnk.group(2)}",
            }
        )
    return out

def _parse_cuadernillos_cards(body: str) -> list[dict]:
    """Tarjetas de cuadernillo: portada, N paginas, fecha y pdf_url.

    Presente en la portada del buscador, en listados por fecha y en los
    resultados (por texto) de cuadernillos SIN dispositivo individual.
    """
    out = []
    seen: set[str] = set()
    for chunk in _CARD_SPLIT_RE.split(body)[1:]:
        m = _CUA_CARD.search(chunk)
        if not m or "/dispositivo/" in chunk[:200]:
            continue
        th = _THUMB_RE.search(chunk)
        if not th:
            continue
        tipo, fecha = th.group(2), th.group(3)
        code = f"{tipo}{fecha}"
        if code in seen:
            continue
        seen.add(code)
        sp = _CUA_SPANS.search(chunk)
        out.append(
            {
                "codigo": code,
                "tipo": tipo,
                "titulo": _clean(m.group("title")),
                "fecha": _clean(sp.group("fecha")) if sp else "",
                "paginas": int(sp.group("pag")) if sp else None,
                "thumbnail_url": f"{BASE_URL}/api/archivo/thumbnail/{th.group(1)}/*/{code}_thumb.jpg",
                "url_viewer": f"{BASE_URL}/cuadernillo/{tipo}/{fecha}",
                "pdf_url": f"{BASE_URL}/api/archivo/file/{th.group(1)}/*/{code}.pdf",
            }
        )
    return out

def _parse_dispositivo(body: str) -> dict:
    d: dict = {}
    m = _RE_OG_URL.search(body)
    if m:
        mm = re.search(r"/dispositivo/([A-Z]{2})/(\d+-\d+)", m.group(1))
        if mm:
            d["cuadernillo"], d["op"] = mm.group(1), mm.group(2)
    m = re.search(r"(?s)<h1[^>]*>(.*?)</h1>", body)
    d["sumilla"] = _clean(m.group(1)) if m else ""
    subs = [_clean(x.group(1)) for x in re.finditer(r"<h2[^>]*>(.*?)</h2>", body)]
    d["encabezados_h2"] = subs
    d["tipo_dispositivo"] = subs[0] if subs else ""
    d["numero"] = subs[1] if len(subs) > 1 else ""
    paras = [
        _clean(p.group(1))
        for p in re.finditer(r'<p[^>]*class="[^"]*cuerpo[^"]*"[^>]*>(.*?)</p>', body)
    ]
    paras = [
        p.replace("D iario", "Diario").replace("O ficial", "Oficial")
        for p in paras
    ]
    d["texto"] = "\n".join(paras)
    d["parrafos_totales"] = len(paras)
    d["fecha_texto"] = ""
    for p in paras:
        mm = re.search(r"Lima,?\s+\d{1,2} de [a-z]+ de \d{4}", p, re.I)
        if mm:
            d["fecha_texto"] = _clean(mm.group(0))
            break
    th = _RE_OG_IMAGE.search(body)
    if th:
        thumb = th.group(1) if th.group(1).startswith("http") else BASE_URL + th.group(1)
        d["thumbnail_url"] = thumb
        d["pdf_url"] = _pdf_from_thumb(thumb)
    d["sin_texto_html"] = d["parrafos_totales"] == 0
    return d

def _params_busqueda(
    consulta: Optional[str],
    op: Optional[str],
    cuadernillo: Optional[str],
    fecha: Optional[str],
    fecha_ini: Optional[str],
    fecha_fin: Optional[str],
    start: int,
) -> dict[str, str]:
    p: dict[str, str] = {"start": str(max(0, start))}
    if op and op.strip():
        p["op"] = op.strip()
    elif consulta and consulta.strip():
        p["query"] = consulta.strip()
    if cuadernillo:
        p["tipoPublicacion"] = cuadernillo
        if cuadernillo not in SIN_INDIVIDUAL:
            p["ci"] = "ONLY"
    if fecha:
        p["fecha"] = fecha
    if fecha_ini:
        p["fechaIni"] = fecha_ini
    if fecha_fin:
        p["fechaFin"] = fecha_fin
    return p

def _sres_key(params: dict, sector: Optional[str]) -> str:
    base = "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    return "S|" + base + "|sector=" + (sector or "")

def _sres_get(key: str) -> Optional[dict]:
    if CACHE_TTL <= 0:
        return None
    hit = _SEARCH_CACHE.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    if hit:
        _SEARCH_CACHE.pop(key, None)
    return None

def _sres_put(key: str, res: dict) -> None:
    if CACHE_TTL <= 0:
        return
    while len(_SEARCH_CACHE) >= _SEARCH_CACHE_MAX:
        _SEARCH_CACHE.pop(next(iter(_SEARCH_CACHE)))
    _SEARCH_CACHE[key] = (time.time(), res)

def _motivo_sin_resultados(total: Optional[int]) -> str:
    if total:
        return (
            f"el sitio reporta {total} coincidencias de texto, pero esta "
            "pagina no expuso dispositivos individuales: las coincidencias "
            "están en cuadernillos sin página individual (PC/DJ/JU/CA/IN/TU)"
        )
    return "sin coincidencias en el sitio para esos filtros"

def _nota_vacio(params: dict) -> str:
    tips = []
    q = params.get("query", "")
    if q and " " in q and '"' not in q:
        tips.append(
            "el sitio combina las palabras: usa menos palabras o la frase "
            "exacta entre comillas dobles"
        )
    if params.get("fechaIni") and not params.get("fechaFin"):
        tips.append("rango de fechas incompleto (falta fecha_fin)")
    tips.append(
        "verifica el día con listar_cuadernillos "
        "(esa fecha puede no tener publicación)"
    )
    return "Sugerencias: " + "; ".join(tips) + "."

@mcp.tool()
async def buscar_dispositivos(
    consulta: Optional[str] = None,
    cuadernillo: Optional[str] = None,
    sector: Optional[str] = None,
    fecha: Optional[str] = None,
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    start: int = 0,
    paginas: int = 1,
) -> dict:
    """Busca dispositivos y cuadernillos en El Peruano.

    Args:
        consulta: texto libre. Frases exactas entre comillas dobles,
                  ej: '"prescripcion adquisitiva de dominio"'.
                  El sitio combina palabras por coincidencia de texto y
                  no garantiza un AND estricto entre todas las palabras.
        cuadernillo: sigla del tipo de publicacion (ver listar_tipos_publicacion):
                     NL, BO, EX, PC, DJ, JU, SE, CA, IN, TU. Opcional.
        sector: post-filtro por subcadena sobre el campo `sector` de los
                resultados obtenidos (ej: "salud", "transportes"). NO filtra
                todo el corpus: solo la pagina(s) de resultados actual.
        fecha: dia puntual YYYYMMDD.
        fecha_ini, fecha_fin: rango de fechas YYYYMMDD.
        start: desplazamiento de resultados (pagina N -> start = N*20).
        paginas: paginas consecutivas a traer en una sola llamada (1 = 20 resultados).

    Nota de latencia: sin filtro de cuadernillo, el sitio puede tardar
    10-40 s en generar la respuesta (con filtro NL/BO es mas rapido).
    El sitio pagina por coincidencias de texto y puede repetir
    dispositivos entre paginas: usa `paginas` (deduplica internamente)
    en lugar de `start` manual.

    Devuelve:
        coincidencias_texto: contador del sitio (coincidencias de texto, no
        dispositivos unicos). dispositivos_unicos: cantidad de dispositivos
        individuales devueltos. ms: duracion de la llamada en milisegundos.
        data: dispositivos con pagina individual (sector, tipo, numero,
        fecha, titulo, url).
        cuadernillos_con_coincidencias: cuando las coincidencias caen en
        cuadernillos SIN pagina individual (PC/DJ/JU/CA/IN/TU) el resultado
        son cuadernillos (compacto): codigo, fecha, paginas y pdf_url; el
        texto integral esta en el PDF.
        siguiente_start: start para la proxima pagina, si hay mas.
        cacheado: true si la respuesta vino de la cache de resultados.
        sin_resultados_motivo + nota: cuando no hay resultados, con la causa
        y sugerencias.
    """
    cuadernillo = (cuadernillo or "").strip().upper() or None
    if cuadernillo and cuadernillo not in TIPOS_PUBLICACION:
        return {"ok": False, "error": f"Cuadernillo '{cuadernillo}' inválido. Ver listar_tipos_publicacion."}
    fecha_raw = fecha
    fecha = _norm_fecha(fecha_raw)
    if fecha_raw and not fecha:
        return {"ok": False, "error": f"Fecha inválida: {fecha_raw}. Formatos: YYYYMMDD, AAAA-MM-DD o DD/MM/AAAA."}
    ini_raw = fecha_ini
    fecha_ini = _norm_fecha(ini_raw)
    if ini_raw and not fecha_ini:
        return {"ok": False, "error": f"Fecha inválida: {ini_raw}."}
    fin_raw = fecha_fin
    fecha_fin = _norm_fecha(fin_raw)
    if fin_raw and not fecha_fin:
        return {"ok": False, "error": f"Fecha inválida: {fin_raw}."}
    if fecha_ini and fecha_fin and fecha_fin < fecha_ini:
        return {"ok": False, "error": "Rango inválido: fecha_fin es anterior a fecha_ini."}
    sector_f = (sector or "").strip() or None
    paginas = max(1, min(int(paginas or 1), MAX_PAGINAS))
    t0 = time.time()
    params = _params_busqueda(
        consulta, None, cuadernillo, fecha, fecha_ini, fecha_fin, int(start),
    )

    ckey = _sres_key(params, sector_f)
    cached = _sres_get(ckey)
    if cached is not None:
        import copy

        out = copy.deepcopy(cached)
        out["cacheado"] = True
        out["cache"] = "resultado"
        out["ms"] = max(1, int((time.time() - t0) * 1000))
        return out

    docs, cuads = [], []
    seen_d: set[str] = set()
    seen_c: set[str] = set()
    total: Optional[int] = None
    hay_mas = False
    for _ in range(paginas):
        body = await _fetch("/", params=params)
        if total is None:
            total = _parse_total(body)
        for d in _parse_cards(body):
            if d["op"] not in seen_d:
                seen_d.add(d["op"])
                docs.append(d)
        for c in _parse_cuadernillos_cards(body):
            if c["codigo"] not in seen_c:
                seen_c.add(c["codigo"])
                cuads.append(c)
        hay_mas = f"start={int(params['start']) + PAGE_SIZE}" in body
        if not hay_mas:
            break
        params["start"] = str(int(params["start"]) + PAGE_SIZE)

    if sector_f:
        docs = [d for d in docs if sector_f.lower() in d.get("sector", "").lower()]

    docs_d = [d for d in docs if not sector_f or sector_f.lower() in d.get("sector", "").lower()]
    r: dict[str, Any] = {
        "ok": True,
        "cacheado": False,
        "coincidencias_texto": total if total is not None else 0,
        "dispositivos_unicos": len({d["op"] for d in docs_d}),
        "data": docs_d,
        "cuadernillos_con_coincidencias": [
            {k: v for k, v in c.items() if k not in ("thumbnail_url", "url_viewer")}
            for c in cuads
        ],
    }
    if not docs and cuads:
        r["cuadernillos_compacto"] = True
    r["filtros"] = {
        k: v
        for k, v in params.items()
        if k != "start" and k != "ci" and v
    }
    if sector_f:
        r["filtros"]["sector"] = sector_f
    if hay_mas:
        r["siguiente_start"] = int(params["start"])
    if cuadernillo in SIN_INDIVIDUAL:
        r["nota"] = (
            f"El cuadernillo {cuadernillo} no tiene dispositivos individuales: "
            "el texto integral solo está en el PDF de cada cuadernillo (pdf_url)."
        )
    if sector_f and not docs_d:
        r["sin_resultados_motivo"] = (
            f"el filtro sector='{sector_f}' no matcheo dispositivos en esta "
            "pagina de resultados"
        )
        r["nota"] = _nota_vacio(params)
    elif not docs and not cuads:
        r["sin_resultados_motivo"] = _motivo_sin_resultados(total)
        r["nota"] = _nota_vacio(params)
    _sres_put(ckey, r)
    import copy

    out = copy.deepcopy(r)
    out["cache"] = "pagina"
    out["ms"] = max(1, int((time.time() - t0) * 1000))
    return out
@mcp.tool()
async def buscar_por_op(op: str) -> dict:
    """Busqueda puntual por numero de orden de publicacion (OP).

    Args:
        op: formato NNNNNNN-N, ej '2558398-1'.
    """
    op = op.strip()
    t0 = time.time()
    if not OP_RE.match(op):
        return {"ok": False, "error": "Formato OP inválido: NNNNNNN-N (7 dígitos, guion, dígito)."}
    body = await _fetch("/", params={"op": op, "start": "0"})
    docs = _parse_cards(body)
    total = _parse_total(body)
    if not docs:
        return {"ok": False, "op": op, "nota": "OP no encontrada o inexistente."}
    d0 = docs[0]
    return {
        "ok": True,
        "op": op,
        "cuadernillo": d0["cuadernillo"],
        "tipo_dispositivo": d0["tipo_dispositivo"],
        "numero": d0["numero"],
        "sector": d0["sector"],
        "fecha": d0["fecha"],
        "titulo": d0["titulo"],
        "url": d0["url"],
        "coincidencias_texto": total if total is not None else 0,
        "ms": max(1, int((time.time() - t0) * 1000)),
        "data": [d0],
    }

async def _enriquecer(d: dict) -> None:
    """Completa metadatos del dispositivo con el card del buscador por OP
    (sector, fecha de publicacion y los que falten: sumilla, tipo, numero)."""
    if not d.get("op"):
        return
    try:
        body = await _fetch("/", params={"op": d["op"], "start": "0"})
        for c in _parse_cards(body):
            if c["op"] == d["op"]:
                d["sector"] = d.get("sector") or c.get("sector") or ""
                d["fecha_publicacion"] = d.get("fecha_publicacion") or c.get("fecha") or ""
                if not d.get("sumilla"):
                    d["sumilla"] = c.get("titulo") or d.get("sumilla", "")
                if not d.get("tipo_dispositivo"):
                    d["tipo_dispositivo"] = c.get("tipo_dispositivo") or ""
                if not d.get("numero"):
                    d["numero"] = c.get("numero") or ""
                break
    except RuntimeError:
        pass

@mcp.tool()
async def obtener_dispositivo(
    cuadernillo: Optional[str] = None,
    op: str = "",
    desde: int = 0,
    hasta: int = 0,
) -> dict:
    """Devuelve el dispositivo completo: sumilla, tipo, numero, sector,
    fechas y texto integral, mas pdf_url y thumbnail.

    Nota: el texto integral puede ser muy largo. Para documentos extensos
    usa obtener_dispositivo_texto (rangos de parrafos) o limita aqui con
    desde/hasta para no saturar el contexto.

    Args:
        cuadernillo: sigla de publicacion (ver listar_tipos_publicacion).
        op: numero de orden, ej '2558398-1'.
        desde, hasta: opcionales; rango de parrafos 1-indexed (inclusivo).
                      Vacios = texto completo.
    """
    op = op.strip()
    t0 = time.time()
    if not cuadernillo:
        body = await _fetch("/", params={"op": op, "start": "0"})
        docs = _parse_cards(body)
        if not docs:
            return {"ok": False, "error": f"Dispositivo no encontrado por OP: {op}"}
        cuadernillo = docs[0]["cuadernillo"]
    cuadernillo = cuadernillo.strip().upper()
    try:
        body = await _fetch(f"/dispositivo/{cuadernillo}/{op}")
    except RuntimeError as e:
        if "404" in str(e):
            return {"ok": False, "error": f"Dispositivo no encontrado: {cuadernillo}/{op}"}
        raise
    d = _parse_dispositivo(body)
    if not d.get("op"):
        return {"ok": False, "error": f"Dispositivo no encontrado: {cuadernillo}/{op}"}
    await _enriquecer(d)
    d["ok"] = True
    d["ms"] = int((time.time() - t0) * 1000)
    if d.get("sin_texto_html"):
        d["nota"] = (
            "Este tipo de publicacion no expone texto HTML individual; "
            "usa pdf_url (el cliente es quien descarga el archivo)."
        )
    elif int(desde) > 0 or int(hasta) > 0:
        paras = d["texto"].split("\n")
        ini = max(0, int(desde) - 1) if int(desde) > 0 else 0
        fin = int(hasta) if int(hasta) > 0 else len(paras)
        sel = paras[ini: max(ini, min(fin, len(paras)))]
        d["texto"] = "\n".join(sel)
        d["parrafos_rango"] = [ini + 1, max(ini, min(fin, len(paras)))]
        d["parrafos_totales"] = len(paras)
    return d

@mcp.tool()
async def obtener_dispositivo_texto(
    cuadernillo: Optional[str] = None,
    op: str = "",
    desde: int = 1,
    hasta: int = 50,
) -> dict:
    """Texto de un dispositivo por parrafos, en dosis controladas
    (para documentos largos sin saturar el contexto).

    Incluye los metadatos del dispositivo (sumilla, tipo, numero, sector,
    fecha). Sin tope de parrafos: usa rangos razonables.

    Args:
        cuadernillo: sigla de publicacion.
        op: numero de orden.
        desde, hasta: rango de parrafos (1-indexed, inclusivo).
    """
    op = op.strip()
    t0 = time.time()
    if not cuadernillo:
        body = await _fetch("/", params={"op": op, "start": "0"})
        docs = _parse_cards(body)
        if not docs:
            return {"ok": False, "error": f"Dispositivo no encontrado por OP: {op}"}
        cuadernillo = docs[0]["cuadernillo"]
    cuadernillo = cuadernillo.strip().upper()
    body = await _fetch(f"/dispositivo/{cuadernillo}/{op}")
    d = _parse_dispositivo(body)
    if not d.get("op"):
        return {"ok": False, "nota": f"Dispositivo no encontrado: {cuadernillo}/{op}"}
    await _enriquecer(d)
    d["ms"] = int((time.time() - t0) * 1000)
    paras = d["texto"].split("\n")
    ini = max(0, int(desde) - 1)
    sel = paras[ini: max(ini, int(hasta))]
    d.pop("texto", None)
    d["texto"] = "\n".join(sel)
    d["parrafos_devueltos"] = len(sel)
    d["parrafos_totales"] = len(paras)
    d["rango"] = [ini + 1, hasta]
    d["ok"] = bool(sel)
    if not sel:
        d["nota"] = "Rango vacío o sin texto HTML individual."
    return d

@mcp.tool()
async def listar_cuadernillos(
    fecha: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    tipo: Optional[str] = None,
) -> dict:
    """Lista cuadernillos publicados en una fecha o rango (con pdf_url).

    Args:
        fecha: YYYYMMDD, AAAA-MM-DD o DD/MM/AAAA (vacio = los ultimos publicados).
        fecha_fin: opcional; con fecha, recorre el rango hasta esta fecha
                   (maximo 7 dias INCLUSIVE por llamada).
        tipo: filtro por sigla de los cuadernillos listados (ej "NL").
    """
    f_ini = _norm_fecha(fecha)
    f_fin = _norm_fecha(fecha_fin)
    if f_fin and not f_ini:
        return {"ok": False, "error": "Fecha fin sin fecha inicial."}
    if f_ini and f_fin and f_fin < f_ini:
        return {"ok": False, "error": "Rango inválido: fecha_fin es anterior a fecha."}
    fechas: list[str] = []
    if f_ini and f_fin:
        from datetime import date, timedelta

        d0 = date(int(f_ini[:4]), int(f_ini[4:6]), int(f_ini[6:8]))
        d1 = date(int(f_fin[:4]), int(f_fin[4:6]), int(f_fin[6:8]))
        d = d0
        while d <= d1 and len(fechas) < 7:
            fechas.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        if d <= d1:
            return {"ok": False, "error": "Rango demasiado amplio: maximo 7 dias por llamada."}
    elif f_ini:
        fechas = [f_ini]
    else:
        fechas = [None]
    tipo_f = (tipo or "").strip().upper() or None
    data: list[dict] = []
    for fd in fechas:
        params: dict[str, str] = {"start": "0"}
        if fd:
            params["fecha"] = fd
        body = await _fetch("/", params=params)
        for c in _parse_cuadernillos_cards(body):
            if tipo_f and c["tipo"] != tipo_f:
                continue
            if all(c["codigo"] != x["codigo"] for x in data):
                data.append(c)
    return {
        "ok": bool(data),
        "fecha": f_ini,
        "fecha_fin": f_fin,
        "tipo": tipo_f,
        "total": len(data),
        "data": data,
        "nota": "total = cuadernillos devueltos en esta respuesta.",
    }
@mcp.tool()
async def descargar_pdf(cuadernillo: str, op: str) -> dict:
    """URL de descarga del PDF de un dispositivo individual.

    El MCP NUNCA almacena archivos: devuelve la URL publica en
    busquedas.elperuano.pe para que el usuario descargue por su cuenta.

    Args:
        cuadernillo: sigla de publicacion (ver listar_tipos_publicacion).
        op: numero de orden, ej '2558398-1'.
    """
    cuadernillo = cuadernillo.strip().upper()
    op = op.strip()
    try:
        body = await _fetch(f"/dispositivo/{cuadernillo}/{op}")
    except RuntimeError as e:
        if "404" in str(e):
            return {"ok": False, "error": f"Dispositivo no encontrado: {cuadernillo}/{op}"}
        raise
    d = _parse_dispositivo(body)
    if not d.get("pdf_url"):
        return {"ok": False, "error": f"PDF no disponible para {cuadernillo}/{op}."}
    probe = await _head_bytes(d["pdf_url"])
    if probe["status"] == 0:
        return {"ok": False, "error": "No se pudo verificar el PDF (fallo de red).", "pdf_url": d["pdf_url"]}
    if probe["status"] == 404:
        return {"ok": False, "error": f"El sitio no expone PDF para {cuadernillo}/{op}."}
    out: dict[str, Any] = {
        "ok": True,
        "documento": " ".join(x for x in [d.get("tipo_dispositivo"), d.get("numero")] if x),
        "nombre_sugerido": f"EP_{op}.pdf",
        "pdf_url": d["pdf_url"],
        "thumbnail_url": d.get("thumbnail_url"),
        "nota": "El MCP no guarda el PDF: descargalo desde pdf_url en tu dispositivo.",
    }
    if probe["bytes"] is not None:
        out["bytes"] = probe["bytes"]
    return out

@mcp.tool()
async def descargar_cuadernillo(codigo: str) -> dict:
    """URL de descarga del PDF completo de un cuadernillo.

    Es la unica via de texto para cuadernillos SIN dispositivo
    individual (PC/DJ/JU/CA/IN/TU). El MCP no descarga ni guarda:
    devuelve la URL publica para el cliente.

    Devuelve tambien url_viewer (pagina visor del cuadernillo). Nota: el
    sitio publica el archivo con nombres con sufijo de pagina inicial
    (ej PC20260606_4335.pdf); otras URLs del mismo cuadernillo (sin
    sufijo) tambien sirven el mismo PDF.

    Args:
        codigo: codigo TIPO+FECHA, ej 'NL20230923' o 'PC20260927'.
    """
    m = CUADERNILLO_RE.match(codigo.strip().upper())
    if not m:
        return {"ok": False, "error": "Código inválido: formato TIPO+YYYYMMDD (ej NL20230923)."}
    tipo, fecha = m.group(1), m.group(2)
    code = f"{tipo}{fecha}"
    try:
        body = await _fetch(f"/cuadernillo/{tipo}/{fecha}")
    except RuntimeError as e:
        if "404" in str(e):
            return {"ok": False, "error": f"Cuadernillo no publicado: {code}"}
        raise
    if len(body) < 4000 and "__reactRouterContext" in body:
        return {"ok": False, "error": f"Cuadernillo no publicado: {code} (no existe para esa fecha)."}
    pdf: Optional[str] = None
    mm = _RE_PDF_FILE.search(body)
    if mm:
        pdf = f"{BASE_URL}/api/archivo/file/{mm.group(1)}/*/{mm.group(2)}.pdf"
    if not pdf:
        th = _RE_OG_IMAGE.search(body)
        if th:
            pdf = _pdf_from_thumb(th.group(1) if th.group(1).startswith("http") else BASE_URL + th.group(1))
    if not pdf:
        return {"ok": False, "error": f"PDF no disponible para {code}."}
    probe = await _head_bytes(pdf)
    if probe["status"] == 0:
        return {"ok": False, "error": "No se pudo verificar el PDF (fallo de red).", "pdf_url": pdf}
    if probe["status"] == 404:
        return {"ok": False, "error": f"El sitio no expone PDF de {code}."}
    out: dict[str, Any] = {
        "ok": True,
        "codigo": code,
        "documento": TIPOS_PUBLICACION.get(tipo, tipo),
        "nombre_sugerido": f"{code}.pdf",
        "pdf_url": pdf,
        "url_viewer": f"{BASE_URL}/cuadernillo/{tipo}/{fecha}",
    }
    if probe["bytes"] is not None:
        out["bytes"] = probe["bytes"]
    nota = (
        "El MCP no guarda el PDF; úsalo desde pdf_url. "
        + (
            f"{tipo} no tiene dispositivos individuales: el texto integral solo está en este PDF."
            if tipo in SIN_INDIVIDUAL
            else "Este es el PDF del cuadernillo completo."
        )
    )
    out["nota"] = nota
    return out

@mcp.tool()
async def obtener_cuadernillo_texto(
    codigo: str,
    pagina_desde: int = 1,
    pagina_hasta: int = 5,
    query: Optional[str] = None,
) -> dict:
    """Busca texto DENTRO del PDF de un cuadernillo (query) o lee su texto
    por paginas, en rangos controlados.

    Es la unica via de lectura del texto integral para cuadernillos SIN
    pagina individual (PC/DJ/JU/CA/IN/TU): el PDF se descarga en memoria y
    se procesa pagina por pagina; nunca se guarda en disco.

    Args:
        codigo: codigo TIPO+FECHA, ej 'PC20260927'.
        pagina_desde, pagina_hasta: rango de paginas del PDF (1-indexed,
        inclusivo). Maximo 10 paginas por llamada.
        query: opcional; texto a buscar en TODAS las paginas del PDF
               (case-insensitive). Si se indica, el resultado son las
               paginas que contienen el texto, con extractos de contexto
               (ignora pagina_desde/pagina_hasta). Para PDFs de cientos
               de paginas.
    """
    if not _PYPDF_OK:
        return {
            "ok": False,
            "error": "Lectura de PDF no disponible en esta instancia "
            "(requiere pypdf instalado en el entorno del servidor).",
        }
    m = CUADERNILLO_RE.match(codigo.strip().upper())
    if not m:
        return {"ok": False, "error": "Código inválido: formato TIPO+YYYYMMDD (ej NL20230923)."}
    tipo, fecha = m.group(1), m.group(2)
    code = f"{tipo}{fecha}"
    try:
        body = await _fetch(f"/cuadernillo/{tipo}/{fecha}")
    except RuntimeError as e:
        if "404" in str(e):
            return {"ok": False, "error": f"Cuadernillo no publicado: {code}"}
        raise
    if len(body) < 4000 and "__reactRouterContext" in body:
        return {"ok": False, "error": f"Cuadernillo no publicado: {code} (no existe para esa fecha)."}
    mm = _RE_PDF_FILE.search(body)
    if not mm:
        th = _RE_OG_IMAGE.search(body)
        pdf = (
            _pdf_from_thumb(th.group(1) if th.group(1).startswith("http") else BASE_URL + th.group(1))
            if th
            else None
        )
    else:
        pdf = f"{BASE_URL}/api/archivo/file/{mm.group(1)}/*/{mm.group(2)}.pdf"
    if not pdf:
        return {"ok": False, "error": f"PDF no disponible para {code}."}
    r = await _client().get(pdf)
    if r.status_code >= 400:
        return {"ok": False, "error": f"No se pudo obtener el PDF ({r.status_code}).", "pdf_url": pdf}
    import io as _io

    import pypdf

    try:
        reader = pypdf.PdfReader(_io.BytesIO(r.content))
    except Exception as e:
        return {"ok": False, "error": f"PDF ilegible: {e}", "pdf_url": pdf}
    total = len(reader.pages)
    q = (query or "").strip()
    if q:
        q_low = q.lower()
        matches = []
        for n in range(1, total + 1):
            texto = reader.pages[n - 1].extract_text() or ""
            low = texto.lower()
            idx = low.find(q_low)
            if idx >= 0:
                a = max(0, idx - 120)
                extracto = re.sub(r"\s+", " ", texto[a: idx + len(q_low) + 200]).strip()
                matches.append({"pagina": n, "extracto": extracto})
            if len(matches) >= 50:
                break
        return {
            "ok": True,
            "codigo": code,
            "documento": TIPOS_PUBLICACION.get(tipo, tipo),
            "paginas_totales": total,
            "query": q,
            "matches": matches,
            "total_matches": len(matches),
            "nota": (
                "Texto extraido del PDF en memoria (el cuadernillo no se "
                "guarda en disco). Usa el numero de pagina con "
                "pagina_desde/pagina_hasta para leer el contexto completo."
                + ("" if matches else " Sin coincidencias dentro de este cuadernillo.")
            ),
        }
    p_ini = max(1, int(pagina_desde))
    p_fin = min(total, max(p_ini, int(pagina_hasta)), p_ini + 9)
    if p_ini > total:
        return {"ok": False, "error": f"pagina_desde ({p_ini}) excede paginas_totales ({total})."}
    paginas = []
    for n in range(p_ini, p_fin + 1):
        texto = reader.pages[n - 1].extract_text() or ""
        texto = re.sub(r"[ \t]+", " ", texto).strip()
        paginas.append({"pagina": n, "texto": texto})
    return {
        "ok": True,
        "codigo": code,
        "documento": TIPOS_PUBLICACION.get(tipo, tipo),
        "paginas_totales": total,
        "rango": [p_ini, p_fin],
        "paginas": paginas,
        "nota": "Texto extraido del PDF en memoria: el cuadernillo no se guarda en disco. "
        "Usa pagina_desde/pagina_hasta para navegar el documento por partes.",
    }

@mcp.tool()
async def listar_tipos_publicacion() -> dict:
    """Siglas de cuadernillos (tipoPublicacion) y sus titulos.

    PC/DJ/JU/CA/IN/TU no tienen dispositivo individual; para esos la
    busqueda devuelve cuadernillos_con_coincidencias y el texto integral
    solo vive en el PDF del cuadernillo (descargar_cuadernillo).
    """
    return {
        "data": [
            {"code": k, "titulo": v, "sin_individual": k in SIN_INDIVIDUAL}
            for k, v in TIPOS_PUBLICACION.items()
        ]
    }

def main() -> None:
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
