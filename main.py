"""
TikTok Profile API — MVM Dashboard
Microservico que faz scraping de perfis do TikTok.

Endpoints:
  GET  /health          → status do worker
  GET  /profile         → scraping de 1 perfil (usado pelo dashboard)
  POST /batch           → recebe lista de usernames, processa em background
  GET  /batch/status    → status do processamento batch
  GET  /batch/results   → resultados quando terminar
"""

import re
import json
import random
import os
import time
import threading

import requests
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor

try:
    import brotli  # noqa: F401
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False

# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("API_KEY", "")
WORKER_ID = os.environ.get("WORKER_ID", "1")
ACCEPT_ENCODING = "gzip, deflate, br, zstd" if BROTLI_AVAILABLE else "gzip, deflate"

DELAY_MIN = 1.0
DELAY_MAX = 2.0

app = FastAPI(
    title="TikTok Profile API",
    description="Scraping de perfis TikTok para o dashboard MVM",
    version="2.0.0",
    docs_url=None,
    redoc_url=None,
)


# ============================================================
# AUTH
# ============================================================

async def verify_api_key(x_api_key: str = Header(...)):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY not configured")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


# ============================================================
# TIKTOK HEADERS
# ============================================================

HEADERS_LIST = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "sec-ch-ua": '"Chromium";v="145", "Not-A.Brand";v="24", "Google Chrome";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    },
]


def tt_headers():
    h = random.choice(HEADERS_LIST).copy()
    h["Accept-Encoding"] = ACCEPT_ENCODING
    return h


# ============================================================
# EXTRACTION
# ============================================================

def extract_profile(html: str, username: str) -> dict | None:
    """Extrai dados do perfil a partir do HTML do TikTok."""

    m = re.search(
        r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    if m:
        try:
            d = json.loads(m.group(1))
            ud = d.get("__DEFAULT_SCOPE__", {}).get("webapp.user-detail", {})

            if ud.get("statusCode") == 10202:
                return {"error": "not_found", "detail": "Account does not exist"}

            ui = ud.get("userInfo", {})
            u = ui.get("user", {})
            sv2 = ui.get("statsV2", {})
            s = ui.get("stats", {})

            if u.get("uniqueId") or u.get("id"):
                try:
                    seg = int(sv2.get("followerCount", 0))
                except (ValueError, TypeError):
                    seg = s.get("followerCount", 0)
                return {
                    "creator_id": u.get("id", ""),
                    "username": u.get("uniqueId", username),
                    "nome": u.get("nickname", ""),
                    "bio": u.get("signature", ""),
                    "avatar_url": u.get("avatarLarger") or u.get("avatarMedium") or "",
                    "seguidores": seg,
                }
        except (json.JSONDecodeError, KeyError):
            pass

    m = re.search(r'<script\s+id="SIGI_STATE"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            d = json.loads(m.group(1))
            um = d.get("UserModule", {})
            u = um.get("users", {}).get(username, {})
            s = um.get("stats", {}).get(username, {})
            sv2 = um.get("statsV2", {}).get(username, {})
            if u.get("uniqueId") or u.get("id"):
                try:
                    seg = int(sv2.get("followerCount", 0))
                except (ValueError, TypeError):
                    seg = s.get("followerCount", 0)
                return {
                    "creator_id": u.get("id", ""),
                    "username": u.get("uniqueId", username),
                    "nome": u.get("nickname", ""),
                    "bio": u.get("signature", ""),
                    "avatar_url": u.get("avatarLarger") or u.get("avatarMedium") or "",
                    "seguidores": seg,
                }
        except (json.JSONDecodeError, KeyError):
            pass

    return None


# ============================================================
# SINGLE SCRAPE (para /profile do dashboard)
# ============================================================

def scrape_tiktok(username: str) -> dict:
    """Faz scraping de um perfil do TikTok com retries."""
    url = f"https://www.tiktok.com/@{username}"
    session = requests.Session()
    last_error = "unknown"

    for attempt in range(3):
        try:
            r = session.get(url, headers=tt_headers(), timeout=15)
        except requests.Timeout:
            last_error = "timeout"
            time.sleep(1)
            continue
        except requests.RequestException:
            last_error = "connection_error"
            time.sleep(1)
            continue

        if r.status_code == 200:
            html = r.text
            if "<html" not in html[:500].lower():
                last_error = "empty_html"
                time.sleep(1)
                continue
            result = extract_profile(html, username)
            if result:
                return result
            last_error = "parse_failed"
            time.sleep(1)
            continue
        elif r.status_code == 404:
            return {"error": "not_found", "detail": "Account does not exist"}
        elif r.status_code == 403:
            last_error = "blocked_403"
            time.sleep(1)
            continue
        elif r.status_code == 429:
            last_error = "rate_limited"
            time.sleep(5)
            continue
        else:
            last_error = f"http_{r.status_code}"
            time.sleep(1)
            continue

    return {"error": last_error, "detail": f"Failed after 3 attempts"}


# ============================================================
# BATCH SCRAPE (para backfill)
# ============================================================

# Estado do batch em memória
batch_state = {
    "status": "idle",    # idle | processing | done
    "round": 0,
    "threads": 20,
    "total": 0,
    "processed": 0,
    "ok": 0,
    "not_found": 0,
    "failed": 0,
    "results": [],       # perfis OK
    "failures": [],      # usernames que falharam
}
batch_lock = threading.Lock()


def scrape_one_direct(username: str) -> dict:
    """Scraping direto no TikTok (1 tentativa, sem retry)."""
    url = f"https://www.tiktok.com/@{username}"
    session = requests.Session()

    try:
        r = session.get(url, headers=tt_headers(), timeout=15)
    except requests.Timeout:
        return {"username": username, "status": "error_timeout"}
    except requests.RequestException:
        return {"username": username, "status": "error_connection"}

    # Delay DEPOIS da request
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    if r.status_code == 200:
        html = r.text
        if "<html" not in html[:500].lower():
            return {"username": username, "status": "error_empty_html"}
        result = extract_profile(html, username)
        if result:
            if "error" in result:
                return {"username": username, "status": "not_found"}
            result["username"] = username
            result["status"] = "ok"
            return result
        return {"username": username, "status": "error_parse"}
    elif r.status_code == 404:
        return {"username": username, "status": "not_found"}
    elif r.status_code == 403:
        return {"username": username, "status": "error_403"}
    elif r.status_code == 429:
        time.sleep(5)
        return {"username": username, "status": "error_429"}
    else:
        return {"username": username, "status": f"error_{r.status_code}"}


def run_batch(usernames: list, num_threads: int, round_num: int):
    """Processa batch em background."""
    global batch_state

    results = []
    failures = []
    ok_count = 0
    nf_count = 0
    fail_count = 0
    processed = 0
    lock = threading.Lock()

    def on_done(future):
        nonlocal processed, ok_count, nf_count, fail_count
        try:
            result = future.result()
        except Exception:
            return

        with lock:
            processed += 1
            status = result.get("status", "")

            if status == "ok":
                results.append(result)
                ok_count += 1
            elif status == "not_found":
                nf_count += 1
            else:
                failures.append(result["username"])
                fail_count += 1

            # Atualizar estado
            with batch_lock:
                batch_state["processed"] = processed
                batch_state["ok"] = ok_count
                batch_state["not_found"] = nf_count
                batch_state["failed"] = fail_count

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for username in usernames:
            future = executor.submit(scrape_one_direct, username)
            future.add_done_callback(on_done)
            futures.append(future)

        for future in futures:
            future.result()

    with batch_lock:
        batch_state["status"] = "done"
        batch_state["results"] = results
        batch_state["failures"] = failures


class BatchRequest(BaseModel):
    usernames: list[str]
    threads: int = 20
    round: int = 0


# ============================================================
# ROUTES
# ============================================================

@app.get("/health")
async def health():
    return {"status": "ok", "worker": WORKER_ID}


@app.get("/profile")
async def get_profile(username: str, _key: str = Depends(verify_api_key)):
    username = username.strip().lstrip("@")
    if not username:
        raise HTTPException(status_code=400, detail="username is required")

    result = scrape_tiktok(username)

    if "error" in result:
        status = 404 if result["error"] == "not_found" else 502
        return JSONResponse(status_code=status, content=result)

    return result


@app.post("/batch")
async def start_batch(req: BatchRequest, _key: str = Depends(verify_api_key)):
    """Recebe lista de usernames e processa em background."""
    global batch_state

    with batch_lock:
        if batch_state["status"] == "processing":
            raise HTTPException(status_code=409, detail="Batch already processing")

        batch_state = {
            "status": "processing",
            "round": req.round,
            "threads": req.threads,
            "total": len(req.usernames),
            "processed": 0,
            "ok": 0,
            "not_found": 0,
            "failed": 0,
            "results": [],
            "failures": [],
        }

    # Rodar em background
    thread = threading.Thread(
        target=run_batch,
        args=(req.usernames, req.threads, req.round),
        daemon=True,
    )
    thread.start()

    return {
        "message": f"Batch iniciado: {len(req.usernames)} usernames, {req.threads} threads",
        "worker": WORKER_ID,
    }


@app.get("/batch/status")
async def batch_status(_key: str = Depends(verify_api_key)):
    """Retorna status do processamento batch."""
    with batch_lock:
        return {
            "worker": WORKER_ID,
            "status": batch_state["status"],
            "round": batch_state["round"],
            "threads": batch_state["threads"],
            "total": batch_state["total"],
            "processed": batch_state["processed"],
            "ok": batch_state["ok"],
            "not_found": batch_state["not_found"],
            "failed": batch_state["failed"],
        }


@app.get("/batch/results")
async def batch_results(_key: str = Depends(verify_api_key)):
    """Retorna resultados do batch quando terminar."""
    with batch_lock:
        if batch_state["status"] != "done":
            raise HTTPException(status_code=425, detail=f"Batch not done yet (status: {batch_state['status']})")

        return {
            "worker": WORKER_ID,
            "results": batch_state["results"],
            "failures": batch_state["failures"],
            "ok": batch_state["ok"],
            "not_found": batch_state["not_found"],
            "failed": batch_state["failed"],
        }
