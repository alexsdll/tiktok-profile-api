"""
TikTok Profile API — MVM Dashboard
Microservico que faz scraping de perfis do TikTok e retorna dados estruturados.
Deploy: Railway (5 workers, cada um com IP diferente para failover).
"""

import re
import json
import random
import os
import time

import requests
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import JSONResponse

try:
    import brotli  # noqa: F401
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False

# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("API_KEY", "")
ACCEPT_ENCODING = "gzip, deflate, br, zstd" if BROTLI_AVAILABLE else "gzip, deflate"

# Retry config
MAX_RETRIES = 3
RETRY_DELAY = 1.0

app = FastAPI(
    title="TikTok Profile API",
    description="Scraping de perfis TikTok para o dashboard MVM",
    version="1.0.0",
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

    # Tenta __UNIVERSAL_DATA_FOR_REHYDRATION__ (formato mais recente)
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

    # Fallback: SIGI_STATE (formato mais antigo)
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


def scrape_tiktok(username: str) -> dict:
    """Faz scraping de um perfil do TikTok com retries."""
    url = f"https://www.tiktok.com/@{username}"
    session = requests.Session()

    last_error = "unknown"

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, headers=tt_headers(), timeout=15)
        except requests.Timeout:
            last_error = "timeout"
            time.sleep(RETRY_DELAY)
            continue
        except requests.RequestException:
            last_error = "connection_error"
            time.sleep(RETRY_DELAY)
            continue

        if r.status_code == 200:
            html = r.text
            if "<html" not in html[:500].lower():
                last_error = "empty_html"
                time.sleep(RETRY_DELAY)
                continue

            result = extract_profile(html, username)
            if result:
                return result
            last_error = "parse_failed"
            time.sleep(RETRY_DELAY)
            continue

        elif r.status_code == 404:
            return {"error": "not_found", "detail": "Account does not exist"}
        elif r.status_code == 403:
            last_error = "blocked_403"
            time.sleep(RETRY_DELAY)
            continue
        elif r.status_code == 429:
            last_error = "rate_limited"
            time.sleep(5)
            continue
        else:
            last_error = f"http_{r.status_code}"
            time.sleep(RETRY_DELAY)
            continue

    return {"error": last_error, "detail": f"Failed after {MAX_RETRIES} attempts"}


# ============================================================
# ROUTES
# ============================================================

@app.get("/health")
async def health():
    return {"status": "ok", "worker": os.environ.get("WORKER_ID", "1")}


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
