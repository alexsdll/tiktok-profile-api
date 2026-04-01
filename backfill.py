"""
Backfill de tiktok_id e avatar para creators existentes.
3 etapas sequenciais com checkpoint em JSON no volume /data.

Etapa 1 — Scraping: busca perfis nos workers → salva JSON incrementalmente
Etapa 2 — Avatars: baixa imagens do CDN TikTok → upload pro Supabase Storage
Etapa 3 — Banco: atualiza creators + registra aliases

Variáveis de ambiente (Railway):
  SUPABASE_URL          → URL do projeto Supabase
  SUPABASE_SERVICE_KEY  → Service role key
  API_KEY               → Chave da API dos workers
  WORKER_COUNT          → Quantidade de workers (default: 10)
  DATA_DIR              → Diretório do volume (default: /data)
"""

import requests
import json
import time
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
API_KEY = os.environ.get("API_KEY", "")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "10"))
DATA_DIR = os.environ.get("DATA_DIR", "/data")

WORKERS = [f"https://w{i}.api.mvmcreators.com.br" for i in range(1, WORKER_COUNT + 1)]

THREADS_PER_WORKER = 5       # 5 × 10 = 50 simultâneas
AVATAR_THREADS = 50           # threads pra download de avatars (CDN aguenta)
DELAY_BETWEEN_REQUESTS = 1.0  # delay por thread entre requests ao worker
LOG_EVERY = 200

RESULTS_FILE = os.path.join(DATA_DIR, "scraping_results.json")
PROGRESS_FILE = os.path.join(DATA_DIR, "scraping_progress.json")


# ============================================================
# HELPERS
# ============================================================

def log(msg: str):
    print(msg, flush=True)


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


# ============================================================
# FETCH CREATORS SEM TIKTOK_ID
# ============================================================

def fetch_alunos_without_tiktok_id():
    """Busca alunos (com discord_id) sem tiktok_id."""
    alunos = []
    offset = 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/creators",
            headers={**supabase_headers(), "Accept": "application/json", "Accept-Profile": "public"},
            params={
                "select": '"Creator username"',
                "tiktok_id": "is.null",
                "discord_id": "not.is.null",
                "limit": 1000,
                "offset": offset,
            },
            timeout=30,
        )
        rows = r.json()
        if not rows:
            break
        alunos.extend([row["Creator username"] for row in rows])
        offset += 1000
    return alunos


def fetch_ativos_2026_without_tiktok_id(excluir: set):
    """Busca creators ativos em 2026 (GMV>0, vídeos>0 ou lives>0) sem tiktok_id, excluindo alunos."""
    outros = []
    offset = 0
    while True:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/get_active_creators_without_tiktok_id",
            headers={
                **supabase_headers(),
                "Accept": "application/json",
                "Range": f"{offset}-{offset + 999}",
            },
            json={},
            timeout=120,
        )
        if r.status_code in (200, 206):
            rows = r.json()
            if not rows:
                break
            outros.extend([row["creator_username"] for row in rows if row["creator_username"] not in excluir])
            offset += 1000
            if len(rows) < 1000:
                break
        else:
            log(f"   ⚠️ RPC falhou (HTTP {r.status_code}): {r.text[:200]}")
            break
    return outros


# ============================================================
# ETAPA 1 — SCRAPING
# ============================================================

assign_lock = threading.Lock()
assign_counter = [0]


def get_initial_worker_index():
    with assign_lock:
        idx = assign_counter[0] % WORKER_COUNT
        assign_counter[0] += 1
        return idx


def scrape_one(username: str) -> dict:
    """Faz scraping de um creator com failover rotacional."""
    start_idx = get_initial_worker_index()
    retried = False

    for attempt in range(WORKER_COUNT):
        worker_idx = (start_idx + attempt) % WORKER_COUNT
        worker_url = WORKERS[worker_idx]

        time.sleep(DELAY_BETWEEN_REQUESTS)

        try:
            r = requests.get(
                f"{worker_url}/profile",
                params={"username": username},
                headers={"x-api-key": API_KEY},
                timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                if "error" not in data:
                    return {
                        "username": username,
                        "status": "retry_ok" if retried else "ok",
                        "creator_id": data.get("creator_id", ""),
                        "nome": data.get("nome", ""),
                        "bio": data.get("bio", ""),
                        "avatar_url": data.get("avatar_url", ""),
                        "seguidores": data.get("seguidores", 0),
                        "worker": f"w{worker_idx + 1}",
                    }
            elif r.status_code == 404:
                return {"username": username, "status": "not_found"}
        except Exception:
            pass

        retried = True
        time.sleep(DELAY_BETWEEN_REQUESTS)  # extra delay no retry

    return {"username": username, "status": "scrape_failed"}


def scrape_batch(label: str, creators: list, results: list, completed_set: set):
    """Processa um bloco de creators. Retorna results e completed_set atualizados."""
    remaining = [c for c in creators if c not in completed_set]

    log(f"\n   🏷️ {label}")
    log(f"   ✅ Já processados: {len(creators) - len(remaining):,}")
    log(f"   📋 Restantes: {len(remaining):,}\n")

    if not remaining:
        log(f"   ✨ {label} já completo!")
        return results, completed_set

    stats = {"ok": 0, "retry_ok": 0, "not_found": 0, "scrape_failed": 0}
    stats_lock = threading.Lock()
    results_lock = threading.Lock()
    start_time = datetime.now()
    total = len(remaining)
    max_threads = THREADS_PER_WORKER * WORKER_COUNT

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(scrape_one, c): c for c in remaining}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                username = result["username"]
                status = result.get("status", "unknown")

                with stats_lock:
                    if status in stats:
                        stats[status] += 1

                with results_lock:
                    results.append(result)
                    completed_set.add(username)

                    if i % LOG_EVERY == 0 or i == total:
                        save_json(RESULTS_FILE, results)
                        save_json(PROGRESS_FILE, {"completed": list(completed_set)})

                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = i / elapsed if elapsed > 0 else 0
                        eta_min = (total - i) / rate / 60 if rate > 0 else 0
                        total_ok = stats["ok"] + stats["retry_ok"]
                        total_err = stats["not_found"] + stats["scrape_failed"]

                        log(f"📊 [{i:,}/{total:,}] {i/total*100:.1f}% — {label}")
                        log(f"   ✅ OK: {stats['ok']:,}")
                        log(f"   🔄 Retry OK: {stats['retry_ok']:,}")
                        log(f"   ❌ Erros: {total_err:,} (👻 {stats['not_found']:,} NF | 💀 {stats['scrape_failed']:,} fail)")
                        log(f"   ⚡ {rate:.1f}/s | ⏱️ ETA {eta_min:.0f}min\n")

            except Exception as e:
                log(f"   ⚠️ Exception: {e}")

    save_json(RESULTS_FILE, results)
    save_json(PROGRESS_FILE, {"completed": list(completed_set)})

    total_ok = stats["ok"] + stats["retry_ok"]
    log(f"   ✅ {label} completo: {total_ok:,} perfis obtidos")
    return results, completed_set


def etapa1_scraping(alunos: list, ativos: list):
    """Etapa 1: scraping em dois blocos — alunos primeiro, depois ativos 2026."""
    total_geral = len(alunos) + len(ativos)

    log(f"\n{'='*60}")
    log(f"📡 ETAPA 1 — SCRAPING ({total_geral:,} creators)")
    log(f"   {WORKER_COUNT} workers × {THREADS_PER_WORKER} threads = {THREADS_PER_WORKER * WORKER_COUNT} simultâneas")
    log(f"   Delay: {DELAY_BETWEEN_REQUESTS}s entre requests")
    log(f"   Ordem: 🎓 Alunos ({len(alunos):,}) → 👤 Ativos 2026 ({len(ativos):,})")
    log(f"{'='*60}")

    # Carregar progresso anterior
    progress = load_json(PROGRESS_FILE, {"completed": []})
    completed_set = set(progress.get("completed", []))
    results = load_json(RESULTS_FILE, [])

    # Bloco 1: Alunos
    results, completed_set = scrape_batch("🎓 Alunos", alunos, results, completed_set)

    # Bloco 2: Ativos 2026
    results, completed_set = scrape_batch("👤 Ativos 2026", ativos, results, completed_set)

    total_ok = sum(1 for r in results if r.get("status") in ("ok", "retry_ok"))
    log(f"\n   ✅ Etapa 1 completa: {total_ok:,} perfis obtidos no total")
    return results


# ============================================================
# ETAPA 2 — DOWNLOAD + UPLOAD AVATARS
# ============================================================

def upload_avatar(item: dict) -> dict:
    """Baixa avatar do CDN do TikTok e faz upload pro Supabase Storage."""
    username = item["username"]
    avatar_url = item.get("avatar_url", "")

    if not avatar_url:
        return {"username": username, "status": "no_url"}

    try:
        # Download do CDN do TikTok
        img_r = requests.get(avatar_url, timeout=15)
        if img_r.status_code != 200:
            return {"username": username, "status": "download_failed"}

        img_data = img_r.content
        storage_path = f"{username}.webp"

        # Upload pro Supabase Storage
        upload_r = requests.post(
            f"{SUPABASE_URL}/storage/v1/object/creator-avatars/{storage_path}",
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "image/webp",
                "x-upsert": "true",
            },
            data=img_data,
            timeout=15,
        )

        if upload_r.status_code in (200, 201):
            public_url = f"{SUPABASE_URL}/storage/v1/object/public/creator-avatars/{storage_path}"
            return {"username": username, "status": "ok", "storage_url": public_url}
        else:
            return {"username": username, "status": f"upload_failed_{upload_r.status_code}"}
    except Exception as e:
        return {"username": username, "status": f"error_{e}"}


def etapa2_avatars(results: list):
    """Etapa 2: download de avatars do CDN + upload pro Supabase Storage."""
    # Filtrar só quem tem avatar_url e foi ok no scraping
    with_avatar = [r for r in results if r.get("status") in ("ok", "retry_ok") and r.get("avatar_url")]

    log(f"\n{'='*60}")
    log(f"📸 ETAPA 2 — AVATARS ({len(with_avatar):,} imagens)")
    log(f"   {AVATAR_THREADS} threads direto no CDN do TikTok")
    log(f"{'='*60}\n")

    if not with_avatar:
        log("   ✨ Nenhum avatar pra processar!")
        return {}

    avatar_map = {}  # username → storage_url
    stats = {"ok": 0, "failed": 0}
    stats_lock = threading.Lock()
    start_time = datetime.now()
    total = len(with_avatar)

    with ThreadPoolExecutor(max_workers=AVATAR_THREADS) as executor:
        futures = {executor.submit(upload_avatar, item): item for item in with_avatar}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                with stats_lock:
                    if result["status"] == "ok":
                        stats["ok"] += 1
                        avatar_map[result["username"]] = result["storage_url"]
                    else:
                        stats["failed"] += 1

                    if i % LOG_EVERY == 0 or i == total:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = i / elapsed if elapsed > 0 else 0
                        eta_min = (total - i) / rate / 60 if rate > 0 else 0

                        log(f"📸 [{i:,}/{total:,}] {i/total*100:.1f}%")
                        log(f"   ✅ Salvos: {stats['ok']:,} | ❌ Falha: {stats['failed']:,}")
                        log(f"   ⚡ {rate:.1f}/s | ⏱️ ETA {eta_min:.0f}min\n")
            except Exception as e:
                log(f"   ⚠️ Exception: {e}")

    # Salvar mapa de avatars
    save_json(os.path.join(DATA_DIR, "avatar_map.json"), avatar_map)
    log(f"   ✅ Etapa 2 completa: {stats['ok']:,} avatars salvos")
    return avatar_map


# ============================================================
# ETAPA 3 — ATUALIZAR BANCO
# ============================================================

def etapa3_banco(results: list, avatar_map: dict):
    """Etapa 3: atualiza creators no banco + registra aliases."""
    ok_results = [r for r in results if r.get("status") in ("ok", "retry_ok") and r.get("creator_id")]

    log(f"\n{'='*60}")
    log(f"🗄️ ETAPA 3 — ATUALIZANDO BANCO ({len(ok_results):,} creators)")
    log(f"{'='*60}\n")

    if not ok_results:
        log("   ✨ Nada pra atualizar!")
        return

    stats = {"updated": 0, "alias": 0, "failed": 0}
    start_time = datetime.now()
    total = len(ok_results)

    for i, item in enumerate(ok_results, 1):
        username = item["username"]
        tiktok_id = item["creator_id"]

        try:
            # Update creators
            update_data = {"tiktok_id": tiktok_id}
            if item.get("nome"):
                update_data["creator_name"] = item["nome"]
            if item.get("seguidores") is not None:
                update_data["followers"] = item["seguidores"]
            if username in avatar_map:
                update_data["tiktok_avatar_url"] = avatar_map[username]

            r = requests.patch(
                f"{SUPABASE_URL}/rest/v1/creators",
                headers={**supabase_headers(), "Accept-Profile": "public"},
                params={"Creator username": f"eq.{username}"},
                json=update_data,
                timeout=15,
            )

            # Registrar alias
            r2 = requests.post(
                f"{SUPABASE_URL}/rest/v1/creator_aliases",
                headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
                json={
                    "tiktok_id": tiktok_id,
                    "username": username,
                    "is_current": True,
                    "first_seen_at": datetime.now().isoformat(),
                    "last_seen_at": datetime.now().isoformat(),
                },
                timeout=15,
            )

            stats["updated"] += 1
            stats["alias"] += 1
        except Exception:
            stats["failed"] += 1

        if i % LOG_EVERY == 0 or i == total:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = i / elapsed if elapsed > 0 else 0
            eta_min = (total - i) / rate / 60 if rate > 0 else 0

            log(f"🗄️ [{i:,}/{total:,}] {i/total*100:.1f}%")
            log(f"   ✅ Atualizados: {stats['updated']:,} | 🏷️ Aliases: {stats['alias']:,} | ❌ Falha: {stats['failed']:,}")
            log(f"   ⚡ {rate:.1f}/s | ⏱️ ETA {eta_min:.0f}min\n")

    log(f"   ✅ Etapa 3 completa: {stats['updated']:,} creators atualizados")


# ============================================================
# MAIN
# ============================================================

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY or not API_KEY:
        log("❌ Variáveis SUPABASE_URL, SUPABASE_SERVICE_KEY e API_KEY são obrigatórias!")
        sys.exit(1)

    os.makedirs(DATA_DIR, exist_ok=True)

    log(f"🚀 Backfill — 3 etapas com checkpoint")
    log(f"   📡 {WORKER_COUNT} workers × {THREADS_PER_WORKER} threads = {THREADS_PER_WORKER * WORKER_COUNT} simultâneas")
    log(f"   📸 {AVATAR_THREADS} threads pra avatars")
    log(f"   💾 Checkpoint: {DATA_DIR}")
    log("")

    # Verificar workers
    online = 0
    for i, w in enumerate(WORKERS, 1):
        try:
            r = requests.get(f"{w}/health", timeout=5)
            if r.status_code == 200:
                online += 1
                log(f"   ✅ w{i} online")
            else:
                log(f"   ⚠️ w{i} HTTP {r.status_code}")
        except Exception:
            log(f"   ❌ w{i} offline")
    log(f"\n   📡 Workers online: {online}/{WORKER_COUNT}\n")

    if online == 0:
        log("❌ Nenhum worker online!")
        sys.exit(1)

    # Buscar creators
    log("🔍 Buscando creators sem tiktok_id...")
    alunos = fetch_alunos_without_tiktok_id()
    log(f"   🎓 Alunos sem tiktok_id: {len(alunos):,}")

    alunos_set = set(alunos)
    ativos = fetch_ativos_2026_without_tiktok_id(alunos_set)
    log(f"   👤 Ativos 2026 sem tiktok_id: {len(ativos):,}")
    log(f"   📋 Total a processar: {len(alunos) + len(ativos):,}")

    if not alunos and not ativos:
        log("✅ Todos os creators já têm tiktok_id!")
        return

    start_time = datetime.now()

    # ETAPA 1 — Scraping (alunos primeiro, depois ativos)
    results = etapa1_scraping(alunos, ativos)

    # ETAPA 2 — Avatars
    avatar_map = etapa2_avatars(results)

    # ETAPA 3 — Banco
    etapa3_banco(results, avatar_map)

    # Relatório final
    duration = datetime.now() - start_time
    hours = int(duration.total_seconds() // 3600)
    mins = int((duration.total_seconds() % 3600) // 60)

    ok_count = sum(1 for r in results if r.get("status") in ("ok", "retry_ok"))
    nf_count = sum(1 for r in results if r.get("status") == "not_found")
    fail_count = sum(1 for r in results if r.get("status") == "scrape_failed")

    log(f"\n{'='*60}")
    log(f"🏁 BACKFILL COMPLETO — {hours}h {mins}min")
    log(f"{'='*60}")
    log(f"   📋 Total processados: {len(results):,}")
    log(f"   ✅ OK: {ok_count:,}")
    log(f"   👻 Não encontrado: {nf_count:,}")
    log(f"   💀 Falha scraping: {fail_count:,}")
    log(f"   📸 Avatars salvos: {len(avatar_map):,}")
    log(f"\n   Taxa de sucesso: {ok_count/len(results)*100:.1f}%" if results else "")
    log(f"🏁 Finalizado!")


if __name__ == "__main__":
    main()
