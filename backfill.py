"""
Backfill de tiktok_id e avatar para creators existentes.
Roda uma vez, preenche todos os creators sem tiktok_id.

Prioridade: alunos primeiro, depois creators ativos em 2026.
Failover rotacional: cada creator tenta w1→w2→w3→...→w10 até dar certo.
Threads: 20 por worker = 200 simultâneas.

Variáveis de ambiente (Railway):
  SUPABASE_URL          → URL do projeto Supabase
  SUPABASE_SERVICE_KEY  → Service role key
  API_KEY               → Chave da API dos workers
  WORKER_COUNT          → Quantidade de workers (default: 10)
"""

import requests
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

WORKERS = [f"https://w{i}.api.mvmcreators.com.br" for i in range(1, WORKER_COUNT + 1)]

THREADS_PER_WORKER = 20
LOG_EVERY = 200


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


# ============================================================
# FETCH CREATORS SEM TIKTOK_ID
# ============================================================

def fetch_creators_without_tiktok_id():
    """Busca todos os creators sem tiktok_id: alunos primeiro, depois o resto."""
    log("🔍 Buscando creators sem tiktok_id...")

    # 1. Alunos (prioridade)
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

    log(f"   🎓 Alunos sem tiktok_id: {len(alunos):,}")

    # 2. Outros creators sem tiktok_id
    outros = []
    offset = 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/creators",
            headers={**supabase_headers(), "Accept": "application/json", "Accept-Profile": "public"},
            params={
                "select": '"Creator username"',
                "tiktok_id": "is.null",
                "discord_id": "is.null",
                "limit": 1000,
                "offset": offset,
            },
            timeout=30,
        )
        rows = r.json()
        if not rows:
            break
        outros.extend([row["Creator username"] for row in rows])
        offset += 1000

    log(f"   👤 Outros creators sem tiktok_id: {len(outros):,}")

    # Alunos primeiro, depois o resto (deduplicar)
    seen = set()
    unique = []
    for c in alunos + outros:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    log(f"   📋 Total a processar: {len(unique):,}")
    return unique


# ============================================================
# WORKER ASSIGNMENT
# ============================================================

assign_lock = threading.Lock()
assign_counter = [0]


def get_initial_worker_index():
    """Round-robin: cada creator começa em um worker diferente."""
    with assign_lock:
        idx = assign_counter[0] % WORKER_COUNT
        assign_counter[0] += 1
        return idx


# ============================================================
# SCRAPE + UPLOAD AVATAR
# ============================================================

def scrape_and_save(username: str) -> dict:
    """Faz scraping com failover rotacional. Tenta TODOS os workers independente do erro."""
    start_idx = get_initial_worker_index()

    profile = None
    first_worker = start_idx + 1  # w1-indexed para log
    success_worker = None
    retried = False

    for attempt in range(WORKER_COUNT):
        worker_idx = (start_idx + attempt) % WORKER_COUNT
        worker_url = WORKERS[worker_idx]

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
                    profile = data
                    success_worker = worker_idx + 1
                    if attempt > 0:
                        retried = True
                    break
            elif r.status_code == 404:
                return {
                    "username": username,
                    "status": "not_found",
                    "worker": f"w{worker_idx + 1}",
                }
        except Exception:
            pass

        # Qualquer erro → tenta próximo worker
        if attempt > 0:
            retried = True

    if not profile:
        return {"username": username, "status": "scrape_failed", "worker": f"w{first_worker}"}

    tiktok_id = profile.get("creator_id", "")
    if not tiktok_id:
        return {"username": username, "status": "no_creator_id", "worker": f"w{success_worker}"}

    # Download e upload do avatar
    avatar_url = profile.get("avatar_url", "")
    final_avatar_url = None

    if avatar_url:
        try:
            img_r = requests.get(avatar_url, timeout=15)
            if img_r.status_code == 200:
                img_data = img_r.content
                storage_path = f"{username}.webp"

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
                    final_avatar_url = f"{SUPABASE_URL}/storage/v1/object/public/creator-avatars/{storage_path}"
        except Exception:
            pass

    # Update no banco: creators
    update_data = {"tiktok_id": tiktok_id}
    if profile.get("nome"):
        update_data["creator_name"] = profile["nome"]
    if profile.get("seguidores") is not None:
        update_data["followers"] = profile["seguidores"]
    if final_avatar_url:
        update_data["tiktok_avatar_url"] = final_avatar_url

    requests.patch(
        f"{SUPABASE_URL}/rest/v1/creators",
        headers={**supabase_headers(), "Accept-Profile": "public"},
        params={"Creator username": f"eq.{username}"},
        json=update_data,
        timeout=15,
    )

    # Registrar alias
    requests.post(
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

    return {
        "username": username,
        "status": "retry_ok" if retried else "ok",
        "tiktok_id": tiktok_id,
        "avatar": "yes" if final_avatar_url else "no",
        "worker": f"w{success_worker}",
    }


# ============================================================
# MAIN
# ============================================================

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY or not API_KEY:
        log("❌ Variaveis SUPABASE_URL, SUPABASE_SERVICE_KEY e API_KEY sao obrigatorias!")
        sys.exit(1)

    max_threads = THREADS_PER_WORKER * WORKER_COUNT

    log(f"🚀 Backfill — {WORKER_COUNT} workers × {THREADS_PER_WORKER} threads = {max_threads} simultâneas")
    log(f"🔄 Failover: cada creator tenta todos os {WORKER_COUNT} workers em rotação")
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
    log(f"\n   📡 Workers online: {online}/{WORKER_COUNT}")

    if online == 0:
        log("❌ Nenhum worker online!")
        sys.exit(1)

    log("")

    # Buscar creators
    creators = fetch_creators_without_tiktok_id()
    total = len(creators)

    if total == 0:
        log("✅ Todos os creators já têm tiktok_id!")
        return

    # Stats
    stats = {
        "ok": 0,
        "retry_ok": 0,
        "not_found": 0,
        "scrape_failed": 0,
        "no_creator_id": 0,
        "avatar_ok": 0,
    }
    stats_lock = threading.Lock()
    start_time = datetime.now()

    log(f"\n{'='*60}")
    log(f"🔄 Processando {total:,} creators ({max_threads} threads)")
    log(f"{'='*60}\n")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(scrape_and_save, c): c for c in creators}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                with stats_lock:
                    status = result.get("status", "unknown")
                    worker = result.get("worker", "?")

                    if status == "ok":
                        stats["ok"] += 1
                    elif status == "retry_ok":
                        stats["retry_ok"] += 1
                    elif status in stats:
                        stats[status] += 1

                    if status in ("ok", "retry_ok") and result.get("avatar") == "yes":
                        stats["avatar_ok"] += 1

                    if i % LOG_EVERY == 0 or i == total:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = i / elapsed if elapsed > 0 else 0
                        eta_min = (total - i) / rate / 60 if rate > 0 else 0

                        total_ok = stats["ok"] + stats["retry_ok"]
                        total_err = stats["not_found"] + stats["scrape_failed"] + stats["no_creator_id"]

                        log(f"📊 [{i:,}/{total:,}] {i/total*100:.1f}%")
                        log(f"   ✅ OK: {stats['ok']:,}")
                        log(f"   ❌ Erros: {total_err:,} (👻 {stats['not_found']:,} NF | 💀 {stats['scrape_failed']:,} fail | 🆔 {stats['no_creator_id']:,} sem ID)")
                        log(f"   🔄 Retry OK: {stats['retry_ok']:,}")
                        log(f"   📸 Avatars: {stats['avatar_ok']:,}")
                        log(f"   ⚡ {rate:.1f}/s | ⏱️ ETA {eta_min:.0f}min")
                        log("")

            except Exception as e:
                log(f"   ⚠️ Exception: {e}")

    # Relatório final
    duration = datetime.now() - start_time
    hours = int(duration.total_seconds() // 3600)
    mins = int((duration.total_seconds() % 3600) // 60)

    total_ok = stats["ok"] + stats["retry_ok"]
    total_err = stats["not_found"] + stats["scrape_failed"] + stats["no_creator_id"]

    log(f"{'='*60}")
    log(f"🏁 BACKFILL COMPLETO — {hours}h {mins}min")
    log(f"{'='*60}")
    log(f"   📋 Total processados: {total:,}")
    log(f"   ✅ OK: {stats['ok']:,}")
    log(f"   🔄 Retry OK: {stats['retry_ok']:,}")
    log(f"   ❌ Erros: {total_err:,}")
    log(f"      👻 Não encontrado: {stats['not_found']:,}")
    log(f"      💀 Falha scraping: {stats['scrape_failed']:,}")
    log(f"      🆔 Sem creator_id: {stats['no_creator_id']:,}")
    log(f"   📸 Avatars salvos: {stats['avatar_ok']:,}")
    log(f"")
    log(f"   Taxa de sucesso: {total_ok/total*100:.1f}%")
    log(f"🏁 Finalizado!")


if __name__ == "__main__":
    main()
