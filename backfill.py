"""
Backfill de tiktok_id e avatar para creators existentes.
3 etapas sequenciais com checkpoint em JSON no volume /data.

Etapa 1 — Scraping: rounds com rotação de workers (igual repo antigo)
Etapa 2 — Avatars: baixa imagens do CDN TikTok → upload pro Supabase Storage
Etapa 3 — Banco: atualiza creators + registra aliases

Threads por round: [20, 15, 10, 7, 4, 3, 2, 1]
Delay: 1-2s DEPOIS de cada request
Falhas do round N vão pro round N+1 em outro worker.

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
import random
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
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

THREADS_PER_ROUND = [20, 15, 10, 7, 4, 3, 2, 1]
TOTAL_ROUNDS = len(THREADS_PER_ROUND)
DELAY_MIN = 1.0
DELAY_MAX = 2.0
AVATAR_THREADS = 50
LOG_EVERY = 100

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
    """Busca creators ativos em 2026 sem tiktok_id, excluindo alunos."""
    outros = []
    offset = 0
    while True:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/get_active_creators_without_tiktok_id",
            headers={**supabase_headers(), "Accept": "application/json"},
            json={"p_limit": 1000, "p_offset": offset},
            timeout=120,
        )
        if r.status_code == 200:
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
# ETAPA 1 — SCRAPING POR ROUNDS
# ============================================================

def scrape_one_attempt(username: str, worker_url: str) -> dict:
    """Uma tentativa de scraping em um worker específico."""
    try:
        r = requests.get(
            f"{worker_url}/profile",
            params={"username": username},
            headers={"x-api-key": API_KEY},
            timeout=20,
        )

        # Delay DEPOIS da request (1-2s)
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        if r.status_code == 200:
            data = r.json()
            if "error" not in data:
                return {
                    "username": username,
                    "status": "ok",
                    "creator_id": data.get("creator_id", ""),
                    "nome": data.get("nome", ""),
                    "bio": data.get("bio", ""),
                    "avatar_url": data.get("avatar_url", ""),
                    "seguidores": data.get("seguidores", 0),
                }
            else:
                return {"username": username, "status": "not_found"}
        elif r.status_code == 404:
            return {"username": username, "status": "not_found"}
        else:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            return {"username": username, "status": f"error_{r.status_code}"}
    except Exception:
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        return {"username": username, "status": "error_connection"}


def process_round(round_num: int, usernames: list, worker_url: str, all_results: list):
    """Processa um round: tenta todos os usernames em um worker específico."""
    num_threads = THREADS_PER_ROUND[min(round_num, len(THREADS_PER_ROUND) - 1)]
    total = len(usernames)

    if total == 0:
        return [], []

    log(f"   📋 {total:,} usernames | {num_threads} threads | {worker_url.split('//')[1].split('.')[0]}")

    results = []
    failures = []
    lock = threading.Lock()
    processed = [0]
    round_start = datetime.now()

    def on_done(future):
        try:
            result = future.result()
        except Exception as e:
            log(f"   ⚠️ Thread exception: {e}")
            return

        with lock:
            processed[0] += 1
            pos = processed[0]
            username = result["username"]
            status = result["status"]

            if status == "ok":
                results.append(result)
                if round_num == 0:
                    if pos % LOG_EVERY == 0 or pos <= 5:
                        log(f"   ✅ [{pos}/{total}] @{result['nome']} | {result['seguidores']:,} seg")
                else:
                    log(f"   🎯 [{pos}/{total}] @{result['nome']} | {result['seguidores']:,} seg (recuperado round {round_num + 1})")
            elif status == "not_found":
                pass  # Conta não existe, não precisa retry
            else:
                failures.append(username)

            # Log de progresso
            if pos % LOG_EVERY == 0 and pos > 0:
                elapsed = (datetime.now() - round_start).total_seconds()
                rate = pos / elapsed if elapsed > 0 else 0
                eta = (total - pos) / rate / 60 if rate > 0 else 0
                total_results = len(all_results) + len(results)
                log(f"   💾 [{pos}/{total}] {pos/total*100:.0f}% — ✅ +{len(results)} (total {total_results:,}) | ❌ {len(failures)} | ⚡ {rate:.1f}/s | ⏱️ {eta:.0f}min")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for username in usernames:
            future = executor.submit(scrape_one_attempt, username, worker_url)
            future.add_done_callback(on_done)
            futures.append(future)

        for future in futures:
            future.result()

    return results, failures


def get_worker_for_round(base_worker_idx: int, round_num: int) -> str:
    """Qual worker este chunk usa neste round? Rotação circular."""
    idx = (base_worker_idx + round_num) % WORKER_COUNT
    return WORKERS[idx]


def etapa1_scraping(alunos: list, ativos: list):
    """Etapa 1: scraping por rounds — alunos primeiro, depois ativos."""
    total_geral = len(alunos) + len(ativos)

    log(f"\n{'='*60}")
    log(f"📡 ETAPA 1 — SCRAPING ({total_geral:,} creators)")
    log(f"   Threads por round: {THREADS_PER_ROUND}")
    log(f"   Delay: {DELAY_MIN}-{DELAY_MAX}s após cada request")
    log(f"   Ordem: 🎓 Alunos ({len(alunos):,}) → 👤 Ativos 2026 ({len(ativos):,})")
    log(f"{'='*60}")

    # Carregar progresso anterior
    progress = load_json(PROGRESS_FILE, {"completed": []})
    completed_set = set(progress.get("completed", []))
    all_results = load_json(RESULTS_FILE, [])

    # Processar cada grupo
    for group_label, group_creators in [("🎓 Alunos", alunos), ("👤 Ativos 2026", ativos)]:
        remaining = [c for c in group_creators if c not in completed_set]

        log(f"\n{'='*40}")
        log(f"   {group_label}: {len(group_creators):,} total, {len(remaining):,} restantes")
        log(f"{'='*40}")

        if not remaining:
            log(f"   ✨ {group_label} já completo!")
            continue

        # Dividir em chunks (1 por worker)
        chunk_size = len(remaining) // WORKER_COUNT
        chunks = {}
        for c in range(WORKER_COUNT):
            start_idx = c * chunk_size
            end_idx = len(remaining) if c == WORKER_COUNT - 1 else (c + 1) * chunk_size
            chunks[c] = remaining[start_idx:end_idx]

        # Rounds
        for round_num in range(TOTAL_ROUNDS):
            num_threads = THREADS_PER_ROUND[min(round_num, len(THREADS_PER_ROUND) - 1)]

            # Contar total de falhas restantes
            total_remaining = sum(len(chunks[c]) for c in range(WORKER_COUNT))
            if total_remaining == 0:
                log(f"\n   ✨ Todas as contas processadas no round {round_num}!")
                break

            log(f"\n   🔁 ROUND {round_num + 1}/{TOTAL_ROUNDS} — {num_threads} threads — {total_remaining:,} restantes")

            round_results = []
            new_chunks = {}

            for worker_idx in range(WORKER_COUNT):
                chunk = chunks[worker_idx]
                if not chunk:
                    new_chunks[worker_idx] = []
                    continue

                worker_url = get_worker_for_round(worker_idx, round_num)
                results, failures = process_round(round_num, chunk, worker_url, all_results)

                round_results.extend(results)
                # Falhas deste chunk vão pro próximo round no mesmo slot
                new_chunks[worker_idx] = failures

            # Acumular resultados
            for r in round_results:
                all_results.append(r)
                completed_set.add(r["username"])

            # Marcar not_found como completados também (não tentar de novo)
            # Já está feito pois not_found não vai pra failures

            # Rotacionar chunks: cada worker pega as falhas do anterior
            rotated_chunks = {}
            for worker_idx in range(WORKER_COUNT):
                prev_idx = (worker_idx - 1) % WORKER_COUNT
                rotated_chunks[worker_idx] = new_chunks[prev_idx]
            chunks = rotated_chunks

            # Salvar checkpoint
            save_json(RESULTS_FILE, all_results)
            save_json(PROGRESS_FILE, {"completed": list(completed_set)})

            ok_count = len(round_results)
            fail_count = sum(len(chunks[c]) for c in range(WORKER_COUNT))
            log(f"\n   📊 Round {round_num + 1}: ✅ {ok_count:,} OK | ❌ {fail_count:,} falhas restantes")

            if fail_count == 0:
                break

            log(f"   ⏱️ Pausa 5s antes do próximo round...")
            time.sleep(5)

    total_ok = sum(1 for r in all_results if r.get("status") == "ok")
    log(f"\n   ✅ Etapa 1 completa: {total_ok:,} perfis obtidos")
    return all_results


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
        img_r = requests.get(avatar_url, timeout=15)
        if img_r.status_code != 200:
            return {"username": username, "status": "download_failed"}

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
            public_url = f"{SUPABASE_URL}/storage/v1/object/public/creator-avatars/{storage_path}"
            return {"username": username, "status": "ok", "storage_url": public_url}
        else:
            return {"username": username, "status": f"upload_failed_{upload_r.status_code}"}
    except Exception as e:
        return {"username": username, "status": f"error_{e}"}


def etapa2_avatars(results: list):
    """Etapa 2: download de avatars do CDN + upload pro Supabase Storage."""
    with_avatar = [r for r in results if r.get("status") == "ok" and r.get("avatar_url")]

    log(f"\n{'='*60}")
    log(f"📸 ETAPA 2 — AVATARS ({len(with_avatar):,} imagens)")
    log(f"   {AVATAR_THREADS} threads direto no CDN do TikTok")
    log(f"{'='*60}\n")

    if not with_avatar:
        log("   ✨ Nenhum avatar pra processar!")
        return {}

    avatar_map = {}
    stats = {"ok": 0, "failed": 0}
    stats_lock = threading.Lock()
    start_time = datetime.now()
    total = len(with_avatar)

    with ThreadPoolExecutor(max_workers=AVATAR_THREADS) as executor:
        futures = {executor.submit(upload_avatar, item): item for item in with_avatar}

        from concurrent.futures import as_completed
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

    save_json(os.path.join(DATA_DIR, "avatar_map.json"), avatar_map)
    log(f"   ✅ Etapa 2 completa: {stats['ok']:,} avatars salvos")
    return avatar_map


# ============================================================
# ETAPA 3 — ATUALIZAR BANCO
# ============================================================

def etapa3_banco(results: list, avatar_map: dict):
    """Etapa 3: atualiza creators no banco + registra aliases."""
    ok_results = [r for r in results if r.get("status") == "ok" and r.get("creator_id")]

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
            update_data = {"tiktok_id": tiktok_id}
            if item.get("nome"):
                update_data["creator_name"] = item["nome"]
            if item.get("seguidores") is not None:
                update_data["followers"] = item["seguidores"]
            if username in avatar_map:
                update_data["tiktok_avatar_url"] = avatar_map[username]

            requests.patch(
                f"{SUPABASE_URL}/rest/v1/creators",
                headers={**supabase_headers(), "Accept-Profile": "public"},
                params={"Creator username": f"eq.{username}"},
                json=update_data,
                timeout=15,
            )

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

    log(f"🚀 Backfill — 3 etapas com rounds + checkpoint")
    log(f"   📡 {WORKER_COUNT} workers")
    log(f"   🔄 Threads por round: {THREADS_PER_ROUND}")
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

    ok_count = sum(1 for r in results if r.get("status") == "ok")
    nf_count = sum(1 for r in results if r.get("status") == "not_found")
    fail_count = sum(1 for r in results if r.get("status", "").startswith("error"))

    log(f"\n{'='*60}")
    log(f"🏁 BACKFILL COMPLETO — {hours}h {mins}min")
    log(f"{'='*60}")
    log(f"   📋 Total processados: {len(results):,}")
    log(f"   ✅ OK: {ok_count:,}")
    log(f"   👻 Não encontrado: {nf_count:,}")
    log(f"   💀 Falha scraping: {fail_count:,}")
    log(f"   📸 Avatars salvos: {len(avatar_map):,}")
    if results:
        log(f"\n   Taxa de sucesso: {ok_count/len(results)*100:.1f}%")
    log(f"🏁 Finalizado!")


if __name__ == "__main__":
    main()
