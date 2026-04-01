"""
Backfill Coordenador — envia listas pros workers e coleta resultados.

Fluxo:
  1. Busca creators no Supabase
  2. Divide em chunks (1 por worker)
  3. Manda cada chunk pro worker via POST /batch
  4. Workers processam em background (scraping direto no TikTok)
  5. Coordenador faz polling no /batch/status até todos terminarem
  6. Coleta resultados via GET /batch/results
  7. Rounds: falhas do w1 vão pro w2, falhas do w2 pro w3, etc.
  8. Baixa avatars do CDN e faz upload pro Supabase Storage
  9. Atualiza banco (tiktok_id, avatar, aliases)

Processa ALUNOS primeiro (3 etapas completas), depois ATIVOS 2026.

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

THREADS_PER_ROUND = [20, 15, 10, 7, 4, 3, 2, 1]
TOTAL_ROUNDS = len(THREADS_PER_ROUND)
AVATAR_THREADS = 50
LOG_EVERY = 100
POLL_INTERVAL = 5  # segundos entre polls de status


# ============================================================
# HELPERS
# ============================================================

def log(msg: str):
    print(msg, flush=True)


def worker_name(idx):
    return f"w{idx + 1}"


def worker_url(idx):
    return WORKERS[idx]


def api_headers():
    return {"x-api-key": API_KEY}


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
# FETCH CREATORS
# ============================================================

def fetch_alunos_without_tiktok_id():
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
# SCRAPING POR ROUNDS
# ============================================================

def get_worker_for_round(base_idx, round_num):
    return (base_idx + round_num) % WORKER_COUNT


def send_batch_to_worker(w_idx, usernames, num_threads, round_num):
    """Envia lista de usernames pro worker processar."""
    wname = worker_name(w_idx)
    url = worker_url(w_idx)

    try:
        r = requests.post(
            f"{url}/batch",
            headers={**api_headers(), "Content-Type": "application/json"},
            json={"usernames": usernames, "threads": num_threads, "round": round_num},
            timeout=30,
        )
        if r.status_code == 200:
            log(f"   📤 Coordenador enviou {len(usernames):,} contas para {wname}")
            return True
        else:
            log(f"   ❌ {wname} rejeitou o batch (HTTP {r.status_code}): {r.text[:100]}")
            return False
    except Exception as e:
        log(f"   ❌ {wname} não respondeu: {e}")
        return False


def poll_worker_status(w_idx):
    """Faz polling no worker até terminar."""
    wname = worker_name(w_idx)
    url = worker_url(w_idx)
    last_processed = 0

    while True:
        try:
            r = requests.get(f"{url}/batch/status", headers=api_headers(), timeout=10)
            if r.status_code == 200:
                data = r.json()
                status = data["status"]
                processed = data["processed"]
                total = data["total"]
                ok = data["ok"]
                failed = data["failed"]

                if processed != last_processed and processed % LOG_EVERY < POLL_INTERVAL:
                    log(f"   ⏳ {wname}: [{processed}/{total}] ✅ {ok} | ❌ {failed}")
                    last_processed = processed

                if status == "done":
                    log(f"   ✅ {wname} terminou: {ok} OK, {data['not_found']} NF, {failed} falhas")
                    return True
        except Exception:
            pass

        time.sleep(POLL_INTERVAL)


def collect_worker_results(w_idx):
    """Coleta resultados do worker."""
    url = worker_url(w_idx)

    try:
        r = requests.get(f"{url}/batch/results", headers=api_headers(), timeout=30)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"results": [], "failures": [], "ok": 0, "not_found": 0, "failed": 0}


def run_scraping(label, creators, results_file, progress_file, max_rounds=None):
    """Scraping por rounds com todos os workers em paralelo."""
    if max_rounds is None:
        max_rounds = TOTAL_ROUNDS

    progress = load_json(progress_file, {"completed": []})
    completed_set = set(progress.get("completed", []))
    all_results = load_json(results_file, [])

    already_done = len(completed_set)
    remaining = [c for c in creators if c not in completed_set]

    log(f"\n{'='*60}")
    log(f"📡 SCRAPING — {label} ({len(creators):,} total)")
    log(f"   ✅ Já processados: {already_done:,}")
    log(f"   📋 Restantes: {len(remaining):,}")
    log(f"   🔄 Rounds: {max_rounds} | Threads: {THREADS_PER_ROUND[:max_rounds]}")
    log(f"   📡 {WORKER_COUNT} workers em paralelo")
    log(f"{'='*60}")

    if not remaining:
        log(f"   ✨ {label} já completo!")
        return all_results

    # Dividir em chunks
    chunk_size = len(remaining) // WORKER_COUNT
    chunks = {}
    for c in range(WORKER_COUNT):
        start = c * chunk_size
        end = len(remaining) if c == WORKER_COUNT - 1 else (c + 1) * chunk_size
        chunks[c] = remaining[start:end]

    for round_num in range(max_rounds):
        num_threads = THREADS_PER_ROUND[min(round_num, len(THREADS_PER_ROUND) - 1)]
        total_remaining = sum(len(chunks[c]) for c in range(WORKER_COUNT))

        if total_remaining == 0:
            log(f"\n   ✨ Todas as contas processadas!")
            break

        log(f"\n   🔁 ROUND {round_num + 1}/{max_rounds} — {num_threads} threads/worker — {total_remaining:,} restantes")

        # 1. Enviar batches para todos os workers
        log(f"\n   📤 Coordenador distribuindo contas para os workers...")
        active_workers = []
        for base_idx in range(WORKER_COUNT):
            chunk = chunks[base_idx]
            if not chunk:
                continue
            w_idx = get_worker_for_round(base_idx, round_num)
            success = send_batch_to_worker(w_idx, chunk, num_threads, round_num)
            if success:
                active_workers.append((base_idx, w_idx))

        if not active_workers:
            log(f"   ❌ Nenhum worker aceitou o batch!")
            break

        # 2. Esperar todos os workers terminarem (polling em paralelo)
        log(f"\n   ⏳ Coordenador aguardando {len(active_workers)} workers terminarem...")

        with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
            poll_futures = {executor.submit(poll_worker_status, w_idx): (base_idx, w_idx)
                           for base_idx, w_idx in active_workers}
            for future in as_completed(poll_futures):
                future.result()

        # 3. Coletar resultados de todos os workers
        log(f"\n   📥 Coordenador coletando resultados...")
        round_results = []
        new_chunks = {}

        for base_idx, w_idx in active_workers:
            wname = worker_name(w_idx)
            data = collect_worker_results(w_idx)
            results = data.get("results", [])
            failures = data.get("failures", [])

            log(f"   📥 {wname} entregou {len(results):,} perfis e {len(failures):,} falhas")

            round_results.extend(results)
            new_chunks[base_idx] = failures

        # Preencher chunks vazios
        for base_idx in range(WORKER_COUNT):
            if base_idx not in new_chunks:
                new_chunks[base_idx] = []

        # Acumular resultados
        for r in round_results:
            all_results.append(r)
            completed_set.add(r["username"])

        # Rotacionar chunks: cada slot pega as falhas do anterior
        rotated = {}
        for idx in range(WORKER_COUNT):
            prev = (idx - 1) % WORKER_COUNT
            rotated[idx] = new_chunks.get(prev, [])
        chunks = rotated

        # Checkpoint
        save_json(results_file, all_results)
        save_json(progress_file, {"completed": list(completed_set)})

        ok_count = len(round_results)
        fail_count = sum(len(chunks[c]) for c in range(WORKER_COUNT))
        log(f"\n   📊 Round {round_num + 1}: ✅ {ok_count:,} OK | ❌ {fail_count:,} falhas restantes")

        if fail_count == 0:
            break

        log(f"   ⏱️ Pausa 5s antes do próximo round...")
        time.sleep(5)

    total_ok = sum(1 for r in all_results if r.get("status") == "ok")
    log(f"\n   ✅ Scraping {label} completo: {total_ok:,} perfis obtidos")
    return all_results


# ============================================================
# AVATARS
# ============================================================

def upload_avatar(item: dict) -> dict:
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


def run_avatars(label, results):
    with_avatar = [r for r in results if r.get("status") == "ok" and r.get("avatar_url")]

    log(f"\n{'='*60}")
    log(f"📸 AVATARS — {label} ({len(with_avatar):,} imagens)")
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

                        log(f"   📸 [{i:,}/{total:,}] {i/total*100:.1f}%")
                        log(f"      ✅ Salvos: {stats['ok']:,} | ❌ Falha: {stats['failed']:,}")
                        log(f"      ⚡ {rate:.1f}/s | ⏱️ ETA {eta_min:.0f}min\n")
            except Exception as e:
                log(f"   ⚠️ Exception: {e}")

    log(f"   ✅ Avatars {label}: {stats['ok']:,} salvos")
    return avatar_map


# ============================================================
# BANCO
# ============================================================

def run_banco(label, results, avatar_map):
    ok_results = [r for r in results if r.get("status") == "ok" and r.get("creator_id")]

    log(f"\n{'='*60}")
    log(f"🗄️ BANCO — {label} ({len(ok_results):,} creators)")
    log(f"{'='*60}\n")

    if not ok_results:
        log("   ✨ Nada pra atualizar!")
        return

    stats = {"updated": 0, "failed": 0}
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
        except Exception:
            stats["failed"] += 1

        if i % LOG_EVERY == 0 or i == total:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = i / elapsed if elapsed > 0 else 0
            eta_min = (total - i) / rate / 60 if rate > 0 else 0

            log(f"   🗄️ [{i:,}/{total:,}] {i/total*100:.1f}%")
            log(f"      ✅ {stats['updated']:,} atualizados | ❌ {stats['failed']:,} falhas")
            log(f"      ⚡ {rate:.1f}/s | ⏱️ ETA {eta_min:.0f}min\n")

    log(f"   ✅ Banco {label}: {stats['updated']:,} atualizados")


# ============================================================
# PROCESSAR GRUPO (3 etapas)
# ============================================================

def process_group(label, creators, file_prefix, max_rounds=None):
    results_file = os.path.join(DATA_DIR, f"{file_prefix}_results.json")
    progress_file = os.path.join(DATA_DIR, f"{file_prefix}_progress.json")

    log(f"\n{'='*60}")
    log(f"🚀 PROCESSANDO: {label} ({len(creators):,} creators)")
    log(f"{'='*60}")

    start_time = datetime.now()

    results = run_scraping(label, creators, results_file, progress_file, max_rounds=max_rounds)
    avatar_map = run_avatars(label, results)
    save_json(os.path.join(DATA_DIR, f"{file_prefix}_avatars.json"), avatar_map)
    run_banco(label, results, avatar_map)

    duration = datetime.now() - start_time
    mins = int(duration.total_seconds() // 60)
    ok_count = sum(1 for r in results if r.get("status") == "ok")

    log(f"\n{'='*60}")
    log(f"✅ {label} COMPLETO — {mins}min | ✅ {ok_count:,} OK | 📸 {len(avatar_map):,} avatars")
    log(f"{'='*60}\n")

    return results, avatar_map


# ============================================================
# MAIN
# ============================================================

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY or not API_KEY:
        log("❌ Variáveis SUPABASE_URL, SUPABASE_SERVICE_KEY e API_KEY são obrigatórias!")
        sys.exit(1)

    os.makedirs(DATA_DIR, exist_ok=True)

    log(f"🎯 Coordenador de Backfill")
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

    global_start = datetime.now()

    # GRUPO 1: ALUNOS
    log("🔍 Buscando alunos sem tiktok_id...")
    alunos = fetch_alunos_without_tiktok_id()
    log(f"   🎓 Alunos: {len(alunos):,}")

    if alunos:
        process_group("🎓 Alunos", alunos, "alunos", max_rounds=1)
    else:
        log("   ✨ Todos os alunos já têm tiktok_id!")

    # GRUPO 2: ATIVOS 2026
    log("\n🔍 Buscando creators ativos 2026...")
    alunos_set = set(alunos) if alunos else set()
    ativos = fetch_ativos_2026_without_tiktok_id(alunos_set)
    log(f"   👤 Ativos 2026: {len(ativos):,}")

    if ativos:
        process_group("👤 Ativos 2026", ativos, "ativos", max_rounds=3)
    else:
        log("   ✨ Todos os ativos já têm tiktok_id!")

    duration = datetime.now() - global_start
    hours = int(duration.total_seconds() // 3600)
    mins = int((duration.total_seconds() % 3600) // 60)

    log(f"\n{'='*60}")
    log(f"🏁 BACKFILL COMPLETO — {hours}h {mins}min")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
