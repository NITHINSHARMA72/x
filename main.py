import os
import asyncio
import logging
import random
import threading
from flask import Flask, request, jsonify

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

# ================= CONFIG =================

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

SESSION = "forwarder_session"

FORWARD_DELAY = (3, 7)
BATCH_SIZE = 10
BATCH_DELAY = 25

MAX_RETRY = 3
BACKOFF_BASE = 5

PORT = int(os.getenv("PORT", 10000))

# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("FORWARDER")

# ================= TELEGRAM =================

client = TelegramClient(SESSION, API_ID, API_HASH)

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

queue = asyncio.Queue()

# ================= FLASK =================

app = Flask(__name__)

# ================= HELPERS =================

async def login():
    await client.connect()

    if not await client.is_user_authorized():
        raise RuntimeError(
            "❌ Session not authorized. Upload forwarder_session.session file."
        )

    log.info("✅ Telegram login success")

async def get_groups():
    groups = []
    async for d in client.iter_dialogs():
        if d.is_group or d.is_channel:
            if not getattr(d.entity, "broadcast", False):
                groups.append(d)
    return groups

async def forward_message(message, target_chat_id):
    for attempt in range(MAX_RETRY):
        try:
            await asyncio.sleep(random.uniform(*FORWARD_DELAY))

            await client.forward_messages(
                target_chat_id,
                message["id"],
                from_peer=message["from"]
            )

            return True, "OK"

        except FloodWaitError as e:
            wait = e.seconds + BACKOFF_BASE * (2 ** attempt)
            log.warning(f"FloodWait {wait}s")
            await asyncio.sleep(wait)

        except ChatWriteForbiddenError:
            return False, "WRITE_FORBIDDEN"

        except Exception as e:
            log.error(str(e))
            await asyncio.sleep(BACKOFF_BASE * (2 ** attempt))

    return False, "FAILED"

async def worker():
    while True:
        item = await queue.get()

        message = {
            "id": item["message_id"],
            "from": item["from_chat"]
        }

        groups = await get_groups()
        results = []

        for i in range(0, len(groups), BATCH_SIZE):
            batch = groups[i:i + BATCH_SIZE]

            for g in batch:
                ok, err = await forward_message(message, g.id)
                results.append(f"{g.title} → {ok}")

            await asyncio.sleep(BATCH_DELAY)

        report = "\n".join(results)
        await client.send_message("me", f"📊 Forward Report\n\n{report}")

        queue.task_done()

# ================= WEBHOOK =================

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json

    if not data or "from_chat" not in data or "message_id" not in data:
        return jsonify({"error": "from_chat and message_id required"}), 400

    asyncio.run_coroutine_threadsafe(queue.put(data), loop)
    return jsonify({"status": "queued"}), 200

# ================= START =================

def start():
    loop.run_until_complete(login())
    loop.create_task(worker())
    loop.run_forever()

threading.Thread(target=start, daemon=True).start()

# ================= MAIN =================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
