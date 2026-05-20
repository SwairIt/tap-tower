"""Long-polling бот для Tap Tower (СИНХРОННЫЙ) — пока нет публичного webhook-URL.

Все вызовы Telegram — sync httpx (async-путь через uvloop игнорирует наш
IPv4 socket-патч и таймаутит на проде). Когда появится PUBLIC_URL + webhook —
этот процесс останавливаем (polling и webhook взаимоисключающи).

Запуск:
    set -a; . ./.env; set +a
    .venv/bin/python -m backend.poll_bot
"""

from __future__ import annotations

import json
import time

import httpx

from backend.app import TG_API, process_update, tg_call


def main() -> None:
    # снимаем webhook, иначе getUpdates вернёт 409
    try:
        tg_call("deleteWebhook", {"drop_pending_updates": False})
    except Exception as e:  # noqa: BLE001
        print("deleteWebhook:", e, flush=True)

    offset = 0
    print("Tap Tower poller started", flush=True)
    with httpx.Client(timeout=60) as cl:
        while True:
            try:
                r = cl.get(
                    f"{TG_API}/getUpdates",
                    params={
                        "offset": offset,
                        "timeout": 50,
                        "allowed_updates": json.dumps(["message", "pre_checkout_query"]),
                    },
                )
                data = r.json()
                if not data.get("ok"):
                    print("getUpdates not ok:", data, flush=True)
                    time.sleep(3)
                    continue
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    try:
                        process_update(upd)
                    except Exception as e:  # noqa: BLE001
                        print("process_update error:", e, flush=True)
            except Exception as e:  # noqa: BLE001
                print("poll loop error:", e, flush=True)
                time.sleep(3)


if __name__ == "__main__":
    main()
