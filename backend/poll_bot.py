"""Long-polling бот для Tap Tower — пока нет публичного webhook-URL.

Зовёт getUpdates и отдаёт апдейты в общий backend.app.process_update
(тот же код, что у webhook). Когда появится PUBLIC_URL + webhook — этот
процесс останавливаем (polling и webhook взаимоисключающи).

Запуск:
    set -a; . ./.env; set +a
    .venv/bin/python -m backend.poll_bot
"""

from __future__ import annotations

import asyncio
import json

import httpx

from backend.app import TG_API, process_update


async def main() -> None:
    async with httpx.AsyncClient(timeout=70) as cl:
        # снимаем webhook, иначе getUpdates вернёт 409
        try:
            await cl.post(f"{TG_API}/deleteWebhook", json={"drop_pending_updates": False})
        except Exception as e:  # noqa: BLE001
            print("deleteWebhook:", e)

        offset = 0
        print("Tap Tower poller started")
        while True:
            try:
                r = await cl.get(
                    f"{TG_API}/getUpdates",
                    params={
                        "offset": offset,
                        "timeout": 50,
                        "allowed_updates": json.dumps(["message", "pre_checkout_query"]),
                    },
                )
                data = r.json()
                if not data.get("ok"):
                    print("getUpdates not ok:", data)
                    await asyncio.sleep(3)
                    continue
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    try:
                        await process_update(upd)
                    except Exception as e:  # noqa: BLE001
                        print("process_update error:", e)
            except Exception as e:  # noqa: BLE001
                print("poll loop error:", e)
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
