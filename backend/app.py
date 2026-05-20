"""Tap Tower backend — FastAPI.

Отдаёт Mini App (статику), хранит лидерборд (SQLite), верифицирует Telegram
initData (HMAC), создаёт Stars-инвойсы (createInvoiceLink, currency=XTR, без
платёжного провайдера), обрабатывает webhook оплат (pre_checkout / successful_payment).

Запуск:
    uv run uvicorn backend.app:app --host 127.0.0.1 --port 8099 --reload
или:
    pip install -r backend/requirements.txt && uvicorn backend.app:app

Переменные окружения (.env в корне tap-tower):
    TELEGRAM_BOT_TOKEN   — токен бота (обязателен)
    PUBLIC_URL           — публичный https-URL Mini App (для setMenuButton/webhook)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

# ── Прод-VPS getdoday.ru отдаёт только AAAA для api.telegram.org, а из трёх DC
# routable лишь один IPv4. Тот же фикс, что в боте Doday. Включается флагом
# FORCE_TG_IPV4=1 (только на проде); локально обычное разрешение имён. ──
if os.environ.get("FORCE_TG_IPV4") == "1":
    _TG_IPV4 = ("149.154.167.220",)
    _orig_getaddrinfo = socket.getaddrinfo

    def _ipv4_only(host, *args, **kwargs):  # type: ignore[no-untyped-def]
        if host == "api.telegram.org":
            port = args[0] if args else kwargs.get("port", 443)
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))
                for ip in _TG_IPV4
            ]
        return _orig_getaddrinfo(host, *args, **kwargs)

    socket.getaddrinfo = _ipv4_only  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "backend" / "taptower.db"
INDEX_HTML = ROOT / "index.html"

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
if not BOT_TOKEN:
    # читаем .env вручную (без зависимости от python-dotenv)
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("TELEGRAM_BOT_TOKEN="):
                BOT_TOKEN = line.split("=", 1)[1].strip()

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

app = FastAPI(title="Tap Tower API")


# ───────────────────────── DB ─────────────────────────
@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS scores (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                first_name TEXT,
                best       INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS purchases (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                item        TEXT NOT NULL,
                stars       INTEGER NOT NULL,
                charge_id   TEXT,
                created_at  INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_scores_best ON scores(best DESC);
            """
        )


init_db()


# ──────────────────── Telegram initData ────────────────────
def verify_init_data(init_data: str) -> dict[str, Any] | None:
    """Проверяет подпись Telegram WebApp initData. Возвращает распарсенные поля
    (включая user как dict) или None если подпись невалидна."""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        pairs = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        return None
    received = pairs.pop("hash", None)
    if not received:
        return None
    data_check = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calc = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, received):
        return None
    # свежесть (не старше 24ч) — защита от replay
    try:
        if time.time() - int(pairs.get("auth_date", "0")) > 86400:
            return None
    except ValueError:
        return None
    if "user" in pairs:
        try:
            pairs["user"] = json.loads(pairs["user"])
        except json.JSONDecodeError:
            pairs["user"] = {}
    return pairs


# ──────────────────────── Schemas ────────────────────────
class ScoreIn(BaseModel):
    init_data: str = Field(min_length=1)
    score: int = Field(ge=0, le=100000)


class InvoiceIn(BaseModel):
    init_data: str = Field(min_length=1)
    item: str = Field(default="continue")  # 'continue' | 'skin:<id>'


# ──────────────────────── Routes ────────────────────────
@app.get("/")
async def index() -> FileResponse:
    return FileResponse(INDEX_HTML)


@app.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@app.post("/api/score")
async def submit_score(body: ScoreIn) -> JSONResponse:
    data = verify_init_data(body.init_data)
    if not data or "user" not in data:
        return JSONResponse({"error": "bad_init_data"}, status_code=401)
    u = data["user"]
    uid = int(u["id"])
    with db() as c:
        row = c.execute("SELECT best FROM scores WHERE user_id=?", (uid,)).fetchone()
        prev = row["best"] if row else 0
        best = max(prev, body.score)
        c.execute(
            """INSERT INTO scores(user_id, username, first_name, best, updated_at)
               VALUES(?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 best=excluded.best, username=excluded.username,
                 first_name=excluded.first_name, updated_at=excluded.updated_at""",
            (uid, u.get("username"), u.get("first_name"), best, int(time.time())),
        )
    return JSONResponse({"best": best, "submitted": body.score})


@app.get("/api/leaderboard")
async def leaderboard(limit: int = 20) -> JSONResponse:
    limit = max(1, min(limit, 100))
    with db() as c:
        rows = c.execute(
            "SELECT username, first_name, best FROM scores ORDER BY best DESC, updated_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
    top = [
        {"name": r["first_name"] or r["username"] or "Игрок", "best": r["best"]}
        for r in rows
    ]
    return JSONResponse({"top": top})


@app.post("/api/invoice")
async def create_invoice(body: InvoiceIn) -> JSONResponse:
    """Создаёт ссылку на Stars-инвойс. Для XTR провайдер не нужен."""
    data = verify_init_data(body.init_data)
    if not data or "user" not in data:
        return JSONResponse({"error": "bad_init_data"}, status_code=401)
    uid = int(data["user"]["id"])

    catalog = {
        "continue": ("Продолжить игру", "Продолжить с текущей высоты", 1),
        "skin:neon": ("Скин «Неон»", "Неоновая тема башни навсегда", 50),
        "skin:gold": ("Скин «Золото»", "Золотая тема башни навсегда", 75),
    }
    if body.item not in catalog:
        return JSONResponse({"error": "unknown_item"}, status_code=400)
    title, desc, stars = catalog[body.item]
    payload = f"{body.item}:{uid}:{int(time.time())}"

    async with httpx.AsyncClient(timeout=15) as cl:
        r = await cl.post(
            f"{TG_API}/createInvoiceLink",
            json={
                "title": title,
                "description": desc,
                "payload": payload,
                "currency": "XTR",  # Telegram Stars
                "prices": [{"label": title, "amount": stars}],
            },
        )
    j = r.json()
    if not j.get("ok"):
        return JSONResponse({"error": "tg_error", "detail": j}, status_code=502)
    return JSONResponse({"link": j["result"], "stars": stars})


async def process_update(update: dict[str, Any]) -> None:
    """Обработка одного Telegram-апдейта. Используется и webhook'ом, и поллером."""
    # 1) pre_checkout — обязательно ответить ok в течение 10с
    pcq = update.get("pre_checkout_query")
    if pcq:
        async with httpx.AsyncClient(timeout=15) as cl:
            await cl.post(
                f"{TG_API}/answerPreCheckoutQuery",
                json={"pre_checkout_query_id": pcq["id"], "ok": True},
            )
        return

    msg = update.get("message") or {}

    # 2) successful_payment — фиксируем покупку
    sp = msg.get("successful_payment")
    if sp:
        uid = int(msg["from"]["id"])
        payload = sp.get("invoice_payload", "")
        item = payload.split(":")[0] if payload else "?"
        with db() as c:
            c.execute(
                "INSERT INTO purchases(user_id, item, stars, charge_id, created_at) VALUES(?,?,?,?,?)",
                (uid, item, sp.get("total_amount", 0),
                 sp.get("telegram_payment_charge_id"), int(time.time())),
            )
        return

    # 3) /start — приветствие + кнопка запуска Mini App (если есть PUBLIC_URL)
    text = msg.get("text", "")
    if text.startswith("/start") or text.startswith("/play"):
        chat_id = msg["chat"]["id"]
        public = os.environ.get("PUBLIC_URL", "").strip()
        if public:
            kb = {"inline_keyboard": [[{"text": "🗼 Играть", "web_app": {"url": public}}]]}
            body = {
                "chat_id": chat_id,
                "text": "🗼 *Tap Tower* — строй башню одним тапом!\n\nЖми кнопку и побей рекорд друзей.",
                "parse_mode": "Markdown",
                "reply_markup": kb,
            }
        else:
            body = {
                "chat_id": chat_id,
                "text": "🗼 *Tap Tower* почти готов!\n\nИгра уже на сервере, осталось включить публичный доступ — кнопка «Играть» появится совсем скоро. Загляни позже 🙌",
                "parse_mode": "Markdown",
            }
        async with httpx.AsyncClient(timeout=15) as cl:
            await cl.post(f"{TG_API}/sendMessage", json=body)
        return


@app.post("/webhook")
async def webhook(request: Request) -> JSONResponse:
    """Telegram webhook → общий обработчик."""
    await process_update(await request.json())
    return JSONResponse({"ok": True})
