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
import re
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
from starlette.concurrency import run_in_threadpool

# ── Прод-VPS getdoday.ru отдаёт только AAAA для api.telegram.org, а из трёх DC
# routable лишь один IPv4. Тот же фикс, что в боте Doday. Включается флагом
# FORCE_TG_IPV4=1 (только на проде); локально обычное разрешение имён. ──
if os.environ.get("FORCE_TG_IPV4") == "1":
    import asyncio.base_events

    _TG_IPV4 = ("149.154.167.220",)

    # 1) синхронный resolver (httpx sync, urllib, и пр.)
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

    # 2) async resolver event-loop'а (httpx async через anyio/asyncio).
    #    Без этого async-путь резолвит api.telegram.org в IPv6 → таймаут.
    _orig_async_gai = asyncio.base_events.BaseEventLoop.getaddrinfo

    async def _ipv4_only_async(self, host, port=0, *args, **kwargs):  # type: ignore[no-untyped-def]
        if host == "api.telegram.org":
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))
                for ip in _TG_IPV4
            ]
        return await _orig_async_gai(self, host, port, *args, **kwargs)

    asyncio.base_events.BaseEventLoop.getaddrinfo = _ipv4_only_async  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "backend" / "taptower.db"
HUB_HTML = ROOT / "hub" / "index.html"
GAMES_DIR = ROOT / "games"
_SLUG_OK = re.compile(r"^[a-z0-9-]{1,40}$")

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


def tg_call(method: str, payload: dict[str, Any] | None = None,
            params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Синхронный вызов Telegram Bot API. ВАЖНО: только sync httpx — async-путь
    через uvloop игнорирует наш socket-патч IPv4 и таймаутит на проде."""
    with httpx.Client(timeout=60) as cl:
        if params is not None:
            r = cl.get(f"{TG_API}/{method}", params=params)
        else:
            r = cl.post(f"{TG_API}/{method}", json=payload or {})
    return r.json()


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
        # миграция: дропаем pre-multigame таблицы без колонки `game`
        for tbl in ("scores", "purchases"):
            exists = c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
            ).fetchone()
            if exists:
                cols = [r[1] for r in c.execute(f"PRAGMA table_info({tbl})").fetchall()]
                if "game" not in cols:
                    c.execute(f"DROP TABLE {tbl}")
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS scores (
                game       TEXT NOT NULL DEFAULT 'tap-tower',
                user_id    INTEGER NOT NULL,
                username   TEXT,
                first_name TEXT,
                best       INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (game, user_id)
            );
            CREATE TABLE IF NOT EXISTS purchases (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                game        TEXT NOT NULL DEFAULT 'tap-tower',
                user_id     INTEGER NOT NULL,
                item        TEXT NOT NULL,
                stars       INTEGER NOT NULL,
                charge_id   TEXT,
                created_at  INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_scores_game_best ON scores(game, best DESC);
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
    game: str = Field(default="tap-tower", max_length=40)


class InvoiceIn(BaseModel):
    init_data: str = Field(min_length=1)
    item: str = Field(default="continue")  # 'continue' | 'skin:<id>'
    game: str = Field(default="tap-tower", max_length=40)
    title: str = Field(default="", max_length=64)
    stars: int = Field(default=0, ge=0, le=2500)


def _safe_slug(slug: str) -> str | None:
    return slug if _SLUG_OK.match(slug) else None


# ──────────────────────── Routes ────────────────────────
@app.get("/")
async def hub() -> FileResponse:
    """Хаб — список всех игр."""
    return FileResponse(HUB_HTML)


@app.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/api/games")
async def list_games() -> JSONResponse:
    """Авто-обнаружение игр: сканируем games/<slug>/ с index.html (+ meta.json)."""
    games: list[dict[str, Any]] = []
    if GAMES_DIR.is_dir():
        for d in sorted(GAMES_DIR.iterdir()):
            if not d.is_dir() or not (d / "index.html").is_file():
                continue
            meta: dict[str, Any] = {}
            meta_file = d / "meta.json"
            if meta_file.is_file():
                try:
                    meta = json.loads(meta_file.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    meta = {}
            games.append(
                {
                    "slug": d.name,
                    "title": meta.get("title", d.name),
                    "emoji": meta.get("emoji", "🎮"),
                    "tagline": meta.get("tagline", ""),
                    "order": meta.get("order", 100),
                }
            )
    games.sort(key=lambda g: (g["order"], g["title"]))
    return JSONResponse({"games": games})


@app.get("/games/{slug}/", response_model=None)
@app.get("/games/{slug}", response_model=None)
async def serve_game(slug: str) -> FileResponse | JSONResponse:
    s = _safe_slug(slug)
    if not s:
        return JSONResponse({"error": "bad_slug"}, status_code=404)
    index_file = GAMES_DIR / s / "index.html"
    if not index_file.is_file():
        return JSONResponse({"error": "not_found"}, status_code=404)
    return FileResponse(index_file)


@app.get("/games/{slug}/{asset:path}", response_model=None)
async def serve_game_asset(slug: str, asset: str) -> FileResponse | JSONResponse:
    s = _safe_slug(slug)
    if not s or ".." in asset or asset.startswith("/"):
        return JSONResponse({"error": "bad_path"}, status_code=404)
    f = (GAMES_DIR / s / asset).resolve()
    base = (GAMES_DIR / s).resolve()
    if base not in f.parents or not f.is_file():
        return JSONResponse({"error": "not_found"}, status_code=404)
    return FileResponse(f)


@app.post("/api/score")
async def submit_score(body: ScoreIn) -> JSONResponse:
    data = verify_init_data(body.init_data)
    if not data or "user" not in data:
        return JSONResponse({"error": "bad_init_data"}, status_code=401)
    game = _safe_slug(body.game) or "tap-tower"
    u = data["user"]
    uid = int(u["id"])
    with db() as c:
        row = c.execute(
            "SELECT best FROM scores WHERE game=? AND user_id=?", (game, uid)
        ).fetchone()
        prev = row["best"] if row else 0
        best = max(prev, body.score)
        c.execute(
            """INSERT INTO scores(game, user_id, username, first_name, best, updated_at)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(game, user_id) DO UPDATE SET
                 best=excluded.best, username=excluded.username,
                 first_name=excluded.first_name, updated_at=excluded.updated_at""",
            (game, uid, u.get("username"), u.get("first_name"), best, int(time.time())),
        )
    return JSONResponse({"best": best, "submitted": body.score})


@app.get("/api/leaderboard")
async def leaderboard(game: str = "tap-tower", limit: int = 20) -> JSONResponse:
    g = _safe_slug(game) or "tap-tower"
    limit = max(1, min(limit, 100))
    with db() as c:
        rows = c.execute(
            "SELECT username, first_name, best FROM scores WHERE game=? "
            "ORDER BY best DESC, updated_at ASC LIMIT ?",
            (g, limit),
        ).fetchall()
    top = [
        {"name": r["first_name"] or r["username"] or "Игрок", "best": r["best"]}
        for r in rows
    ]
    return JSONResponse({"top": top})


# Дефолтный каталог Stars (для Tap Tower). Новые игры могут прислать свой
# title/stars в теле запроса (валидируется), либо использовать 'continue'.
_DEFAULT_CATALOG = {
    "continue": ("Продолжить игру", "Продолжить с текущего места", 1),
}


@app.post("/api/invoice")
async def create_invoice(body: InvoiceIn) -> JSONResponse:
    """Создаёт ссылку на Stars-инвойс. Для XTR провайдер не нужен."""
    data = verify_init_data(body.init_data)
    if not data or "user" not in data:
        return JSONResponse({"error": "bad_init_data"}, status_code=401)
    uid = int(data["user"]["id"])
    game = _safe_slug(body.game) or "tap-tower"

    if body.item in _DEFAULT_CATALOG:
        title, desc, stars = _DEFAULT_CATALOG[body.item]
    elif body.title and 1 <= body.stars <= 2500:
        # игра прислала свой товар (скин/буст) — название + цена в Stars
        title, desc, stars = body.title, body.title, body.stars
    else:
        return JSONResponse({"error": "unknown_item"}, status_code=400)

    payload = f"{game}:{body.item}:{uid}:{int(time.time())}"
    j = await run_in_threadpool(
        tg_call,
        "createInvoiceLink",
        {
            "title": title,
            "description": desc,
            "payload": payload,
            "currency": "XTR",  # Telegram Stars
            "prices": [{"label": title, "amount": stars}],
        },
    )
    if not j.get("ok"):
        return JSONResponse({"error": "tg_error", "detail": j}, status_code=502)
    return JSONResponse({"link": j["result"], "stars": stars})


def process_update(update: dict[str, Any]) -> None:
    """Обработка одного Telegram-апдейта (СИНХРОННАЯ). Зовётся поллером напрямую
    и webhook'ом через run_in_threadpool."""
    # 1) pre_checkout — обязательно ответить ok в течение 10с
    pcq = update.get("pre_checkout_query")
    if pcq:
        tg_call("answerPreCheckoutQuery", {"pre_checkout_query_id": pcq["id"], "ok": True})
        return

    msg = update.get("message") or {}

    # 2) successful_payment — фиксируем покупку
    sp = msg.get("successful_payment")
    if sp:
        uid = int(msg["from"]["id"])
        # payload формат: "game:item:uid:ts"
        parts = sp.get("invoice_payload", "").split(":")
        game = parts[0] if len(parts) >= 1 and parts[0] else "tap-tower"
        item = parts[1] if len(parts) >= 2 else "?"
        with db() as c:
            c.execute(
                "INSERT INTO purchases(game, user_id, item, stars, charge_id, created_at) "
                "VALUES(?,?,?,?,?,?)",
                (game, uid, item, sp.get("total_amount", 0),
                 sp.get("telegram_payment_charge_id"), int(time.time())),
            )
        return

    # 3) /start — приветствие + кнопка запуска Mini App (если есть PUBLIC_URL)
    text = msg.get("text", "")
    if text.startswith("/start") or text.startswith("/play"):
        chat_id = msg["chat"]["id"]
        public = os.environ.get("PUBLIC_URL", "").strip()
        if public:
            body = {
                "chat_id": chat_id,
                "text": "🗼 *Tap Tower* — строй башню одним тапом!\n\nЖми кнопку и побей рекорд друзей.",
                "parse_mode": "Markdown",
                "reply_markup": {"inline_keyboard": [[{"text": "🗼 Играть", "web_app": {"url": public}}]]},
            }
        else:
            body = {
                "chat_id": chat_id,
                "text": "🗼 *Tap Tower* почти готов!\n\nИгра уже на сервере, осталось включить публичный доступ — кнопка «Играть» появится совсем скоро. Загляни позже 🙌",
                "parse_mode": "Markdown",
            }
        tg_call("sendMessage", body)
        return


@app.post("/webhook")
async def webhook(request: Request) -> JSONResponse:
    """Telegram webhook → общий обработчик (в threadpool, т.к. process_update sync)."""
    update = await request.json()
    await run_in_threadpool(process_update, update)
    return JSONResponse({"ok": True})
