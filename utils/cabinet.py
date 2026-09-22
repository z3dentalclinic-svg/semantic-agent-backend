"""
cabinet.py — личный кабинет: сохранённые прогоны, проекты-дерево, движения по балансу, отзывы, заявки на пополнение,
настройки, реферальный код. Эндпоинты /cabinet/*.

Регистрация в main.py (две строки, ПОСЛЕ install_access_gate(app)):
    from utils.cabinet import install_cabinet
    install_cabinet(app)

cab_0.1 (Andrew, 2026-09-21) — решения Andrew:
  • результаты хранятся год и больше, целиком (вместе с трассировками — бета, помогает чинить баги); повторный прогон
    ради старого результата не нужен: «Открыть» в кабинете → автопилот показывает сохранённое, ничего не списывается;
  • проекты — дерево до 3 уровней (проект → папка → подпапка), прогон без проекта → «Без проекта»; прогон можно
    перенести в другую папку из кабинета; список проектов виден в автопилоте, переключение в один клик;
  • биллинг как в банке: списание (дата, сумма, сид, операция) и зачисление (дата, сумма, кто/за что);
  • история поисков: сид + какие операции сделаны (парсинг, минуса, интенты, страницы, портрет) + время + цена;
  • настройки: язык (список из 10, активен только русский — закрытая бета), цветовая схема;
  • рефералы 10+5: $5 приглашённому и $10 пригласившему — механика включится с открытием регистрации; сейчас код и счётчики;
  • отзывы к прогону (цель беты — фидбек) и заявки на пополнение (до подключения оплаты — владелец пополняет вручную).

Как прогон собирается: автопилот шлёт с каждым запросом заголовки X-Run (id запуска, генерирует сам) и X-Project
(id папки). Ворота (access_gate ag_0.5, RESPONSE_HOOKS) отдают сюда каждый ответ /api/*, он пишется в
<RUNS_DIR>/<user>/<run>/<op>.json.gz — исходный, до очистки; при выдаче тестеру очищается scrub_for_tester.
Без X-Run (другие стенды) — группировка по пользователю и сиду в окне RUN_WINDOW_SEC.

Зависит от utils.access_gate (identify, balance, журнал usage/audit, очистка) — кабинет надстройка над воротами.
Своя база: <ACCESS_DB dir>/cabinet.sqlite. Модули конвейера не трогаются.
"""
from __future__ import annotations

import gzip
import json
import httpx
import os
import re
import secrets
import sqlite3
import threading
import time
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from utils import access_gate as gate

BUILD = "cab_0.2"   # 0.1.1: path операции в ops_detail; 0.2: валюта показа (USD/EUR/UAH/PLN/GBP), курс раз в 12 ч

# ─── настройки ───
MAX_DEPTH = 3                       # проект → папка → подпапка
RUN_WINDOW_SEC = 2 * 3600           # без X-Run: тот же сид у того же пользователя в окне = тот же прогон
REFERRAL_BONUS = {"invited": 5.0, "referrer": 10.0}
LANGUAGES = [("ru", "Русский", True), ("uk", "Українська", False), ("en", "English", False), ("pl", "Polski", False),
             ("de", "Deutsch", False), ("es", "Español", False), ("fr", "Français", False), ("it", "Italiano", False),
             ("tr", "Türkçe", False), ("pt", "Português", False)]
THEMES = ("light", "dark")
CURRENCIES = ("USD", "EUR", "UAH", "PLN", "GBP")            # только показ; баланс и списания всегда в USD
RATES_URL = "https://open.er-api.com/v6/latest/USD"        # бесплатно, без ключа; при недоступности — запасные курсы
RATES_FALLBACK = {"USD": 1.0, "EUR": 0.92, "UAH": 41.5, "PLN": 3.95, "GBP": 0.78}
RATES_TTL = 12 * 3600
# путь → операция в истории; None = не сохранять (служебные)
OPS = {"/api/light-search": "parse", "/api/relevant-search": "parse", "/api/suffix-map": "parse", "/api/prefix-map": "parse",
       "/api/infix-map": "parse", "/api/apply-filters": "filters", "/api/test-clustering": "clusters",
       "/api/minus-semantics": "minus", "/api/minus-wide": "minus", "/api/intent-map": "intents",
       "/api/content-map": "pages", "/api/client-portrait": "portrait"}
OP_NAMES = {"parse": "парсинг", "filters": "фильтры", "clusters": "кластеры", "minus": "минуса", "intents": "интенты",
            "pages": "страницы", "portrait": "портрет"}
# ориентир цен для кабинета (в ценах тестера, ×PRICE_MULT уже учтён) — правится здесь
PRICE_GUIDE = [("Прогон (парсинг + фильтры + кластеры)", "0.05–0.15"), ("Релевантный поиск", "0.20–0.40"),
               ("Минус-слова", "0.02–0.05"), ("Список интентов", "0.10–0.15"), ("Карта контента", "0.02–0.04"),
               ("Портрет клиента", "0.05–0.08")]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, seed TEXT NOT NULL DEFAULT '', project_id INTEGER,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, region TEXT NOT NULL DEFAULT '', language TEXT NOT NULL DEFAULT '',
  mode TEXT NOT NULL DEFAULT '', cost REAL NOT NULL DEFAULT 0, charged REAL NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS runs_user ON runs(user_id, updated_at);
CREATE TABLE IF NOT EXISTS run_ops (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, op TEXT NOT NULL, path TEXT NOT NULL, ts INTEGER NOT NULL,
  status INTEGER NOT NULL, cost REAL, charged REAL, file TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS run_ops_run ON run_ops(run_id, ts);
CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, parent_id INTEGER, name TEXT NOT NULL,
  created_at INTEGER NOT NULL, deleted INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS projects_user ON projects(user_id);
CREATE TABLE IF NOT EXISTS feedback (
  id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, run_id TEXT, rating INTEGER, text TEXT NOT NULL DEFAULT '',
  ts INTEGER NOT NULL, seen INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS topup_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, amount REAL NOT NULL, note TEXT NOT NULL DEFAULT '',
  ts INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'new');
CREATE TABLE IF NOT EXISTS settings (user_id INTEGER PRIMARY KEY, language TEXT NOT NULL DEFAULT 'ru',
  theme TEXT NOT NULL DEFAULT 'light', last_project INTEGER, currency TEXT NOT NULL DEFAULT 'USD');
CREATE TABLE IF NOT EXISTS referrals (user_id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS referral_uses (
  id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_id INTEGER NOT NULL, invited_id INTEGER NOT NULL, ts INTEGER NOT NULL,
  paid INTEGER NOT NULL DEFAULT 0);
"""


class _DB:
    def __init__(self):
        self.lock = threading.RLock()
        self.conn: Optional[sqlite3.Connection] = None
        self.path = ""

    def open(self, path: str):
        with self.lock:
            if self.conn is not None:
                self.conn.close()
            self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
            self.conn.row_factory = sqlite3.Row
            try:
                self.conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
            self.conn.executescript(_SCHEMA)
            cols = {r[1] for r in self.conn.execute("PRAGMA table_info(settings)").fetchall()}   # база cab_0.1 → 0.2
            if "currency" not in cols:
                self.conn.execute("ALTER TABLE settings ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD'")
            self.path = path

    def q(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def one(self, sql, args=()):
        r = self.q(sql, args)
        return r[0] if r else None

    def x(self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).lastrowid


db = _DB()
RUNS_DIR = ""


def _now() -> int:
    return int(time.time())


def _safe_id(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "", s or "")[:40]


# ══════════════════════════ сохранение прогонов (хук ворот) ══════════════════════════

def _find_run(uid: int, run_hdr: str, seed: str) -> Optional[dict]:
    if run_hdr:
        return db.one("SELECT * FROM runs WHERE id = ? AND user_id = ?", (run_hdr, uid))
    if not seed:
        return None
    return db.one("SELECT * FROM runs WHERE user_id = ? AND seed = ? AND deleted = 0 AND updated_at >= ? "
                  "ORDER BY updated_at DESC LIMIT 1", (uid, seed, _now() - RUN_WINDOW_SEC))


def _project_ok(uid: int, pid) -> Optional[int]:
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return None
    p = db.one("SELECT id FROM projects WHERE id = ? AND user_id = ? AND deleted = 0", (pid, uid))
    return pid if p else None


def on_response(ctx: dict):
    """Хук access_gate: каждый успешный JSON-ответ /api/* → файл + запись операции в прогоне."""
    if ctx["status"] >= 400 or not isinstance(ctx["data"], dict):
        return
    op = OPS.get(ctx["path"])
    if not op:
        return
    user, d, h = ctx["user"], ctx["data"], ctx["headers"]
    uid = user["id"]
    seed = d.get("seed") if isinstance(d.get("seed"), str) else ""
    if not seed:
        from urllib.parse import parse_qs
        seed = (parse_qs(ctx["query"]).get("seed") or [""])[0]
    seed = re.sub(r"\s+", " ", seed).strip()[:200]
    run_hdr = _safe_id(h.get("x-run", ""))
    now = _now()
    with db.lock:
        run = _find_run(uid, run_hdr, seed)
        if run is None:
            rid = run_hdr or ("r_" + secrets.token_urlsafe(9))
            pid = _project_ok(uid, h.get("x-project"))
            db.x("INSERT INTO runs(id, user_id, seed, project_id, created_at, updated_at, region, language, mode) "
                 "VALUES (?,?,?,?,?,?,?,?,?)",
                 (rid, uid, seed, pid, now, now, str(d.get("region") or "")[:40], str(d.get("language") or "")[:20],
                  "relevant" if ctx["path"] == "/api/relevant-search" else "direct"))
            run = {"id": rid, "seed": seed}
        else:
            rid = run["id"]
            if not run.get("seed") and seed:
                db.x("UPDATE runs SET seed = ? WHERE id = ?", (seed, rid))
        udir = os.path.join(RUNS_DIR, str(uid), _safe_id(rid))
        os.makedirs(udir, exist_ok=True)
        n = db.one("SELECT COUNT(*) AS n FROM run_ops WHERE run_id = ? AND op = ?", (rid, op))["n"]
        fname = f"{op}{'' if n == 0 else '_' + str(n + 1)}.json.gz"
        with gzip.open(os.path.join(udir, fname), "wt", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
        cost, charged = ctx["cost"] or 0.0, ctx["charged"] or 0.0
        db.x("INSERT INTO run_ops(run_id, op, path, ts, status, cost, charged, file) VALUES (?,?,?,?,?,?,?,?)",
             (rid, op, ctx["path"], now, ctx["status"], cost, charged, fname))
        db.x("UPDATE runs SET updated_at = ?, cost = cost + ?, charged = charged + ? WHERE id = ?", (now, cost, charged, rid))


# ══════════════════════════ помощники ══════════════════════════

router = APIRouter()


def _me(request: Request):
    """→ (user, None) или (None, JSONResponse)."""
    if not gate.gate_on():
        return None, JSONResponse({"error": "Ворота не настроены", "code": "gate_off"}, status_code=503)
    u, _ = gate.identify({k.lower(): v for k, v in request.headers.items()})
    if not u:
        return None, JSONResponse({"error": "Нужен токен доступа", "code": "no_token"}, status_code=401)
    if not u.get("enabled"):
        return None, JSONResponse({"error": "Доступ отключён", "code": "disabled"}, status_code=403)
    return u, None


def _owner(request: Request):
    u, err = _me(request)
    if err:
        return None, err
    if u["role"] != "owner":
        return None, JSONResponse({"error": "Только для владельца", "code": "owner_only"}, status_code=403)
    return u, None


def _tree(uid: int) -> list:
    rows = db.q("SELECT id, parent_id, name, created_at FROM projects WHERE user_id = ? AND deleted = 0 ORDER BY name", (uid,))
    counts = {r["project_id"]: r["n"] for r in db.q(
        "SELECT project_id, COUNT(*) AS n FROM runs WHERE user_id = ? AND deleted = 0 GROUP BY project_id", (uid,))}
    by_parent: dict = {}
    for r in rows:
        by_parent.setdefault(r["parent_id"], []).append(r)

    def build(parent, depth):
        out = []
        for r in by_parent.get(parent, []):
            node = {"id": r["id"], "name": r["name"], "depth": depth, "runs": counts.get(r["id"], 0),
                    "children": build(r["id"], depth + 1)}
            out.append(node)
        return out
    return build(None, 1)


def _depth_of(uid: int, pid: Optional[int]) -> int:
    depth = 0
    while pid is not None:
        p = db.one("SELECT parent_id FROM projects WHERE id = ? AND user_id = ? AND deleted = 0", (pid, uid))
        if not p:
            break
        depth += 1
        pid = p["parent_id"]
    return depth


def _settings(uid: int) -> dict:
    s = db.one("SELECT * FROM settings WHERE user_id = ?", (uid,)) or {"language": "ru", "theme": "light", "last_project": None,
                                                                      "currency": "USD"}
    return {"language": s["language"], "theme": s["theme"], "last_project": s["last_project"], "currency": s.get("currency") or "USD"}


_rates_cache = {"ts": 0, "rates": dict(RATES_FALLBACK), "date": "", "source": "fallback"}
_rates_lock = threading.Lock()


def get_rates() -> dict:
    """Курсы к доллару для показа. Обновление раз в RATES_TTL; при сбое сети — прежние или запасные."""
    with _rates_lock:
        if time.time() - _rates_cache["ts"] < RATES_TTL:
            return dict(_rates_cache)
        try:
            r = httpx.get(RATES_URL, timeout=6)
            d = r.json()
            src = d.get("rates") or {}
            rates = {c: float(src.get(c, RATES_FALLBACK[c])) for c in CURRENCIES}
            rates["USD"] = 1.0
            _rates_cache.update({"ts": time.time(), "rates": rates, "date": (d.get("time_last_update_utc") or "")[:16],
                                 "source": "open.er-api.com"})
        except Exception:  # noqa: BLE001
            _rates_cache["ts"] = time.time() - RATES_TTL + 600    # не долбить сеть: следующая попытка через 10 минут
        return dict(_rates_cache)


def _referral(uid: int) -> dict:
    r = db.one("SELECT code FROM referrals WHERE user_id = ?", (uid,))
    if not r:
        code = "SA-" + "".join(secrets.choice("ABCDEFGHJKMNPQRSTUVWXYZ23456789") for _ in range(6))
        db.x("INSERT INTO referrals(user_id, code, created_at) VALUES (?,?,?)", (uid, code, _now()))
        r = {"code": code}
    n = db.one("SELECT COUNT(*) AS n, COALESCE(SUM(paid),0) AS paid FROM referral_uses WHERE referrer_id = ?", (uid,))
    return {"code": r["code"], "invited": n["n"], "paid": n["paid"], "bonus": REFERRAL_BONUS, "active": False}


def _run_row(r: dict) -> dict:
    ops = db.q("SELECT op, ts, status, cost, charged FROM run_ops WHERE run_id = ? ORDER BY ts", (r["id"],))
    done = []
    for o in ops:
        if o["op"] not in done:
            done.append(o["op"])
    fb = db.one("SELECT rating, text, ts FROM feedback WHERE run_id = ? ORDER BY id DESC LIMIT 1", (r["id"],))
    return {"id": r["id"], "seed": r["seed"], "project_id": r["project_id"], "created_at": r["created_at"],
            "updated_at": r["updated_at"], "region": r["region"], "language": r["language"], "mode": r["mode"],
            "ops": done, "ops_names": [OP_NAMES.get(o, o) for o in done], "cost": round(r["cost"], 5),
            "charged": round(r["charged"], 4), "feedback": fb}


# ══════════════════════════ /cabinet/* — пользователь ══════════════════════════

@router.get("/cabinet/overview")
async def cab_overview(request: Request):
    u, err = _me(request)
    if err:
        return err
    uid = u["id"]
    out = {"build": BUILD, "role": u["role"], "name": u["name"], "id": uid, "settings": _settings(uid),
           "languages": [{"code": c, "name": n, "active": a} for c, n, a in LANGUAGES], "themes": list(THEMES),
           "currencies": list(CURRENCIES), "rates": {k: v for k, v in get_rates().items() if k != "ts"},
           "projects": _tree(uid), "referral": _referral(uid), "price_guide": PRICE_GUIDE,
           "runs_total": db.one("SELECT COUNT(*) AS n FROM runs WHERE user_id = ? AND deleted = 0", (uid,))["n"]}
    if u["role"] == "tester":
        full = gate.db.one("SELECT * FROM users WHERE id = ?", (uid,))
        out["balance"] = gate.balance_of(full)
        out["email"] = full["email"]
        out["min_balance"] = gate.MIN_BALANCE
        out["topup_pending"] = db.q("SELECT id, amount, ts, status FROM topup_requests WHERE user_id = ? AND status = 'new'", (uid,))
    else:
        out["today"] = gate.usage_today(uid)
        out["email"] = (gate.db.one("SELECT email FROM users WHERE id = ?", (uid,)) or {}).get("email", "")
    return out


@router.get("/cabinet/ledger")
async def cab_ledger(request: Request, limit: int = 500):
    """Движения по балансу, как выписка: списания из журнала ворот, зачисления из аудита владельца."""
    u, err = _me(request)
    if err:
        return err
    uid = u["id"]
    limit = max(1, min(int(limit), 5000))
    rows = []
    if u["role"] == "tester":
        full = gate.db.one("SELECT created_at, credit FROM users WHERE id = ?", (uid,))
        topups = gate.db.q("SELECT ts, detail, action FROM audit WHERE action = 'user_topup' AND target = ?", (str(uid),))
        topped = sum(float(t["detail"] or 0) for t in topups)
        start = round((full["credit"] or 0) - topped, 4)
        if start:
            rows.append({"ts": full["created_at"], "kind": "credit", "amount": start, "title": "Стартовый баланс", "seed": ""})
        for t in topups:
            amt = float(t["detail"] or 0)
            rows.append({"ts": t["ts"], "kind": "credit" if amt >= 0 else "debit", "amount": amt,
                         "title": "Пополнение владельцем" if amt >= 0 else "Корректировка владельцем", "seed": ""})
    usage = gate.db.q("SELECT ts, path, seed, cost, charged, status FROM usage WHERE user_id = ? AND status < 400 "
                      "AND (charged > 0 OR (cost IS NOT NULL AND cost > 0)) ORDER BY id DESC LIMIT ?", (uid, limit))
    for x in usage:
        op = OPS.get(x["path"], x["path"].replace("/api/", ""))
        rows.append({"ts": x["ts"], "kind": "debit", "amount": -(x["charged"] if u["role"] == "tester" else (x["cost"] or 0)),
                     "title": OP_NAMES.get(op, op), "seed": x["seed"] or ""})
    rows.sort(key=lambda r: r["ts"], reverse=True)
    bal = None
    if u["role"] == "tester":
        bal = gate.balance_of(gate.db.one("SELECT * FROM users WHERE id = ?", (uid,)))
    return {"rows": rows[:limit], "balance": bal, "prices_are_real": u["role"] != "tester"}


@router.get("/cabinet/runs")
async def cab_runs(request: Request, project_id: Optional[str] = None, q: str = "", limit: int = 200):
    """История: project_id = число | 'none' (без проекта) | пусто (все)."""
    u, err = _me(request)
    if err:
        return err
    uid = u["id"]
    sql, args = "SELECT * FROM runs WHERE user_id = ? AND deleted = 0", [uid]
    if project_id == "none":
        sql += " AND project_id IS NULL"
    elif project_id:
        try:
            pid = int(project_id)
        except ValueError:
            return JSONResponse({"error": "project_id", "code": "bad_project"}, status_code=400)
        ids = [pid] + _descendants(uid, pid)
        sql += f" AND project_id IN ({','.join('?' * len(ids))})"; args += ids
    if q.strip():
        sql += " AND seed LIKE ?"; args.append("%" + q.strip() + "%")
    sql += " ORDER BY updated_at DESC LIMIT ?"; args.append(max(1, min(int(limit), 2000)))
    return {"rows": [_run_row(r) for r in db.q(sql, tuple(args))]}


def _descendants(uid: int, pid: int) -> list:
    out, stack = [], [pid]
    while stack:
        p = stack.pop()
        for c in db.q("SELECT id FROM projects WHERE parent_id = ? AND user_id = ? AND deleted = 0", (p, uid)):
            out.append(c["id"]); stack.append(c["id"])
    return out


@router.get("/cabinet/runs/{run_id}")
async def cab_run(request: Request, run_id: str):
    u, err = _me(request)
    if err:
        return err
    r = db.one("SELECT * FROM runs WHERE id = ? AND user_id = ? AND deleted = 0", (_safe_id(run_id), u["id"]))
    if not r:
        return JSONResponse({"error": "Прогон не найден", "code": "not_found"}, status_code=404)
    out = _run_row(r)
    out["ops_detail"] = db.q("SELECT op, path, ts, status, cost, charged FROM run_ops WHERE run_id = ? ORDER BY ts", (r["id"],))
    if u["role"] == "tester":
        for o in out["ops_detail"]:
            o.pop("cost", None)
    return out


@router.get("/cabinet/runs/{run_id}/op/{op}")
async def cab_run_op(request: Request, run_id: str, op: str):
    """Сохранённый ответ операции — то, что автопилот показывает при «Открыть». Тестеру — очищенный. Не тарифицируется."""
    u, err = _me(request)
    if err:
        return err
    rid = _safe_id(run_id)
    r = db.one("SELECT id FROM runs WHERE id = ? AND user_id = ? AND deleted = 0", (rid, u["id"]))
    if not r:
        return JSONResponse({"error": "Прогон не найден", "code": "not_found"}, status_code=404)
    o = db.one("SELECT file FROM run_ops WHERE run_id = ? AND op = ? ORDER BY id DESC LIMIT 1", (rid, _safe_id(op)))
    if not o:
        return JSONResponse({"error": "Такой операции в прогоне нет", "code": "no_op"}, status_code=404)
    path = os.path.join(RUNS_DIR, str(u["id"]), rid, o["file"])
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            d = json.load(f)
    except OSError:
        return JSONResponse({"error": "Файл результата не найден", "code": "no_file"}, status_code=404)
    if u["role"] == "tester":
        d = gate.scrub_for_tester(d)
    return d


class RunMoveReq(BaseModel):
    project_id: Optional[int] = None    # None = «Без проекта»


@router.post("/cabinet/runs/{run_id}/move")
async def cab_run_move(request: Request, run_id: str, req: RunMoveReq):
    u, err = _me(request)
    if err:
        return err
    rid = _safe_id(run_id)
    if not db.one("SELECT id FROM runs WHERE id = ? AND user_id = ? AND deleted = 0", (rid, u["id"])):
        return JSONResponse({"error": "Прогон не найден", "code": "not_found"}, status_code=404)
    pid = None
    if req.project_id is not None:
        pid = _project_ok(u["id"], req.project_id)
        if pid is None:
            return JSONResponse({"error": "Папка не найдена", "code": "bad_project"}, status_code=404)
    db.x("UPDATE runs SET project_id = ? WHERE id = ?", (pid, rid))
    return {"ok": True, "run_id": rid, "project_id": pid}


@router.post("/cabinet/runs/{run_id}/delete")
async def cab_run_delete(request: Request, run_id: str):
    u, err = _me(request)
    if err:
        return err
    rid = _safe_id(run_id)
    if not db.one("SELECT id FROM runs WHERE id = ? AND user_id = ? AND deleted = 0", (rid, u["id"])):
        return JSONResponse({"error": "Прогон не найден", "code": "not_found"}, status_code=404)
    db.x("UPDATE runs SET deleted = 1 WHERE id = ?", (rid,))      # файлы остаются (бета: разбор багов), из истории уходит
    return {"ok": True}


# ─── проекты ───

class ProjectReq(BaseModel):
    name: str
    parent_id: Optional[int] = None


@router.post("/cabinet/projects")
async def cab_project_create(request: Request, req: ProjectReq):
    u, err = _me(request)
    if err:
        return err
    name = re.sub(r"\s+", " ", req.name or "").strip()[:80]
    if not name:
        return JSONResponse({"error": "Укажите название", "code": "no_name"}, status_code=400)
    parent = None
    if req.parent_id is not None:
        parent = _project_ok(u["id"], req.parent_id)
        if parent is None:
            return JSONResponse({"error": "Родительская папка не найдена", "code": "bad_parent"}, status_code=404)
        if _depth_of(u["id"], parent) >= MAX_DEPTH:
            return JSONResponse({"error": f"Глубже {MAX_DEPTH} уровней нельзя", "code": "too_deep"}, status_code=400)
    pid = db.x("INSERT INTO projects(user_id, parent_id, name, created_at) VALUES (?,?,?,?)", (u["id"], parent, name, _now()))
    return {"ok": True, "id": pid, "projects": _tree(u["id"])}


class ProjectUpdateReq(BaseModel):
    name: Optional[str] = None
    parent_id: Optional[int] = None    # перенос папки; -1 = в корень
    

@router.post("/cabinet/projects/{pid}/update")
async def cab_project_update(request: Request, pid: int, req: ProjectUpdateReq):
    u, err = _me(request)
    if err:
        return err
    if _project_ok(u["id"], pid) is None:
        return JSONResponse({"error": "Папка не найдена", "code": "not_found"}, status_code=404)
    if req.name is not None:
        name = re.sub(r"\s+", " ", req.name).strip()[:80]
        if not name:
            return JSONResponse({"error": "Укажите название", "code": "no_name"}, status_code=400)
        db.x("UPDATE projects SET name = ? WHERE id = ?", (name, pid))
    if req.parent_id is not None:
        new_parent = None if req.parent_id < 0 else _project_ok(u["id"], req.parent_id)
        if req.parent_id >= 0 and new_parent is None:
            return JSONResponse({"error": "Родительская папка не найдена", "code": "bad_parent"}, status_code=404)
        if new_parent == pid or new_parent in _descendants(u["id"], pid):
            return JSONResponse({"error": "Нельзя переместить папку внутрь себя", "code": "cycle"}, status_code=400)
        sub_depth = _subtree_depth(u["id"], pid)
        if _depth_of(u["id"], new_parent) + sub_depth > MAX_DEPTH:
            return JSONResponse({"error": f"Глубже {MAX_DEPTH} уровней нельзя", "code": "too_deep"}, status_code=400)
        db.x("UPDATE projects SET parent_id = ? WHERE id = ?", (new_parent, pid))
    return {"ok": True, "projects": _tree(u["id"])}


def _subtree_depth(uid: int, pid: int) -> int:
    kids = db.q("SELECT id FROM projects WHERE parent_id = ? AND user_id = ? AND deleted = 0", (pid, uid))
    return 1 + max([_subtree_depth(uid, k["id"]) for k in kids], default=0)


@router.post("/cabinet/projects/{pid}/delete")
async def cab_project_delete(request: Request, pid: int):
    """Папка и вложенные папки удаляются, их прогоны переезжают в родителя (или «Без проекта»)."""
    u, err = _me(request)
    if err:
        return err
    p = db.one("SELECT parent_id FROM projects WHERE id = ? AND user_id = ? AND deleted = 0", (pid, u["id"]))
    if not p:
        return JSONResponse({"error": "Папка не найдена", "code": "not_found"}, status_code=404)
    ids = [pid] + _descendants(u["id"], pid)
    marks = ",".join("?" * len(ids))
    db.x(f"UPDATE runs SET project_id = ? WHERE user_id = ? AND project_id IN ({marks})", (p["parent_id"], u["id"], *ids))
    db.x(f"UPDATE projects SET deleted = 1 WHERE id IN ({marks})", tuple(ids))
    s = db.one("SELECT last_project FROM settings WHERE user_id = ?", (u["id"],))
    if s and s["last_project"] in ids:
        db.x("UPDATE settings SET last_project = ? WHERE user_id = ?", (p["parent_id"], u["id"]))
    return {"ok": True, "projects": _tree(u["id"])}


# ─── настройки, профиль ───

class SettingsReq(BaseModel):
    language: Optional[str] = None
    theme: Optional[str] = None
    currency: Optional[str] = None
    last_project: Optional[int] = None   # -1 = «Без проекта»


@router.post("/cabinet/settings")
async def cab_settings(request: Request, req: SettingsReq):
    u, err = _me(request)
    if err:
        return err
    uid = u["id"]
    cur = _settings(uid)
    if req.language is not None:
        active = {c for c, _, a in LANGUAGES if a}
        if req.language not in active:
            return JSONResponse({"error": "Этот язык пока недоступен (закрытая бета)", "code": "lang_inactive"}, status_code=400)
        cur["language"] = req.language
    if req.theme is not None:
        if req.theme not in THEMES:
            return JSONResponse({"error": "Неизвестная схема", "code": "bad_theme"}, status_code=400)
        cur["theme"] = req.theme
    if req.currency is not None:
        if req.currency not in CURRENCIES:
            return JSONResponse({"error": "Неизвестная валюта", "code": "bad_currency"}, status_code=400)
        cur["currency"] = req.currency
    if req.last_project is not None:
        if req.last_project < 0:
            cur["last_project"] = None
        else:
            if _project_ok(uid, req.last_project) is None:
                return JSONResponse({"error": "Папка не найдена", "code": "bad_project"}, status_code=404)
            cur["last_project"] = req.last_project
    db.x("INSERT INTO settings(user_id, language, theme, last_project, currency) VALUES (?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
         "language = excluded.language, theme = excluded.theme, last_project = excluded.last_project, currency = excluded.currency",
         (uid, cur["language"], cur["theme"], cur["last_project"], cur["currency"]))
    return {"ok": True, "settings": cur}


class ProfileReq(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None


@router.post("/cabinet/profile")
async def cab_profile(request: Request, req: ProfileReq):
    u, err = _me(request)
    if err:
        return err
    if u["role"] == "owner":
        return JSONResponse({"error": "У владельца нет профиля в базе", "code": "owner"}, status_code=400)
    sets, args = [], []
    if req.name is not None:
        n = req.name.strip()[:80]
        if not n:
            return JSONResponse({"error": "Имя не может быть пустым", "code": "no_name"}, status_code=400)
        sets.append("name = ?"); args.append(n)
    if req.email is not None:
        sets.append("email = ?"); args.append(req.email.strip()[:120])
    if not sets:
        return JSONResponse({"error": "Нечего менять", "code": "empty"}, status_code=400)
    gate.db.x(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", tuple(args) + (u["id"],))
    gate.audit("user:" + str(u["id"]), "profile_update", str(u["id"]), json.dumps(req.model_dump(exclude_none=True), ensure_ascii=False))
    return {"ok": True}


@router.post("/cabinet/reissue")
async def cab_reissue(request: Request):
    """Сам себе новый токен: старый перестаёт работать сразу (потерян ноутбук и т.п.)."""
    u, err = _me(request)
    if err:
        return err
    if u["role"] == "owner":
        return JSONResponse({"error": "Ключ владельца меняется в Render", "code": "owner"}, status_code=400)
    token = gate._new_token()
    gate.db.x("UPDATE users SET token_hash = ? WHERE id = ?", (gate._sha(token), u["id"]))
    gate.audit("user:" + str(u["id"]), "self_reissue", str(u["id"]), "")
    return {"token": token, "note": "Токен показывается один раз"}


# ─── отзывы, заявки на пополнение ───

class FeedbackReq(BaseModel):
    run_id: Optional[str] = None
    rating: Optional[int] = None     # 1–5
    text: str = ""


@router.post("/cabinet/feedback")
async def cab_feedback(request: Request, req: FeedbackReq):
    u, err = _me(request)
    if err:
        return err
    rid = _safe_id(req.run_id or "") or None
    if rid and not db.one("SELECT id FROM runs WHERE id = ? AND user_id = ?", (rid, u["id"])):
        return JSONResponse({"error": "Прогон не найден", "code": "not_found"}, status_code=404)
    rating = req.rating if req.rating is None else max(1, min(5, int(req.rating)))
    text = (req.text or "").strip()[:4000]
    if rating is None and not text:
        return JSONResponse({"error": "Поставьте оценку или напишите текст", "code": "empty"}, status_code=400)
    fid = db.x("INSERT INTO feedback(user_id, run_id, rating, text, ts) VALUES (?,?,?,?,?)", (u["id"], rid, rating, text, _now()))
    return {"ok": True, "id": fid}


class TopupReq(BaseModel):
    amount: float = 20.0
    note: str = ""


@router.post("/cabinet/topup-request")
async def cab_topup_request(request: Request, req: TopupReq):
    u, err = _me(request)
    if err:
        return err
    if u["role"] != "tester":
        return JSONResponse({"error": "Баланс есть только у тестера", "code": "not_tester"}, status_code=400)
    amt = float(req.amount or 0)
    if not (0 < amt <= 1000):
        return JSONResponse({"error": "Сумма от $1 до $1000", "code": "bad_amount"}, status_code=400)
    if db.one("SELECT id FROM topup_requests WHERE user_id = ? AND status = 'new'", (u["id"],)):
        return JSONResponse({"error": "Заявка уже отправлена, владелец её видит", "code": "pending"}, status_code=409)
    rid = db.x("INSERT INTO topup_requests(user_id, amount, note, ts) VALUES (?,?,?,?)", (u["id"], amt, (req.note or "")[:300], _now()))
    return {"ok": True, "id": rid, "note": "Заявка отправлена — владелец пополнит баланс вручную"}


@router.get("/cabinet/export.csv")
async def cab_export(request: Request):
    from fastapi.responses import PlainTextResponse
    u, err = _me(request)
    if err:
        return err
    led = await cab_ledger(request, limit=5000)
    if isinstance(led, JSONResponse):
        return led
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["date", "kind", "amount_usd", "title", "seed"])
    for r in led["rows"]:
        w.writerow([time.strftime("%Y-%m-%d %H:%M", time.gmtime(r["ts"])), r["kind"], f"{r['amount']:.4f}", r["title"], r["seed"]])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=semantic-agent-ledger.csv"})


# ══════════════════════════ /cabinet/admin/* — владелец ══════════════════════════

@router.get("/cabinet/admin/feedback")
async def adm_feedback(request: Request, limit: int = 200):
    _, err = _owner(request)
    if err:
        return err
    rows = db.q("SELECT * FROM feedback ORDER BY id DESC LIMIT ?", (max(1, min(int(limit), 2000)),))
    for r in rows:
        run = db.one("SELECT seed FROM runs WHERE id = ?", (r["run_id"],)) if r["run_id"] else None
        r["seed"] = run["seed"] if run else ""
        usr = gate.db.one("SELECT name, role FROM users WHERE id = ?", (r["user_id"],))
        r["user_name"] = usr["name"] if usr else "#" + str(r["user_id"])
    return {"rows": rows, "unseen": sum(1 for r in rows if not r["seen"])}


@router.post("/cabinet/admin/feedback/{fid}/seen")
async def adm_feedback_seen(request: Request, fid: int):
    _, err = _owner(request)
    if err:
        return err
    db.x("UPDATE feedback SET seen = 1 WHERE id = ?", (fid,))
    return {"ok": True}


@router.get("/cabinet/admin/topup-requests")
async def adm_topups(request: Request):
    _, err = _owner(request)
    if err:
        return err
    rows = db.q("SELECT * FROM topup_requests ORDER BY id DESC LIMIT 500")
    for r in rows:
        usr = gate.db.one("SELECT name FROM users WHERE id = ?", (r["user_id"],))
        r["user_name"] = usr["name"] if usr else "#" + str(r["user_id"])
    return {"rows": rows, "open": sum(1 for r in rows if r["status"] == "new")}


class TopupCloseReq(BaseModel):
    status: str = "done"                 # done | rejected


@router.post("/cabinet/admin/topup-requests/{rid}/close")
async def adm_topup_close(request: Request, rid: int, req: TopupCloseReq):
    """Закрыть заявку. Само пополнение — как раньше, /access/admin/user/{uid}/topup из админки."""
    _, err = _owner(request)
    if err:
        return err
    if req.status not in ("done", "rejected"):
        return JSONResponse({"error": "status: done | rejected", "code": "bad_status"}, status_code=400)
    if not db.one("SELECT id FROM topup_requests WHERE id = ?", (rid,)):
        return JSONResponse({"error": "Заявка не найдена", "code": "not_found"}, status_code=404)
    db.x("UPDATE topup_requests SET status = ? WHERE id = ?", (req.status, rid))
    return {"ok": True}


@router.get("/cabinet/admin/runs")
async def adm_runs(request: Request, user_id: Optional[int] = None, limit: int = 200):
    """Прогоны всех пользователей — для разбора багов; сам результат — /cabinet/admin/runs/{user_id}/{run_id}/op/{op}."""
    _, err = _owner(request)
    if err:
        return err
    lim = max(1, min(int(limit), 2000))
    rows = (db.q("SELECT * FROM runs WHERE user_id = ? ORDER BY updated_at DESC LIMIT ?", (user_id, lim)) if user_id is not None
            else db.q("SELECT * FROM runs ORDER BY updated_at DESC LIMIT ?", (lim,)))
    out = []
    for r in rows:
        row = _run_row(r); row["user_id"] = r["user_id"]; row["deleted"] = r["deleted"]
        out.append(row)
    return {"rows": out}


@router.get("/cabinet/admin/runs/{user_id}/{run_id}/op/{op}")
async def adm_run_op(request: Request, user_id: int, run_id: str, op: str):
    _, err = _owner(request)
    if err:
        return err
    rid = _safe_id(run_id)
    o = db.one("SELECT file FROM run_ops WHERE run_id = ? AND op = ? ORDER BY id DESC LIMIT 1", (rid, _safe_id(op)))
    if not o:
        return JSONResponse({"error": "Не найдено", "code": "not_found"}, status_code=404)
    try:
        with gzip.open(os.path.join(RUNS_DIR, str(user_id), rid, o["file"]), "rt", encoding="utf-8") as f:
            return json.load(f)
    except OSError:
        return JSONResponse({"error": "Файл не найден", "code": "no_file"}, status_code=404)


# ══════════════════════════ установка ══════════════════════════

def install_cabinet(app, db_path: Optional[str] = None, runs_dir: Optional[str] = None):
    """После install_access_gate(app): своя база рядом с access.sqlite, файлы прогонов в <dir>/runs/."""
    global RUNS_DIR
    base_dir = os.path.dirname(os.path.abspath(gate.db.path)) if gate.db.path else "."
    db.open(db_path or os.path.join(base_dir, "cabinet.sqlite"))
    RUNS_DIR = runs_dir or os.path.join(base_dir, "runs")
    os.makedirs(RUNS_DIR, exist_ok=True)
    if on_response not in gate.RESPONSE_HOOKS:
        gate.RESPONSE_HOOKS.append(on_response)
    app.include_router(router)
    print(f"[cabinet {BUILD}] база {db.path}; прогоны в {RUNS_DIR}")
