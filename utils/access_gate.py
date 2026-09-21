"""
access_gate.py — ворота доступа: владелец / суперпользователи / тестеры по инвайтам, счётчики расходов.

Регистрация в main.py (две строки, В САМОМ КОНЦЕ файла — после всех include_router / register_*):
    from utils.access_gate import install_access_gate
    install_access_gate(app)

ag_0.1 (Andrew, 2026-09-20) — первый тест на реальных пользователях, решения Andrew:
  • вход по инвайт-кодам, не более 15 тестеров; одноразовый код → токен;
  • 4 суперпользователя: вечный доступ, БЕЗ лимитов, только счётчики расходов (чтобы владелец видел траты);
  • владелец один — только он создаёт инвайты и суперпользователей, включает / отключает кого угодно, перевыпускает токены;
  • страховка: ключ владельца живёт ТОЛЬКО в переменной окружения (не в базе, не в почте; восстановления через почту нет),
    смена переменной = мгновенная смена ключа; аварийный рубильник — вторая переменная; токены в базе — хэшами;
    журнал действий владельца.

Переменные окружения (Render → Environment):
    ACCESS_OWNER_KEY   ключ владельца, длинная случайная строка (≥ 32 символов). ПОКА НЕ ЗАДАН — ВОРОТА ВЫКЛЮЧЕНЫ,
                       сервер работает как раньше (безопасный порядок выкладки: код → проверка → переменная; откат = убрать).
    ACCESS_LOCKDOWN    1 — рубильник: всё закрыто для всех, кроме владельца.
    ACCESS_DB          путь к SQLite (по умолчанию /var/data/access.sqlite, локально ./access.sqlite).
    ACCESS_OPEN_PATHS  доп. открытые пути через запятую (например страница рабочего интерфейса), точное совпадение.

Правило ворот — «закрыто всё, кроме списка»: без токена доступны только "/", /favicon.ico и /access/* (вход, активация).
/docs, /openapi.json, /debug/*, страницы стендов — закрыты автоматически (владелец открывает их через /access/owner-login).
    владелец  — всё;
    супер     — всё, кроме OWNER_ONLY_PREFIXES; без лимитов, расходы считаются;
    тестер    — только /api/*, кроме OWNER_ONLY_PREFIXES; лимиты: запусков в день, $ в день, общий дневной потолок тестеров.
Токен: заголовок «Authorization: Bearer <токен>» (или X-Access-Token); для страниц с самого сервера — cookie.

Расход запроса берётся из ответа эндпоинта той же логикой, что Total в autopilot.html (extract_cost) — модули не правятся.
Модуль самодостаточен: ничего не импортирует из других модулей проекта.

ag_0.2 (Andrew, 2026-09-20) — модель «только баланс», так же будет работать боевой сервис:
  • владелец и суперпользователи — НИКАКИХ лимитов (ни суточных, ни денежных), счётчики по реальной цене;
  • тестер — баланс $20 при активации, списание = реальная цена × 2.5 (наценка Andrew); суточных и прочих лимитов нет;
    баланс кончился → «пополните баланс» (тестовый период окончен), владелец может пополнить вручную;
  • порог запуска: прямой поиск /api/light-search — баланс ≥ $0.10, релевантный /api/relevant-search — ≥ $0.25,
    остальные операции — баланс > 0; цена известна только после операции, последняя может увести баланс в минус;
  • шаги прямого конвейера (фильтры, кластеры) идут отдельными запросами после парсинга — им дано окно RUN_GRACE_SEC
    после принятого запуска, чтобы начатый прогон не обрывался посередине из-за списания за фильтры;
  • один платный запрос на тестера одновременно (иначе параллельные запросы проходят проверку баланса все сразу);
  • ответы тестерам переписываются на лету: цены × 2.5, названия моделей / токены / служебные блоки этапов вырезаются —
    реальная себестоимость и стек моделей не видны в инструментах разработчика. Не удалось разобрать JSON-ответ для
    тестера — ответ НЕ отдаётся (закрыто по умолчанию). Владелец и суперпользователи получают ответы как есть.
  • интерфейсы работают с диска (file://): токен только заголовком, cookie — лишь для страниц самого сервера.
  Суточные лимиты ag_0.1 (TESTER_DAILY_RUNS / TESTER_DAILY_COST / GLOBAL_DAILY_COST) закомментированы — точка отката.

ag_0.3 (Andrew, 2026-09-21): тестера владелец создаёт напрямую, как суперпользователя (/access/admin/tester → токен
  показан один раз, баланс TESTER_START_CREDIT), без обмена инвайт-кода — «отослал ключ, человек сразу работает».
  Инвайты остаются для будущей самостоятельной регистрации на сайте; место тестера считается общее: тестеры + коды ≤ 15.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
import time
from http.cookies import SimpleCookie
from typing import Optional
from urllib.parse import parse_qs

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

BUILD = "ag_0.4"   # ag_0.1 суточные лимиты; ag_0.2 баланс + наценка + очистка; ag_0.3 тестер напрямую; ag_0.4 утечки тарифов/провайдера/build

# ─── настройки. Правка чисел — только здесь (лимиты тестера меняются и по каждому пользователю из админки). ───
MAX_SUPERS = 4
MAX_TESTERS = 15                  # активированные тестеры + неиспользованные инвайты
# TESTER_DAILY_RUNS = 10          # ag_0.1: запусков в сутки — отменено Andrew (лимит только балансом)
# TESTER_DAILY_COST = 3.0         # ag_0.1: $ в сутки на тестера — отменено
# GLOBAL_DAILY_COST = 25.0        # ag_0.1: общий дневной потолок тестеров — отменено
TESTER_START_CREDIT = 20.0        # $ на балансе тестера при активации (в ценах с наценкой)
PRICE_MULT = 2.5                  # списание с тестера = реальная цена × PRICE_MULT
MIN_BALANCE = {"/api/light-search": 0.10, "/api/relevant-search": 0.25}   # порог запуска; прочие операции — баланс > 0
RUN_FOLLOW_PATHS = ("/api/apply-filters", "/api/test-clustering")         # шаги прямого конвейера после парсинга
RUN_GRACE_SEC = 900               # окно после принятого запуска, в котором шаги конвейера идут и при балансе ≤ 0
RUN_START_PATHS = ("/api/light-search", "/api/relevant-search")          # начало прогона по сиду = «запуск»
OWNER_ONLY_PREFIXES = ("/access/admin/", "/debug/", "/api/trace/toggle")
OPEN_PATHS = {"/", "/favicon.ico"}                                        # "/" — под проверку живости Render
OPEN_PREFIXES = ("/access/",)                                             # кроме /access/admin/ (OWNER_ONLY выше)
LOCKDOWN_OPEN = ("/access/health", "/access/owner-login", "/access/owner-cookie")   # при рубильнике: живость + вход владельца
AUTH_FAIL_LIMIT, AUTH_FAIL_WINDOW = 10, 600                               # неверных ключей/кодов с одного IP за окно (с)
BODY_CAP = 40 * 1024 * 1024                                               # ответ больше — расход не разбираем
COOKIE_OWNER, COOKIE_TOKEN = "sa_owner", "sa_token"
_CODE_ABC = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"                             # без похожих 0/O, 1/I/L


def owner_key() -> str:
    return (os.environ.get("ACCESS_OWNER_KEY") or "").strip()


def gate_on() -> bool:
    return bool(owner_key())


def lockdown() -> bool:
    return (os.environ.get("ACCESS_LOCKDOWN") or "").strip().lower() in ("1", "true", "on", "yes")


def open_paths() -> set:
    extra = {p.strip() for p in (os.environ.get("ACCESS_OPEN_PATHS") or "").split(",") if p.strip()}
    return OPEN_PATHS | extra


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _owner_cookie_value() -> str:
    return _sha("sa-owner-cookie:" + owner_key())     # смена ключа владельца гасит и cookie


def _eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def _now() -> int:
    return int(time.time())


def _day_start(ts: Optional[int] = None) -> int:
    ts = _now() if ts is None else ts
    return ts - ts % 86400                              # сутки UTC


# ══════════════════════════ база ══════════════════════════

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT, role TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', email TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '', token_hash TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL, last_seen INTEGER, invite_code TEXT, daily_runs INTEGER, daily_cost REAL,
  credit REAL NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS invites (
  code TEXT PRIMARY KEY, note TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, used_by INTEGER, used_at INTEGER,
  revoked INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL,
  method TEXT NOT NULL, path TEXT NOT NULL, status INTEGER NOT NULL, cost REAL, wall_ms INTEGER, seed TEXT,
  charged REAL NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS usage_user_ts ON usage(user_id, ts);
CREATE INDEX IF NOT EXISTS usage_ts ON usage(ts);
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
  target TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '', ip TEXT NOT NULL DEFAULT '');
"""


class _DB:
    """Один SQLite-файл, одна блокировка: операции короткие (мс), пользователей — два десятка."""

    def __init__(self):
        self.lock = threading.RLock()
        self.conn: Optional[sqlite3.Connection] = None
        self.path = ""

    def open(self, path: Optional[str] = None):
        with self.lock:
            if self.conn is not None:
                self.conn.close()
            p = path or os.environ.get("ACCESS_DB") or ("/var/data/access.sqlite" if os.path.isdir("/var/data")
                                                          else "./access.sqlite")
            self.conn = sqlite3.connect(p, check_same_thread=False, isolation_level=None)   # autocommit
            self.conn.row_factory = sqlite3.Row
            try:
                self.conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
            self.conn.executescript(_SCHEMA)
            for table, col, ddl in (("users", "credit", "credit REAL NOT NULL DEFAULT 0"),       # база от ag_0.1 → ag_0.2
                                    ("usage", "charged", "charged REAL NOT NULL DEFAULT 0")):
                cols = {r[1] for r in self.conn.execute(f"PRAGMA table_info({table})").fetchall()}
                if col not in cols:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
            self.path = p

    def q(self, sql: str, args: tuple = ()) -> list:
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def one(self, sql: str, args: tuple = ()) -> Optional[dict]:
        rows = self.q(sql, args)
        return rows[0] if rows else None

    def x(self, sql: str, args: tuple = ()) -> int:
        with self.lock:
            return self.conn.execute(sql, args).lastrowid


db = _DB()


def audit(actor: str, action: str, target: str = "", detail: str = "", ip: str = ""):
    db.x("INSERT INTO audit(ts, actor, action, target, detail, ip) VALUES (?,?,?,?,?,?)",
         (_now(), actor, action, str(target), str(detail)[:500], ip))


# ══════════════════════════ кто пришёл ══════════════════════════

_fails: dict = {}          # ip → [ts неверных попыток]; в памяти процесса — перезапуск обнуляет, этого достаточно
_fails_lock = threading.Lock()


def _fail_add(ip: str):
    with _fails_lock:
        now = _now()
        _fails[ip] = [t for t in _fails.get(ip, []) if now - t < AUTH_FAIL_WINDOW] + [now]


def _fail_blocked(ip: str) -> bool:
    with _fails_lock:
        now = _now()
        lst = [t for t in _fails.get(ip, []) if now - t < AUTH_FAIL_WINDOW]
        _fails[ip] = lst
        return len(lst) >= AUTH_FAIL_LIMIT


def _client_ip(headers: dict, scope_client) -> str:
    xff = headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return scope_client[0] if scope_client else ""


def _token_from(headers: dict) -> str:
    a = headers.get("authorization", "")
    if a.lower().startswith("bearer "):
        return a[7:].strip()
    return headers.get("x-access-token", "").strip()


def _cookies(headers: dict) -> dict:
    raw = headers.get("cookie", "")
    if not raw:
        return {}
    c = SimpleCookie()
    try:
        c.load(raw)
    except Exception:  # noqa: BLE001
        return {}
    return {k: v.value for k, v in c.items()}


def identify(headers: dict) -> tuple:
    """→ (user | None, presented: bool). user: {"id", "role", "name", "enabled", "credit"}.
    presented — какой-то ключ предъявлен (для счётчика неверных попыток)."""
    token = _token_from(headers)
    ck = _cookies(headers)
    key = owner_key()
    if key:
        if token and _eq(token, key):
            return {"id": 0, "role": "owner", "name": "owner", "enabled": 1}, True
        if ck.get(COOKIE_OWNER) and _eq(ck[COOKIE_OWNER], _owner_cookie_value()):
            return {"id": 0, "role": "owner", "name": "owner", "enabled": 1}, True
    tok = token or ck.get(COOKIE_TOKEN, "")
    if tok:
        u = db.one("SELECT id, role, name, enabled, credit FROM users WHERE token_hash = ?", (_sha(tok),))
        return u, True
    return None, False


def usage_today(user_id: int) -> dict:
    d0 = _day_start()
    r = db.one("SELECT COALESCE(SUM(cost),0) AS cost, COUNT(*) AS requests FROM usage WHERE user_id = ? AND ts >= ?",
               (user_id, d0))
    marks = ",".join("?" * len(RUN_START_PATHS))
    runs = db.one(f"SELECT COUNT(*) AS n FROM usage WHERE user_id = ? AND ts >= ? AND status < 400 AND path IN ({marks})",
                  (user_id, d0) + RUN_START_PATHS)
    return {"cost": round(r["cost"], 5), "requests": r["requests"], "runs": runs["n"]}


def balance_of(u: dict) -> dict:
    """Баланс тестера в ценах с наценкой: пополнено − списано."""
    spent = db.one("SELECT COALESCE(SUM(charged),0) AS c FROM usage WHERE user_id = ?", (u["id"],))["c"]
    credit = u.get("credit") or 0.0
    return {"credit": round(credit, 4), "spent": round(spent, 4), "balance": round(credit - spent, 4)}


def _in_run_grace(user_id: int) -> bool:
    marks = ",".join("?" * len(RUN_START_PATHS))
    r = db.one(f"SELECT MAX(ts) AS t FROM usage WHERE user_id = ? AND status < 400 AND path IN ({marks})",
               (user_id,) + RUN_START_PATHS)
    return bool(r and r["t"] and _now() - r["t"] <= RUN_GRACE_SEC)


# ag_0.1 (точка отката, суточные лимиты):
# def testers_cost_today() -> float:
#     r = db.one("SELECT COALESCE(SUM(cost),0) AS cost FROM usage WHERE role = 'tester' AND ts >= ?", (_day_start(),))
#     return r["cost"]
# def user_limits(u: dict) -> dict:
#     return {"daily_runs": u.get("daily_runs") if u.get("daily_runs") is not None else TESTER_DAILY_RUNS,
#             "daily_cost": u.get("daily_cost") if u.get("daily_cost") is not None else TESTER_DAILY_COST}


def check_access(u: Optional[dict], method: str, path: str) -> Optional[tuple]:
    """→ None (пропустить) или (status, code, текст)."""
    owner_only = any(path.startswith(p) for p in OWNER_ONLY_PREFIXES)
    is_open = (path in open_paths() or any(path.startswith(p) for p in OPEN_PREFIXES)) and not owner_only
    if u and u["role"] == "owner":
        return None
    if lockdown():                                            # "/" остаётся открытым — иначе Render сочтёт сервис упавшим
        if path in open_paths() or path in LOCKDOWN_OPEN:
            return None
        return 503, "lockdown", "Сервис временно закрыт владельцем"
    if is_open:
        return None
    if u is None:
        return 401, "no_token", "Нужен токен доступа"
    if not u["enabled"]:
        return 403, "disabled", "Доступ отключён владельцем"
    if owner_only:
        return 403, "owner_only", "Только для владельца"
    if u["role"] == "super":
        return None                                           # без лимитов (решение Andrew), расходы считаются
    if not path.startswith("/api/"):
        return 403, "api_only", "Тестеру доступен только /api/"
    # ag_0.1 (точка отката): суточные лимиты
    # lim, today = user_limits(u), usage_today(u["id"])
    # if path in RUN_START_PATHS and today["runs"] >= lim["daily_runs"]:
    #     return 429, "limit_runs", f"Лимит запусков на сегодня исчерпан ({lim['daily_runs']})"
    # if today["cost"] >= lim["daily_cost"]:
    #     return 429, "limit_cost", "Дневной лимит расхода исчерпан"
    # if testers_cost_today() >= GLOBAL_DAILY_COST:
    #     return 429, "global_cap", "Общий дневной лимит теста исчерпан, попробуйте завтра"
    bal = balance_of(u)["balance"]                            # ag_0.2: единственное ограничение тестера — баланс
    need = MIN_BALANCE.get(path)
    if need is not None:
        if bal < need:
            return 402, "low_balance", (f"Недостаточно средств: для запуска нужно не менее ${need:.2f}, "
                                        f"на балансе ${max(bal, 0):.2f}. Пополните баланс.")
    elif bal <= 0 and not (path in RUN_FOLLOW_PATHS and _in_run_grace(u["id"])):
        return 402, "low_balance", "Баланс исчерпан. Пополните баланс."
    return None


# ══════════════════════════ расход из ответа ══════════════════════════

_MODULE_COST_PATHS = ("/api/intent-map", "/api/content-map", "/api/client-portrait", "/api/minus-semantics",
                      "/api/minus-wide")


def _num(v) -> float:
    try:
        f = float(v)
        return f if f == f and f not in (float("inf"), float("-inf")) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _stage_cost(s) -> float:
    """= stageCost() в autopilot.html: cost_usd, иначе cost; блок с error — 0."""
    if not isinstance(s, dict) or s.get("error"):
        return 0.0
    v = s.get("cost_usd")
    if v is None:
        v = s.get("cost")
    return _num(v)


def extract_cost(path: str, d) -> Optional[float]:
    """Расход запроса из тела ответа — та же арифметика, что Total в autopilot.html. Нет данных о цене → None."""
    if not isinstance(d, dict):
        return None
    if path in _MODULE_COST_PATHS:
        st = d.get("stats")
        return _num(st.get("total_cost")) if isinstance(st, dict) and st.get("total_cost") is not None else None
    found, total = False, 0.0
    for key in ("l2_5_stats", "l25_stats", "l2_5_filter_stats"):      # один и тот же блок под разными именами — берём первый
        if isinstance(d.get(key), dict):
            total += _stage_cost(d[key]); found = True
            break
    for key in ("l3_stats", "geo_exist_stats"):
        if isinstance(d.get(key), dict):
            total += _stage_cost(d[key]); found = True
    m = d.get("metrics")                                              # /api/test-clustering
    if isinstance(m, dict) and m.get("cost_usd") is not None:
        total += _num(m["cost_usd"]); found = True
    cl = d.get("clustering")                                          # /api/relevant-search: кластеры одним вызовом
    if isinstance(cl, dict) and not cl.get("error") and isinstance(cl.get("metrics"), dict):
        total += _num(cl["metrics"].get("cost_usd")); found = True
    rs = (d.get("relevant") or {}).get("stats") if isinstance(d.get("relevant"), dict) else None
    if isinstance(rs, dict):                                          # генерация вариантов
        if rs.get("total_cost") is not None:
            total += _num(rs["total_cost"])
        else:
            total += _num((rs.get("gen") or {}).get("cost")) + _num((rs.get("ver") or {}).get("cost"))
        found = True
    if not found:                                                     # новый модуль по общему контракту stats.total_cost
        st = d.get("stats")
        if isinstance(st, dict) and st.get("total_cost") is not None:
            return round(_num(st["total_cost"]), 6)
    return round(total, 6) if found else None


# ══════════════════════════ ответ для тестера: цены × наценка, внутренности вырезаны ══════════════════════════

_COST_KEY = re.compile(r"(^|_)cost(_|$)", re.I)             # cost, cost_usd, total_cost, cost_cross, l3_cost_usd, _cost_usd
_MODEL_KEY = re.compile(r"^(model|models|by)$|(^|_)model(_|$)", re.I)
# ag_0.3 (точка отката):
# _DROP_KEY = re.compile(r"^(stages|stage|raw|chunk_stats|chunk_errors|by|model|models|in|out|think|price|prices|"
#                        r"prompt_chars)$|token|thinking|effort|budget|temperature|(^|_)model(_|$)", re.I)
# ag_0.4: живой прогон тестера показал в l2_5_stats / l3_stats price_in / price_out (реальные тарифы за 1M — по ним
# угадывается модель и обратно считается наценка), provider ("openai") и build с названием промпта → режем всё с price/
# provider/build/prompt/classification; last_cost/cost_* остаются под _COST_KEY (масштабируются).
_DROP_KEY = re.compile(r"^(stages|stage|raw|chunk_stats|chunk_errors|by|model|models|in|out|think|prompt_chars|"
                       r"classification)$|token|thinking|effort|budget|temperature|price|provider|(^|_)build(_|$)|prompt|"
                       r"(^|_)model(_|$)", re.I)
_seen_models: set = set()      # названия моделей, встреченные в любых ответах процесса (в т.ч. владельца) — для замены в строках
_seen_lock = threading.Lock()


def _collect_models(x):
    """Строки под ключами model / models / by / *_model — это названия моделей; запоминаем, чтобы убрать их и из
    прочих строк ответа (например, метка источника у срезанного ключа)."""
    if isinstance(x, dict):
        for k, v in x.items():
            if isinstance(k, str) and _MODEL_KEY.search(k):
                for m in (v if isinstance(v, list) else [v]):
                    if isinstance(m, str) and len(m.strip()) >= 3:
                        with _seen_lock:
                            _seen_models.add(m.strip())
            _collect_models(v)
    elif isinstance(x, list):
        for v in x:
            _collect_models(v)


def _mask(text: str, models: list) -> str:
    for m in models:
        if m in text:
            text = text.replace(m, "llm")
    return text


def _scale(v):
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, (int, float)):
        return round(v * PRICE_MULT, 6)
    if isinstance(v, str):
        try:
            return f"{float(v) * PRICE_MULT:.6f}"
        except ValueError:
            return v
    return v


def _scrub(x, models: list, under_cost: bool = False):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            ks = k if isinstance(k, str) else str(k)
            if _DROP_KEY.search(ks):
                continue
            is_cost = under_cost or bool(_COST_KEY.search(ks))
            nk = _mask(ks, models)
            while nk in out:
                nk += "_"
            out[nk] = _scrub(v, models, is_cost)
        return out
    if isinstance(x, list):
        return [_scrub(v, models, under_cost) for v in x]
    if under_cost:
        return _scale(x)
    if isinstance(x, str):
        return _mask(x, models)
    return x


def scrub_for_tester(d):
    """Копия ответа для тестера. Ключевые слова и тексты не трогаются: замена идёт только по ТОЧНЫМ названиям моделей,
    встреченным в ответах (не по словам-вендорам — «gemini часы» в ключах остаётся как есть)."""
    _collect_models(d)
    with _seen_lock:
        models = sorted(_seen_models, key=len, reverse=True)
    return _scrub(d, models)


# ══════════════════════════ ворота (чистый ASGI: не буферизует ответ, не мешает долгим запросам) ══════════════════════════

_active: set = set()            # тестеры с идущим платным запросом (в памяти процесса; сервер — один процесс)
_active_lock = threading.Lock()


def _ctype(message) -> bytes:
    for k, v in message.get("headers", []):
        if k.lower() == b"content-type":
            return v
    return b""


def _query_seed(scope) -> str:
    return (parse_qs(scope.get("query_string", b"").decode("latin-1")).get("seed") or [""])[0]


_CORS = [(b"access-control-allow-origin", b"*")]    # ворота стоят снаружи CORSMiddleware — отказ должен читаться из file://


class AccessGateMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not gate_on():
            return await self.app(scope, receive, send)
        method, path = scope["method"], scope["path"]
        if method == "OPTIONS":                                       # CORS-preflight идёт без токена
            return await self.app(scope, receive, send)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        ip = _client_ip(headers, scope.get("client"))

        user, presented = identify(headers)
        if presented and user is None:
            if _fail_blocked(ip):
                return await self._reject(scope, receive, send, 429, "too_many", "Слишком много неверных ключей, подождите")
            _fail_add(ip)
        deny = check_access(user, method, path)
        if deny:
            status, code, text = deny
            if user and user["role"] != "owner":
                self._safe_record(user, method, path, status, None, 0.0, time.perf_counter(), "")
            return await self._reject(scope, receive, send, status, code, text)

        if user is None:                                              # открытый путь без токена — не считаем
            return await self.app(scope, receive, send)
        scope.setdefault("state", {})["access_user"] = {"id": user["id"], "role": user["role"], "name": user["name"]}

        meter = path.startswith("/api/")
        if not meter:                                                 # /access/*, страницы — без учёта и без очереди
            return await self.app(scope, receive, send)
        if user["role"] == "tester":
            return await self._serve_tester(scope, receive, send, user, method, path)

        # владелец / супер: ответ уходит как есть и сразу, копия — только для подсчёта реальной цены
        t0 = time.perf_counter()
        box = {"status": 500, "json": False, "chunks": [], "size": 0, "overflow": False}

        async def send_wrap(message):
            if message["type"] == "http.response.start":
                box["status"] = message["status"]
                box["json"] = b"json" in _ctype(message).lower()
            elif message["type"] == "http.response.body" and box["json"] and not box["overflow"]:
                chunk = message.get("body", b"")
                box["size"] += len(chunk)
                if box["size"] > BODY_CAP:
                    box["overflow"], box["chunks"] = True, []
                else:
                    box["chunks"].append(chunk)
            await send(message)

        try:
            await self.app(scope, receive, send_wrap)
        finally:
            cost, seed = None, ""
            if box["json"] and not box["overflow"] and box["chunks"]:
                try:
                    d = json.loads(b"".join(box["chunks"]))
                    _collect_models(d)
                    cost = extract_cost(path, d)
                    if isinstance(d, dict) and isinstance(d.get("seed"), str):
                        seed = d["seed"]
                except Exception:  # noqa: BLE001
                    pass
            self._safe_record(user, method, path, box["status"], cost, 0.0, t0, seed or _query_seed(scope))

    async def _serve_tester(self, scope, receive, send, user, method, path):
        """Тестер: один платный запрос одновременно; JSON-ответ придерживается целиком, очищается и уходит одной частью."""
        uid = user["id"]
        with _active_lock:
            if uid in _active:
                busy = True
            else:
                busy = False
                _active.add(uid)
        if busy:
            self._safe_record(user, method, path, 429, None, 0.0, time.perf_counter(), "")
            return await self._reject(scope, receive, send, 429, "busy", "Дождитесь завершения предыдущей операции")

        t0 = time.perf_counter()
        box = {"status": 500, "start": None, "json": False, "chunks": [], "size": 0, "failed": False,
               "cost": None, "seed": ""}

        async def send_wrap(message):
            if message["type"] == "http.response.start":
                box["status"] = message["status"]
                box["json"] = b"json" in _ctype(message).lower()
                if not box["json"]:
                    return await send(message)                       # не JSON (файл, текст) — отдаём как есть
                box["start"] = message                               # JSON — придерживаем до конца тела
                return
            if message["type"] != "http.response.body" or not box["json"]:
                return await send(message)
            if box["failed"]:
                return
            box["size"] += len(message.get("body", b""))
            if box["size"] > BODY_CAP:
                box["failed"] = True
                return await self._fail_closed(send, box)
            box["chunks"].append(message.get("body", b""))
            if message.get("more_body"):
                return
            if not box["size"]:                                       # JSON-заголовок с пустым телом (204 и т.п.)
                await send(box["start"])
                return await send(message)
            try:
                d = json.loads(b"".join(box["chunks"]))
                box["cost"] = extract_cost(path, d)
                if isinstance(d, dict) and isinstance(d.get("seed"), str):
                    box["seed"] = d["seed"]
                body = json.dumps(scrub_for_tester(d), ensure_ascii=False).encode("utf-8")
            except Exception:  # noqa: BLE001 — разобрать не вышло: реальные цены и модели могли бы утечь → не отдаём
                box["failed"] = True
                return await self._fail_closed(send, box)
            headers = [(k, v) for k, v in box["start"].get("headers", []) if k.lower() != b"content-length"]
            headers.append((b"content-length", str(len(body)).encode()))
            await send({"type": "http.response.start", "status": box["status"], "headers": headers})
            await send({"type": "http.response.body", "body": body, "more_body": False})

        try:
            await self.app(scope, receive, send_wrap)
        finally:
            with _active_lock:
                _active.discard(uid)
            cost = box["cost"]
            charged = round(cost * PRICE_MULT, 6) if cost else 0.0
            self._safe_record(user, method, path, box["status"], cost, charged, t0, box["seed"] or _query_seed(scope))

    @staticmethod
    async def _fail_closed(send, box):
        body = json.dumps({"error": "Ошибка обработки ответа, операция не тарифицируется — сообщите владельцу",
                           "code": "scrub_failed"}, ensure_ascii=False).encode("utf-8")
        box["status"] = 502
        await send({"type": "http.response.start", "status": 502,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
                    + _CORS})
        await send({"type": "http.response.body", "body": body, "more_body": False})

    def _safe_record(self, user, method, path, status, cost, charged, t0, seed):
        try:
            self._record(user, method, path, status, cost, charged, int((time.perf_counter() - t0) * 1000), seed)
        except Exception:  # noqa: BLE001 — учёт не должен ронять ответ
            pass

    @staticmethod
    def _record(user, method, path, status, cost, charged, wall_ms, seed):
        now = _now()
        db.x("INSERT INTO usage(ts, user_id, role, method, path, status, cost, wall_ms, seed, charged) "
             "VALUES (?,?,?,?,?,?,?,?,?,?)",
             (now, user["id"], user["role"], method, path[:200], status, cost, wall_ms, (seed or "")[:200], charged))
        if user["id"]:
            db.x("UPDATE users SET last_seen = ? WHERE id = ?", (now, user["id"]))

    @staticmethod
    async def _reject(scope, receive, send, status, code, text):
        resp = JSONResponse({"error": text, "code": code}, status_code=status)
        resp.raw_headers.extend(_CORS)
        await resp(scope, receive, send)


# ══════════════════════════ эндпоинты /access/* ══════════════════════════

router = APIRouter()


def _hdrs(request: Request) -> dict:
    return {k.lower(): v for k, v in request.headers.items()}


def _ip(request: Request) -> str:
    return _client_ip(_hdrs(request), (request.client.host, 0) if request.client else None)


def _owner_or_error(request: Request) -> Optional[JSONResponse]:
    """Вторая линия: ворота уже закрыли /access/admin/, но каждый админ-эндпоинт проверяет владельца сам."""
    if not gate_on():
        return JSONResponse({"error": "Ворота не настроены: нет ACCESS_OWNER_KEY", "code": "gate_off"}, status_code=503)
    u, _ = identify(_hdrs(request))
    if not u or u["role"] != "owner":
        return JSONResponse({"error": "Только для владельца", "code": "owner_only"}, status_code=403)
    return None


def _new_token() -> str:
    return "sa_" + secrets.token_urlsafe(32)


def _new_code() -> str:
    s = "".join(secrets.choice(_CODE_ABC) for _ in range(12))
    return f"{s[:4]}-{s[4:8]}-{s[8:]}"


def _norm_code(code: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]", "", code or "").upper()
    return f"{s[:4]}-{s[4:8]}-{s[8:]}" if len(s) == 12 else s


@router.get("/access/health")
async def access_health():
    return {"ok": True, "gate": gate_on(), "lockdown": lockdown(), "build": BUILD}


class ActivateReq(BaseModel):
    code: str
    name: str
    email: str = ""


@router.post("/access/activate")
async def access_activate(req: ActivateReq, request: Request):
    """Инвайт-код → токен тестера. Код одноразовый."""
    if not gate_on():
        return JSONResponse({"error": "Регистрация ещё не открыта", "code": "gate_off"}, status_code=503)
    ip = _ip(request)
    if _fail_blocked(ip):
        return JSONResponse({"error": "Слишком много попыток, подождите 10 минут", "code": "too_many"}, status_code=429)
    code, name = _norm_code(req.code), (req.name or "").strip()[:80]
    if not name:
        return JSONResponse({"error": "Укажите имя", "code": "no_name"}, status_code=400)
    with db.lock:                                                      # проверка и запись — одним куском
        inv = db.one("SELECT * FROM invites WHERE code = ?", (code,))
        if not inv or inv["revoked"] or inv["used_by"]:
            _fail_add(ip)
            return JSONResponse({"error": "Код недействителен или уже использован", "code": "bad_code"}, status_code=403)
        token = _new_token()
        uid = db.x("INSERT INTO users(role, name, email, token_hash, created_at, invite_code, credit) "
                   "VALUES ('tester',?,?,?,?,?,?)",
                   (name, (req.email or "").strip()[:120], _sha(token), _now(), code, TESTER_START_CREDIT))
        db.x("UPDATE invites SET used_by = ?, used_at = ? WHERE code = ?", (uid, _now(), code))
    audit("tester:" + str(uid), "activate", code, name, ip)
    return {"token": token, "role": "tester", "name": name, "balance": TESTER_START_CREDIT,
            "min_balance": MIN_BALANCE, "note": "Токен показывается один раз — сохраните его"}


@router.get("/access/me")
async def access_me(request: Request):
    if not gate_on():
        return {"gate": False}
    u, _ = identify(_hdrs(request))
    if not u:
        return JSONResponse({"error": "Нужен токен доступа", "code": "no_token"}, status_code=401)
    out = {"gate": True, "role": u["role"], "name": u["name"], "enabled": bool(u["enabled"]), "lockdown": lockdown()}
    if u["role"] == "tester":                                      # тестеру — только баланс в его ценах
        out.update(balance_of(u)); out["min_balance"] = MIN_BALANCE
    else:                                                          # супер и владелец — реальный расход за сегодня
        out["today"] = usage_today(u["id"])
    return out


class OwnerKeyReq(BaseModel):
    key: str


@router.post("/access/owner-cookie")
async def access_owner_cookie(req: OwnerKeyReq, request: Request):
    """Ключ владельца → cookie на этом домене: открывает страницы стендов и /docs на самом сервере."""
    if not gate_on():
        return JSONResponse({"error": "Ворота не настроены", "code": "gate_off"}, status_code=503)
    ip = _ip(request)
    if _fail_blocked(ip):
        return JSONResponse({"error": "Слишком много попыток, подождите 10 минут", "code": "too_many"}, status_code=429)
    if not _eq((req.key or "").strip(), owner_key()):
        _fail_add(ip)
        audit("?", "owner_cookie_fail", "", "", ip)
        return JSONResponse({"error": "Неверный ключ", "code": "bad_key"}, status_code=403)
    resp = JSONResponse({"ok": True})
    resp.set_cookie(COOKIE_OWNER, _owner_cookie_value(), max_age=30 * 86400, httponly=True, secure=True, samesite="lax")
    audit("owner", "owner_cookie", "", "", ip)
    return resp


@router.get("/access/owner-login")
async def access_owner_login():
    return HTMLResponse("""<!doctype html><meta charset="utf-8"><title>Вход владельца</title>
<body style="font:15px system-ui;max-width:420px;margin:80px auto;padding:0 16px">
<h3>Вход владельца</h3><p style="color:#666">Ключ из переменной ACCESS_OWNER_KEY. Открывает страницы стендов на этом сервере на 30 дней.</p>
<input id="k" type="password" placeholder="ключ владельца" style="width:100%;padding:10px;box-sizing:border-box">
<button onclick="go()" style="margin-top:10px;padding:10px 18px">Войти</button><p id="m"></p>
<script>async function go(){var r=await fetch('/access/owner-cookie',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({key:document.getElementById('k').value})});var d=await r.json();
document.getElementById('m').textContent=r.ok?'Готово — страницы сервера открыты.':(d.error||'Ошибка');}</script>""")


# ─── админка владельца ───

def _user_row(u: dict) -> dict:
    d0, d7 = _day_start(), _day_start() - 6 * 86400
    agg = db.one("""SELECT COALESCE(SUM(cost),0) AS total, COALESCE(SUM(CASE WHEN ts >= ? THEN cost END),0) AS d7,
                           COALESCE(SUM(charged),0) AS charged, COUNT(*) AS requests FROM usage WHERE user_id = ?""",
                 (d7, u["id"]))
    marks = ",".join("?" * len(RUN_START_PATHS))
    runs = db.one(f"SELECT COUNT(*) AS n FROM usage WHERE user_id = ? AND status < 400 AND path IN ({marks})",
                  (u["id"],) + RUN_START_PATHS)
    return {"id": u["id"], "role": u["role"], "name": u["name"], "email": u["email"], "note": u["note"],
            "enabled": bool(u["enabled"]), "created_at": u["created_at"], "last_seen": u["last_seen"],
            "invite_code": u["invite_code"], "today": usage_today(u["id"]),
            "cost_7d": round(agg["d7"], 5), "cost_total": round(agg["total"], 5), "requests_total": agg["requests"],
            "runs_total": runs["n"],
            # тестер: пополнено / списано (с наценкой) / остаток и маржа = списано − реальная цена
            "billing": ({**balance_of(u), "margin": round(agg["charged"] - agg["total"], 5)}
                        if u["role"] == "tester" else None)}


@router.get("/access/admin/overview")
async def admin_overview(request: Request):
    err = _owner_or_error(request)
    if err:
        return err
    users = [_user_row(u) for u in db.q("SELECT * FROM users ORDER BY role, id")]
    owner_today = usage_today(0)
    all_today = db.one("SELECT COALESCE(SUM(cost),0) AS cost FROM usage WHERE ts >= ?", (_day_start(),))["cost"]
    all_total = db.one("SELECT COALESCE(SUM(cost),0) AS cost FROM usage")["cost"]
    charged_total = db.one("SELECT COALESCE(SUM(charged),0) AS c FROM usage")["c"]
    return {"gate": True, "lockdown": lockdown(), "build": BUILD, "db": db.path,
            "owner_key_short": len(owner_key()) < 32,
            "defaults": {"max_supers": MAX_SUPERS, "max_testers": MAX_TESTERS, "tester_start_credit": TESTER_START_CREDIT,
                         "price_mult": PRICE_MULT, "min_balance": MIN_BALANCE},
            "spend": {"today_all": round(all_today, 5), "today_owner": owner_today["cost"],       # реальные цены
                      "total_all": round(all_total, 5), "charged_total": round(charged_total, 5)},
            "users": users,
            "invites": db.q("SELECT * FROM invites ORDER BY created_at DESC")}


class InvitesReq(BaseModel):
    count: int = 1
    note: str = ""


@router.post("/access/admin/invites")
async def admin_invites(req: InvitesReq, request: Request):
    err = _owner_or_error(request)
    if err:
        return err
    with db.lock:
        testers = db.one("SELECT COUNT(*) AS n FROM users WHERE role = 'tester'")["n"]
        pending = db.one("SELECT COUNT(*) AS n FROM invites WHERE used_by IS NULL AND revoked = 0")["n"]
        room = MAX_TESTERS - testers - pending
        n = max(0, min(int(req.count or 0), room))
        if n <= 0:
            return JSONResponse({"error": f"Мест нет: тестеров {testers} + неиспользованных кодов {pending} = лимит "
                                          f"{MAX_TESTERS}", "code": "no_room"}, status_code=409)
        codes = []
        for _ in range(n):
            c = _new_code()
            db.x("INSERT INTO invites(code, note, created_at) VALUES (?,?,?)", (c, (req.note or "")[:200], _now()))
            codes.append(c)
    audit("owner", "invites_create", str(n), req.note, _ip(request))
    return {"codes": codes, "room_left": room - n}


@router.post("/access/admin/invite/{code}/revoke")
async def admin_invite_revoke(code: str, request: Request):
    err = _owner_or_error(request)
    if err:
        return err
    c = _norm_code(code)
    inv = db.one("SELECT * FROM invites WHERE code = ?", (c,))
    if not inv:
        return JSONResponse({"error": "Код не найден", "code": "not_found"}, status_code=404)
    if inv["used_by"]:
        return JSONResponse({"error": "Код уже использован — отключайте пользователя", "code": "used"}, status_code=409)
    db.x("UPDATE invites SET revoked = 1 WHERE code = ?", (c,))
    audit("owner", "invite_revoke", c, "", _ip(request))
    return {"ok": True}


class SuperReq(BaseModel):
    name: str
    email: str = ""
    note: str = ""


@router.post("/access/admin/super")
async def admin_super(req: SuperReq, request: Request):
    err = _owner_or_error(request)
    if err:
        return err
    name = (req.name or "").strip()[:80]
    if not name:
        return JSONResponse({"error": "Укажите имя", "code": "no_name"}, status_code=400)
    with db.lock:
        n = db.one("SELECT COUNT(*) AS n FROM users WHERE role = 'super'")["n"]
        if n >= MAX_SUPERS:
            return JSONResponse({"error": f"Суперпользователей уже {n} из {MAX_SUPERS}: переименуйте слот и "
                                          f"перевыпустите токен", "code": "no_room"}, status_code=409)
        token = _new_token()
        uid = db.x("INSERT INTO users(role, name, email, note, token_hash, created_at) VALUES ('super',?,?,?,?,?)",
                   (name, (req.email or "").strip()[:120], (req.note or "")[:200], _sha(token), _now()))
    audit("owner", "super_create", str(uid), name, _ip(request))
    return {"id": uid, "token": token, "note": "Токен показывается один раз — передайте его человеку сейчас"}


class TesterReq(BaseModel):
    name: str
    email: str = ""
    note: str = ""


@router.post("/access/admin/tester")
async def admin_tester(req: TesterReq, request: Request):
    """Тестер напрямую: токен показан один раз, баланс стартовый. Место общее с инвайт-кодами."""
    err = _owner_or_error(request)
    if err:
        return err
    name = (req.name or "").strip()[:80]
    if not name:
        return JSONResponse({"error": "Укажите имя", "code": "no_name"}, status_code=400)
    with db.lock:
        testers = db.one("SELECT COUNT(*) AS n FROM users WHERE role = 'tester'")["n"]
        pending = db.one("SELECT COUNT(*) AS n FROM invites WHERE used_by IS NULL AND revoked = 0")["n"]
        if testers + pending >= MAX_TESTERS:
            return JSONResponse({"error": f"Мест нет: тестеров {testers} + неиспользованных кодов {pending} = лимит "
                                          f"{MAX_TESTERS}", "code": "no_room"}, status_code=409)
        token = _new_token()
        uid = db.x("INSERT INTO users(role, name, email, note, token_hash, created_at, credit) "
                   "VALUES ('tester',?,?,?,?,?,?)",
                   (name, (req.email or "").strip()[:120], (req.note or "")[:200], _sha(token), _now(),
                    TESTER_START_CREDIT))
    audit("owner", "tester_create", str(uid), name, _ip(request))
    return {"id": uid, "token": token, "balance": TESTER_START_CREDIT,
            "note": "Токен показывается один раз — передайте его человеку сейчас"}


def _get_user(uid: int) -> Optional[dict]:
    return db.one("SELECT * FROM users WHERE id = ?", (uid,))


@router.post("/access/admin/user/{uid}/enable")
async def admin_user_enable(uid: int, request: Request):
    return await _set_enabled(uid, 1, request)


@router.post("/access/admin/user/{uid}/disable")
async def admin_user_disable(uid: int, request: Request):
    return await _set_enabled(uid, 0, request)


async def _set_enabled(uid: int, val: int, request: Request):
    err = _owner_or_error(request)
    if err:
        return err
    if not _get_user(uid):
        return JSONResponse({"error": "Пользователь не найден", "code": "not_found"}, status_code=404)
    db.x("UPDATE users SET enabled = ? WHERE id = ?", (val, uid))
    audit("owner", "user_enable" if val else "user_disable", str(uid), "", _ip(request))
    return {"ok": True, "id": uid, "enabled": bool(val)}


@router.post("/access/admin/user/{uid}/reissue")
async def admin_user_reissue(uid: int, request: Request):
    """Новый токен; старый перестаёт работать в ту же секунду."""
    err = _owner_or_error(request)
    if err:
        return err
    if not _get_user(uid):
        return JSONResponse({"error": "Пользователь не найден", "code": "not_found"}, status_code=404)
    token = _new_token()
    db.x("UPDATE users SET token_hash = ? WHERE id = ?", (_sha(token), uid))
    audit("owner", "user_reissue", str(uid), "", _ip(request))
    return {"id": uid, "token": token, "note": "Токен показывается один раз"}


class UserUpdateReq(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    note: Optional[str] = None
    # daily_runs / daily_cost — ag_0.1, отменено (лимит только балансом; пополнение — /topup)


@router.post("/access/admin/user/{uid}/update")
async def admin_user_update(uid: int, req: UserUpdateReq, request: Request):
    err = _owner_or_error(request)
    if err:
        return err
    u = _get_user(uid)
    if not u:
        return JSONResponse({"error": "Пользователь не найден", "code": "not_found"}, status_code=404)
    sets, args = [], []
    for field, val, cap in (("name", req.name, 80), ("email", req.email, 120), ("note", req.note, 200)):
        if val is not None:
            sets.append(f"{field} = ?"); args.append(val.strip()[:cap])
    if not sets:
        return JSONResponse({"error": "Нечего менять", "code": "empty"}, status_code=400)
    db.x(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", tuple(args) + (uid,))
    audit("owner", "user_update", str(uid), json.dumps(req.model_dump(exclude_none=True), ensure_ascii=False), _ip(request))
    return {"ok": True, "user": _user_row(_get_user(uid))}


class TopupReq(BaseModel):
    amount: float                          # $ в ценах тестера; отрицательное — корректировка вниз


@router.post("/access/admin/user/{uid}/topup")
async def admin_user_topup(uid: int, req: TopupReq, request: Request):
    """Пополнить баланс тестера вручную (до подключения оплаты — единственный способ продлить доступ)."""
    err = _owner_or_error(request)
    if err:
        return err
    u = _get_user(uid)
    if not u:
        return JSONResponse({"error": "Пользователь не найден", "code": "not_found"}, status_code=404)
    if u["role"] != "tester":
        return JSONResponse({"error": "Баланс есть только у тестера", "code": "not_tester"}, status_code=400)
    db.x("UPDATE users SET credit = credit + ? WHERE id = ?", (float(req.amount), uid))
    audit("owner", "user_topup", str(uid), str(req.amount), _ip(request))
    return {"ok": True, "user": _user_row(_get_user(uid))}


@router.post("/access/admin/revoke-all")
async def admin_revoke_all(request: Request):
    """Отключить всех (и тестеров, и суперпользователей) одним действием. Включаются потом по одному."""
    err = _owner_or_error(request)
    if err:
        return err
    db.x("UPDATE users SET enabled = 0")
    audit("owner", "revoke_all", "", "", _ip(request))
    return {"ok": True}


@router.get("/access/admin/usage")
async def admin_usage(request: Request, user_id: Optional[int] = None, limit: int = 200):
    err = _owner_or_error(request)
    if err:
        return err
    limit = max(1, min(int(limit), 2000))
    if user_id is None:
        rows = db.q("SELECT * FROM usage ORDER BY id DESC LIMIT ?", (limit,))
    else:
        rows = db.q("SELECT * FROM usage WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit))
    return {"rows": rows}


@router.get("/access/admin/audit")
async def admin_audit(request: Request, limit: int = 200):
    err = _owner_or_error(request)
    if err:
        return err
    return {"rows": db.q("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (max(1, min(int(limit), 2000)),))}


# ══════════════════════════ установка ══════════════════════════

def install_access_gate(app, db_path: Optional[str] = None):
    """Вызывать ПОСЛЕДНЕЙ строкой main.py: ворота встают снаружи всех middleware и закрывают все уже зарегистрированные маршруты."""
    db.open(db_path)
    app.include_router(router)
    app.add_middleware(AccessGateMiddleware)
    state = "ВКЛЮЧЕНЫ" if gate_on() else "ВЫКЛЮЧЕНЫ (нет ACCESS_OWNER_KEY) — сервер открыт, как раньше"
    print(f"[access_gate {BUILD}] ворота {state}; база {db.path}; рубильник {'ВКЛ' if lockdown() else 'выкл'}")
    if gate_on() and len(owner_key()) < 32:
        print(f"[access_gate {BUILD}] ВНИМАНИЕ: ACCESS_OWNER_KEY короче 32 символов")
