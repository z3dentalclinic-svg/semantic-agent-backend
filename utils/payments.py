"""
payments.py — оплата (пополнение баланса картой через Paddle) и настоящее удаление аккаунта.

Регистрация в main.py (две строки, ПОСЛЕ install_cabinet(app)):
    from utils.payments import install_payments
    install_payments(app)

pay_0.1 (Andrew, 2026-11-24) — решения Andrew:
  • платёжный сервис — Paddle (продавец для покупателя — Paddle, он же считает и платит налог с продаж по странам);
  • пополнение фиксированными суммами (по умолчанию $5 / $10 / $20 / $50): на каждую сумму в Paddle заведена своя
    цена (price), соответствие «сумма → id цены» лежит в переменной окружения;
  • внесённые деньги — возвратные (Terms: неиспользованный остаток возвращается, срок — до 14 дней);
  • кнопка «Удалить аккаунт» удаляет по-настоящему: запись пользователя, сессии, журнал операций, сохранённые
    прогоны (файлы на диске), проекты, отзывы, настройки, реферальный код. Корзины нет, восстановить нельзя;
  • перед удалением неиспользованный остаток внесённых денег возвращается на карту (заявка на возврат в Paddle);
  • остаются только записи о платежах БЕЗ привязки к человеку (номер операции Paddle, сумма, дата) — для возвратов,
    споров по карте и учёта;
  • чтобы стартовые $2 нельзя было получать заново удалением и повторной регистрацией, после удаления Google-аккаунта
    хранится необратимый отпечаток его идентификатора (sha256, без почты и имени) — таблица deleted_accounts,
    проверяет её access_gate ag_0.8 при регистрации.

Переменные окружения (Render → Environment). ПОКА НЕ ЗАДАНЫ ПЕРВЫЕ ТРИ — ОПЛАТА ВЫКЛЮЧЕНА, кнопка «Пополнить»
работает по-старому (заявка владельцу); удаление аккаунта работает и без них. Откат = убрать переменные.
    PADDLE_CLIENT_TOKEN    клиентский токен Paddle (test_… — песочница, live_… — боевой). Не секрет, уходит в браузер.
    PADDLE_WEBHOOK_SECRET  секрет адреса уведомлений (pdl_ntfset_…) — им Paddle подписывает уведомления об оплате.
    PADDLE_PRICES          «сумма:id цены» через запятую, например: 5:pri_aaa,10:pri_bbb,20:pri_ccc,50:pri_ddd
    PADDLE_API_KEY         серверный ключ Paddle с правом adjustment.write — нужен только для возвратов. Без него
                           возврат не отправляется автоматически, а помечается «к возврату» — владелец видит список
                           в /cabinet/admin/pay и возвращает вручную в кабинете Paddle.
Песочница или бой определяется по токену: test_… → sandbox-api.paddle.com, иначе api.paddle.com.

Как идёт оплата:
  1. приложение спрашивает /cabinet/pay/config (включено ли, суммы, клиентский токен);
  2. человек выбирает сумму → POST /cabinet/pay/start → в базе появляется строка платежа со случайной меткой ref,
     приложение открывает окно оплаты Paddle с этой ценой и меткой (customData.ref);
  3. Paddle после успешной оплаты шлёт уведомление transaction.completed на POST /pay/webhook. Подпись проверяется
     секретом; по метке ref находится человек; сумма зачисляется на баланс (users.credit) ровно один раз;
  4. приложение опрашивает /cabinet/pay/status?ref=… и показывает новый баланс.
  Деньги зачисляет ТОЛЬКО подписанное уведомление Paddle — из браузера зачислить баланс нельзя.

Зависит от utils.access_gate (база, identify, баланс, журнал) и utils.cabinet (база кабинета, папка прогонов).
Модули конвейера не трогаются.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import shutil
import time
import urllib.error
import urllib.request
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from utils import access_gate as gate
from utils import cabinet as cab

BUILD = "pay_0.1"

# ─── настройки ───
WEBHOOK_PATH = "/pay/webhook"        # открытый путь (без токена): Paddle приходит сюда сам, защита — подпись
TS_TOLERANCE = 300                   # с; уведомление старше — отказ (защита от повторной отправки перехваченного)
STARTED_TTL = 7 * 86400              # начатые и не оплаченные платежи старше — удаляются
REFUND_DAYS = 14                     # срок возврата по Terms (показывается человеку)
PADDLE_TIMEOUT = 20                  # с, запрос к Paddle
MAX_QTY = 100                        # количество в одной покупке (Paddle по умолчанию допускает 1–100)
DELETE_WORD = "DELETE"               # слово подтверждения удаления (приложение шлёт его после ввода человеком)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ref TEXT NOT NULL UNIQUE, user_id INTEGER, amount REAL NOT NULL,
  price_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'started', txn_id TEXT UNIQUE, item_id TEXT NOT NULL DEFAULT '',
  currency TEXT NOT NULL DEFAULT '', line_total INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, paid_at INTEGER,
  refunded REAL NOT NULL DEFAULT 0, refund_due REAL NOT NULL DEFAULT 0, refund_note TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS payments_user ON payments(user_id);
CREATE TABLE IF NOT EXISTS pay_events (event_id TEXT PRIMARY KEY, ts INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS deleted_accounts (sub_hash TEXT PRIMARY KEY, deleted_at INTEGER NOT NULL);
"""
# payments.status: started — окно оплаты открыто; paid — оплачено и зачислено; orphan — оплачено, но человека уже нет
#                  (аккаунт удалён до прихода уведомления) — деньги к возврату; mismatch — цена в уведомлении не та.
# payments.amount — в долларах баланса (сумма пакета × количество). refunded — сколько из неё отправлено на возврат,
# refund_due — сколько нужно вернуть вручную (автоматический возврат не прошёл или нет PADDLE_API_KEY).


def _now() -> int:
    return int(time.time())


def client_token() -> str:
    return (os.environ.get("PADDLE_CLIENT_TOKEN") or "").strip()


def webhook_secret() -> str:
    return (os.environ.get("PADDLE_WEBHOOK_SECRET") or "").strip()


def api_key() -> str:
    return (os.environ.get("PADDLE_API_KEY") or "").strip()


def sandbox() -> bool:
    return client_token().startswith("test_")


def api_base() -> str:
    return "https://sandbox-api.paddle.com" if sandbox() else "https://api.paddle.com"


def packs() -> dict:
    """PADDLE_PRICES → {сумма: id цены}. Строки с ошибками пропускаются."""
    out = {}
    for part in (os.environ.get("PADDLE_PRICES") or "").split(","):
        if ":" not in part:
            continue
        a, p = part.split(":", 1)
        try:
            amount = float(a.strip())
        except ValueError:
            continue
        p = p.strip()
        if amount > 0 and p.startswith("pri_"):
            out[amount] = p
    return out


def enabled() -> bool:
    return bool(gate.gate_on() and client_token() and webhook_secret() and packs())


def sub_hash(google_sub: str) -> str:
    """Отпечаток Google-идентификатора удалённого аккаунта. Формула продублирована в access_gate ag_0.8."""
    return hashlib.sha256(("sa-deleted:" + google_sub).encode("utf-8")).hexdigest()


def _err(text: str, code: str, status: int) -> JSONResponse:
    return JSONResponse({"error": text, "code": code}, status_code=status)


def _tester(request: Request):
    """→ (полная строка пользователя, None) или (None, отказ). Оплата и удаление — только у роли tester."""
    u, err = cab._me(request)
    if err:
        return None, err
    if u["role"] != "tester":
        return None, _err("Доступно только пользователям с балансом", "not_tester", 400)
    full = gate.db.one("SELECT * FROM users WHERE id = ?", (u["id"],))
    if not full:
        return None, _err("Нужен токен доступа", "no_token", 401)
    return full, None


router = APIRouter()


# ══════════════════════════ пополнение ══════════════════════════

@router.get("/cabinet/pay/config")
async def pay_config(request: Request):
    u, err = cab._me(request)
    if err:
        return err
    on = enabled() and u["role"] == "tester"
    return {"build": BUILD, "enabled": on, "client_token": client_token() if on else "", "sandbox": sandbox(),
            "packs": sorted(packs().keys()) if on else [], "refund_days": REFUND_DAYS,
            "can_delete": u["role"] == "tester"}


class StartReq(BaseModel):
    amount: float


@router.post("/cabinet/pay/start")
async def pay_start(request: Request, req: StartReq):
    """Начать пополнение: строка платежа с меткой ref; приложение открывает окно Paddle с этой ценой и меткой."""
    u, err = _tester(request)
    if err:
        return err
    if not enabled():
        return _err("Оплата ещё не подключена", "pay_off", 503)
    price = packs().get(float(req.amount))
    if not price:
        return _err("Такой суммы пополнения нет", "bad_amount", 400)
    ref = secrets.token_urlsafe(18)
    now = _now()
    gate.db.x("DELETE FROM payments WHERE status = 'started' AND created_at < ?", (now - STARTED_TTL,))
    gate.db.x("INSERT INTO payments(ref, user_id, amount, price_id, created_at) VALUES (?,?,?,?,?)",
              (ref, u["id"], float(req.amount), price, now))
    return {"ref": ref, "price_id": price, "amount": float(req.amount), "email": u.get("email") or ""}


@router.get("/cabinet/pay/status")
async def pay_status(request: Request, ref: str = ""):
    u, err = _tester(request)
    if err:
        return err
    p = gate.db.one("SELECT status, amount FROM payments WHERE ref = ? AND user_id = ?", (ref, u["id"]))
    if not p:
        return _err("Платёж не найден", "not_found", 404)
    return {"status": p["status"], "amount": p["amount"], "balance": gate.balance_of(u)}


def verify_signature(header: str, raw: bytes, secret: str, now: Optional[int] = None) -> str:
    """Paddle-Signature: «ts=1671552777;h1=<hex>». Подписано: «<ts>:<тело как пришло>», HMAC-SHA256 секретом адреса.
    → "" (верно) или код отказа."""
    ts, sigs = "", []
    for part in (header or "").split(";"):
        k, _, v = part.strip().partition("=")
        if k == "ts":
            ts = v
        elif k == "h1":
            sigs.append(v)            # при смене секрета Paddle шлёт несколько h1
    if not ts or not sigs or not secret:
        return "no_signature"
    try:
        t = int(ts)
    except ValueError:
        return "no_signature"
    if abs((_now() if now is None else now) - t) > TS_TOLERANCE:
        return "stale"
    want = hmac.new(secret.encode("utf-8"), ts.encode("utf-8") + b":" + raw, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(want, s) for s in sigs):
        return "bad_signature"
    return ""


def _int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _apply_completed(ev: dict) -> dict:
    """transaction.completed → зачисление. Вызывается под блокировкой базы: проверка «уже было» и зачисление — одним куском."""
    event_id = str(ev.get("event_id") or "")
    data = ev.get("data") if isinstance(ev.get("data"), dict) else {}
    txn = str(data.get("id") or "")
    cd = data.get("custom_data") if isinstance(data.get("custom_data"), dict) else {}
    ref = str(cd.get("ref") or "")
    if not txn or not ref:
        gate.audit("paddle", "pay_unknown", "", f"txn={txn} нет метки ref")
        return {"ok": True, "result": "no_ref"}
    if event_id and gate.db.one("SELECT event_id FROM pay_events WHERE event_id = ?", (event_id,)):
        return {"ok": True, "result": "duplicate"}
    p = gate.db.one("SELECT * FROM payments WHERE ref = ?", (ref,))
    if not p:
        gate.audit("paddle", "pay_unknown", "", f"txn={txn} метка не найдена")
        return {"ok": True, "result": "unknown_ref"}
    if p["status"] != "started":
        return {"ok": True, "result": "duplicate"}
    if gate.db.one("SELECT id FROM payments WHERE txn_id = ?", (txn,)):
        return {"ok": True, "result": "duplicate"}
    qty = 0
    for it in data.get("items") or []:
        price = it.get("price") if isinstance(it, dict) and isinstance(it.get("price"), dict) else {}
        if price.get("id") == p["price_id"]:
            qty = _int(it.get("quantity"))
    details = data.get("details") if isinstance(data.get("details"), dict) else {}
    line = None
    for li in details.get("line_items") or []:
        if isinstance(li, dict) and (li.get("price_id") == p["price_id"] or line is None):
            line = li
    item_id = str((line or {}).get("id") or "")
    totals = (line or {}).get("totals") if isinstance((line or {}).get("totals"), dict) else {}
    line_total = _int(totals.get("total")) or _int((details.get("totals") or {}).get("grand_total"))
    now = _now()
    if event_id:
        gate.db.x("INSERT OR IGNORE INTO pay_events(event_id, ts) VALUES (?,?)", (event_id, now))
    if qty < 1 or qty > MAX_QTY:
        gate.db.x("UPDATE payments SET status = 'mismatch', txn_id = ?, item_id = ?, currency = ?, line_total = ?, paid_at = ?, "
                  "refund_note = 'цена в уведомлении не совпала с начатым платежом — проверить вручную в Paddle' WHERE id = ?",
                  (txn, item_id, str(data.get("currency_code") or ""), line_total, now, p["id"]))
        gate.audit("paddle", "pay_mismatch", "", f"txn={txn}")
        return {"ok": True, "result": "mismatch"}
    amount = round(p["amount"] * qty, 2)
    u = gate.db.one("SELECT id, role FROM users WHERE id = ?", (p["user_id"],)) if p["user_id"] is not None else None
    if not u or u["role"] != "tester":
        gate.db.x("UPDATE payments SET status = 'orphan', user_id = NULL, amount = ?, txn_id = ?, item_id = ?, currency = ?, "
                  "line_total = ?, paid_at = ?, refund_due = ?, refund_note = 'аккаунт удалён до прихода оплаты' WHERE id = ?",
                  (amount, txn, item_id, str(data.get("currency_code") or ""), line_total, now, amount, p["id"]))
        gate.audit("paddle", "pay_orphan", "", f"txn={txn} amount={amount}")
        return {"ok": True, "result": "orphan"}
    gate.db.x("UPDATE payments SET status = 'paid', amount = ?, txn_id = ?, item_id = ?, currency = ?, line_total = ?, "
              "paid_at = ? WHERE id = ?",
              (amount, txn, item_id, str(data.get("currency_code") or ""), line_total, now, p["id"]))
    gate.db.x("UPDATE users SET credit = credit + ? WHERE id = ?", (amount, u["id"]))
    gate.audit("paddle", "pay_topup", str(u["id"]), str(amount))
    return {"ok": True, "result": "credited", "amount": amount}


@router.post(WEBHOOK_PATH)
async def pay_webhook(request: Request):
    """Уведомление Paddle. Ответ 200 = «принято, не повторять»; 4xx/5xx — Paddle повторит позже."""
    secret = webhook_secret()
    if not secret:
        return _err("Оплата не подключена", "pay_off", 503)
    raw = await request.body()
    bad = verify_signature(request.headers.get("paddle-signature", ""), raw, secret)
    if bad:
        return _err("Подпись не подтверждена", bad, 401)
    try:
        ev = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return _err("Не JSON", "bad_json", 400)
    if not isinstance(ev, dict) or ev.get("event_type") != "transaction.completed":
        return {"ok": True, "result": "ignored"}
    with gate.db.lock:
        return _apply_completed(ev)


# ══════════════════════════ возврат ══════════════════════════

def _paddle_refund(txn_id: str, item_id: str, minor: Optional[int]) -> tuple:
    """Заявка на возврат в Paddle (adjustment). minor=None — полный возврат операции, иначе частичный: сумма в
    минимальных единицах валюты операции, с налогом. → (True, id заявки) или (False, причина)."""
    key = api_key()
    if not key:
        return False, "нет PADDLE_API_KEY"
    body = {"action": "refund", "transaction_id": txn_id, "reason": "Unused balance refund"}
    if minor is None:
        body["type"] = "full"
    else:
        if not item_id:
            return False, "нет id позиции операции"
        body["type"] = "partial"
        body["items"] = [{"item_id": item_id, "type": "partial", "amount": str(int(minor))}]
    req = urllib.request.Request(api_base() + "/adjustments", data=json.dumps(body).encode("utf-8"), method="POST",
                                 headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=PADDLE_TIMEOUT) as r:
            d = json.loads(r.read().decode("utf-8"))
        return True, str(((d or {}).get("data") or {}).get("id") or "ok")
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:300]
        except Exception:  # noqa: BLE001
            detail = ""
        return False, f"Paddle {e.code} {detail}"
    except Exception as e:  # noqa: BLE001 — сеть, таймаут, не JSON
        return False, f"нет связи с Paddle: {type(e).__name__}"


def refund_plan(uid: int) -> list:
    """Какую часть каких оплат вернуть, чтобы отдать неиспользованный остаток внесённых денег. Свежие оплаты — первыми.
    Возвращается не больше, чем человек внёс картой: ручные пополнения владельцем картой не возвращаются."""
    u = gate.db.one("SELECT * FROM users WHERE id = ?", (uid,))
    if not u:
        return []
    left = gate.balance_of(u)["paid_left"]
    plan = []
    for p in gate.db.q("SELECT * FROM payments WHERE user_id = ? AND status = 'paid' ORDER BY paid_at DESC, id DESC", (uid,)):
        if left < 0.005:
            break
        room = round(p["amount"] - p["refunded"] - p["refund_due"], 4)
        part = round(min(left, room), 2)
        if part < 0.01:
            continue
        full = p["refunded"] == 0 and p["refund_due"] == 0 and abs(part - p["amount"]) < 0.005
        minor = None if full else int(p["line_total"] * part / p["amount"]) if p["amount"] else 0
        plan.append({"id": p["id"], "txn_id": p["txn_id"], "item_id": p["item_id"], "part": part, "minor": minor})
        left = round(left - part, 4)
    return plan


def refund_unused(uid: int) -> dict:
    """Вернуть неиспользованный остаток внесённых денег. Сетевые вызовы — вне блокировки базы.
    → {"requested": $ отправлено в Paddle, "due": $ к ручному возврату}. Баланс уменьшается на обе суммы."""
    with gate.db.lock:
        plan = refund_plan(uid)
    requested = due = 0.0
    for step in plan:
        if step["minor"] is not None and step["minor"] < 1:
            ok, note = False, "сумма меньше минимальной единицы валюты"
        else:
            ok, note = _paddle_refund(step["txn_id"], step["item_id"], step["minor"])
        with gate.db.lock:
            if ok:
                gate.db.x("UPDATE payments SET refunded = refunded + ?, refund_note = ? WHERE id = ?",
                          (step["part"], "заявка " + note, step["id"]))
                requested += step["part"]
            else:
                gate.db.x("UPDATE payments SET refund_due = refund_due + ?, refund_note = ? WHERE id = ?",
                          (step["part"], note[:300], step["id"]))
                due += step["part"]
            gate.db.x("UPDATE users SET credit = credit - ? WHERE id = ?", (step["part"], uid))
            gate.audit("paddle", "pay_refund", str(uid), str(-step["part"]))
    return {"requested": round(requested, 2), "due": round(due, 2)}


# ══════════════════════════ удаление аккаунта ══════════════════════════

def purge_user(uid: int) -> dict:
    """Стереть всё о человеке из обеих баз и с диска. Платежи остаются без привязки к человеку."""
    with gate.db.lock:
        u = gate.db.one("SELECT * FROM users WHERE id = ?", (uid,))
        if not u:
            return {"ok": False}
        c = gate.db.conn
        c.execute("PRAGMA secure_delete = ON")                    # удалённые строки затираются нулями в файле базы
        if u.get("google_sub"):
            c.execute("INSERT OR IGNORE INTO deleted_accounts(sub_hash, deleted_at) VALUES (?,?)",
                      (sub_hash(u["google_sub"]), _now()))
        c.execute("DELETE FROM sessions WHERE user_id = ?", (uid,))
        c.execute("DELETE FROM usage WHERE user_id = ?", (uid,))
        c.execute("DELETE FROM audit WHERE actor IN (?, ?) OR (target = ? AND action IN ('super_create', 'tester_create', "
                  "'user_enable', 'user_disable', 'user_reissue', 'user_update', 'user_topup', 'pay_topup', 'pay_refund'))",
                  ("tester:" + str(uid), "user:" + str(uid), str(uid)))   # только записи о человеке: у invites_create target — число кодов
        c.execute("UPDATE invites SET used_by = NULL WHERE used_by = ?", (uid,))
        # начатые платежи не стираем: если оплата придёт уже после удаления, уведомление найдёт строку без человека
        # и пометит её «к возврату» (orphan); неоплаченные уйдут сами через STARTED_TTL
        c.execute("UPDATE payments SET user_id = NULL WHERE user_id = ?", (uid,))
        c.execute("DELETE FROM users WHERE id = ?", (uid,))
    with cab.db.lock:
        c = cab.db.conn
        c.execute("PRAGMA secure_delete = ON")
        c.execute("DELETE FROM run_ops WHERE run_id IN (SELECT id FROM runs WHERE user_id = ?)", (uid,))
        for table in ("runs", "projects", "feedback", "topup_requests", "settings", "referrals"):
            c.execute(f"DELETE FROM {table} WHERE user_id = ?", (uid,))
        c.execute("DELETE FROM referral_uses WHERE referrer_id = ? OR invited_id = ?", (uid, uid))
    files = False
    if cab.RUNS_DIR:
        d = os.path.join(cab.RUNS_DIR, str(int(uid)))
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
            files = True
    with gate._qlock:
        gate._last_wait.pop(uid, None)
    return {"ok": True, "origin": u.get("origin") or "", "files": files}


class DeleteReq(BaseModel):
    confirm: str = ""


@router.post("/cabinet/account/delete")
async def account_delete(request: Request, req: DeleteReq):
    """Удалить аккаунт навсегда: закрыть вход → вернуть неиспользованные деньги → стереть данные."""
    u, err = _tester(request)
    if err:
        return err
    if (req.confirm or "").strip() != DELETE_WORD:
        return _err("Удаление не подтверждено", "not_confirmed", 400)
    uid = u["id"]
    with gate._active_lock:
        busy = uid in gate._active
    with gate._qlock:
        busy = busy or uid in gate._tickets
    if busy:
        return _err("Идёт операция — дождитесь её окончания и повторите", "busy", 409)
    with gate.db.lock:                                            # шаг 1: вход закрыт сразу, на всех устройствах
        gate.db.x("DELETE FROM sessions WHERE user_id = ?", (uid,))
        gate.db.x("UPDATE users SET enabled = 0, token_hash = ? WHERE id = ?", (gate._sha(gate._new_token()), uid))
    refund = await asyncio.to_thread(refund_unused, uid)          # шаг 2: возврат (сеть — не в основном потоке)
    res = await asyncio.to_thread(purge_user, uid)                # шаг 3: стереть
    gate.audit("system", "account_deleted", "", f"origin={res.get('origin', '')} refund_requested={refund['requested']} "
                                                  f"refund_due={refund['due']}")
    return {"ok": True, "refund": round(refund["requested"] + refund["due"], 2), "refund_days": REFUND_DAYS}


# ══════════════════════════ владельцу ══════════════════════════

@router.get("/cabinet/admin/pay")
async def adm_pay(request: Request, limit: int = 200):
    """Платежи и возвраты. to_refund — что разобрать вручную в кабинете Paddle: автоматический возврат не прошёл
    (refund_due — сколько вернуть) или оплачена не та цена (status mismatch — деньги не зачислены)."""
    _, err = cab._owner(request)
    if err:
        return err
    lim = max(1, min(int(limit), 2000))
    rows = gate.db.q("SELECT id, user_id, amount, status, txn_id, currency, created_at, paid_at, refunded, refund_due, "
                     "refund_note FROM payments WHERE status != 'started' ORDER BY id DESC LIMIT ?", (lim,))
    tot = gate.db.one("SELECT COALESCE(SUM(amount),0) AS paid, COALESCE(SUM(refunded),0) AS refunded, "
                      "COALESCE(SUM(refund_due),0) AS due FROM payments WHERE status != 'started'")
    return {"build": BUILD, "enabled": enabled(), "sandbox": sandbox(), "refunds_auto": bool(api_key()),
            "packs": sorted(packs().keys()), "totals": {k: round(v, 2) for k, v in tot.items()},
            "to_refund": [r for r in rows if r["refund_due"] > 0 or r["status"] == "mismatch"], "rows": rows}


# ══════════════════════════ установка ══════════════════════════

def install_payments(app):
    """После install_cabinet(app): таблицы в базе ворот, открытый путь для уведомлений Paddle."""
    with gate.db.lock:
        gate.db.conn.executescript(_SCHEMA)
    gate.OPEN_PATHS.add(WEBHOOK_PATH)
    app.include_router(router)
    state = (("ВКЛЮЧЕНА, " + ("песочница" if sandbox() else "БОЕВОЙ режим") + f", суммы {sorted(packs().keys())}")
             if enabled() else "ВЫКЛЮЧЕНА (нет PADDLE_CLIENT_TOKEN / PADDLE_WEBHOOK_SECRET / PADDLE_PRICES)")
    print(f"[payments {BUILD}] оплата {state}; автовозврат {'есть' if api_key() else 'нет (нет PADDLE_API_KEY)'}")
