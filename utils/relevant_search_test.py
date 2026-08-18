# relevant_search_test.py
# Калибровочный стенд "релевантного поиска" (генерация вариантов сида).
# Отдельный роутер, к пайплайну НЕ подключён.
# Подключение в main: from relevant_search_test import router as relevant_router
#                     app.include_router(relevant_router)

import os
import re
import json
import time
import asyncio
import httpx
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

GEN_RUNS = 3          # прогонов генерации
TOP_N = 3             # потолок вариантов в работу
VER_BATCH = 20        # параллельность верификатора

# ---------------- модели ----------------
# provider: gemini | openai | anthropic
# price: [input $/1M, output $/1M] — правь под актуальный прайс
MODELS = {
    "gemini-flash-lite": {"provider": "gemini", "model": "gemini-3.1-flash-lite", "price": [0.10, 0.40]},
    "gemini-flash":      {"provider": "gemini", "model": "gemini-3.6-flash",      "price": [0.30, 2.50]},
    "gpt":               {"provider": "openai", "model": "gpt-5.5",               "price": [1.25, 10.00]},
    "claude-sonnet":     {"provider": "anthropic", "model": "claude-sonnet-4-6", "price": [3.00, 15.00]},
}
THINKING = ["off", "low", "medium"]

GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ---------------- промпты (утверждены) ----------------
GEN_PROMPT = """Запрос пользователя: "{seed}"

Разбери запрос на слова. Для каждого слова укажи роль:
- obj (предмет), act (действие), app (к чему относится),
  comm (цена/купить/отзывы и т.п.), geo (город/страна),
  brand (бренд/модель), num (число/характеристика), func (предлог/союз)

Для ролей geo, brand, num, func замен НЕ давать — пустой список.

Для остальных слов дай 0-3 замены. Замена — это слово, которым
другой реальный человек заменил бы это слово, ища ТО ЖЕ САМОЕ
в Google. Проверяй подстановкой: замена должна встать на место
исходного слова в "{seed}" так, чтобы запрос искали те же люди
с той же целью.

НЕ давать:
- оценки и модификаторы (дешево, лучший, срочно, рядом)
- обобщения (устройство вместо конкретного предмета)
- сужения (конкретная модель вместо общего слова)
- слова, меняющие цель поиска

Давай только замены, которые сам видел в реальных
поисковых запросах или названиях товаров.

Ответ строго JSON без пояснений:
{{"tokens":[{{"word":"...","role":"...","subs":["..."]}}]}}"""

VER_PROMPT = """Два запроса в Google:
A: "{seed}"
B: "{variant}"

Ищут ли их одни и те же люди с одной и той же целью?
Покажет ли Google по ним практически одинаковую выдачу?

1 — если B это тот же запрос другими словами.
0 — если B ищут другие люди или с другой целью.

Ответ строго: 1 или 0"""

FROZEN_ROLES = {"geo", "brand", "num", "func"}


# ---------------- вызовы моделей ----------------
async def call_llm(client, model_key, thinking, prompt):
    cfg = MODELS[model_key]
    t0 = time.time()
    if cfg["provider"] == "gemini":
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{cfg['model']}:generateContent?key={GEMINI_KEY}"
        body = {"contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 1.0}}
        if thinking == "off":
            body["generationConfig"]["thinkingConfig"] = {"thinkingBudget": 0}
        elif thinking == "low":
            body["generationConfig"]["thinkingConfig"] = {"thinkingLevel": "low"}
        elif thinking == "medium":
            body["generationConfig"]["thinkingConfig"] = {"thinkingLevel": "medium"}
        r = await client.post(url, json=body, timeout=90)
        r.raise_for_status()
        d = r.json()
        text = "".join(p.get("text", "") for p in d["candidates"][0]["content"]["parts"])
        um = d.get("usageMetadata", {})
        tin = um.get("promptTokenCount", 0)
        tout = um.get("candidatesTokenCount", 0) + um.get("thoughtsTokenCount", 0)
    elif cfg["provider"] == "openai":
        url = "https://api.openai.com/v1/chat/completions"
        body = {"model": cfg["model"],
                "messages": [{"role": "user", "content": prompt}]}
        body["reasoning_effort"] = "none" if thinking == "off" else thinking
        r = await client.post(url, json=body, timeout=90,
                              headers={"Authorization": f"Bearer {OPENAI_KEY}"})
        r.raise_for_status()
        d = r.json()
        text = d["choices"][0]["message"]["content"]
        tin = d.get("usage", {}).get("prompt_tokens", 0)
        tout = d.get("usage", {}).get("completion_tokens", 0)
    else:  # anthropic
        url = "https://api.anthropic.com/v1/messages"
        body = {"model": cfg["model"], "max_tokens": 4096,
                "messages": [{"role": "user", "content": prompt}]}
        if thinking != "off":
            body["thinking"] = {"type": "enabled",
                                "budget_tokens": 2048 if thinking == "low" else 8192}
            body["max_tokens"] = 16384
        r = await client.post(url, json=body, timeout=90,
                              headers={"x-api-key": ANTHROPIC_KEY,
                                       "anthropic-version": "2023-06-01"})
        r.raise_for_status()
        d = r.json()
        text = "".join(b.get("text", "") for b in d["content"] if b.get("type") == "text")
        tin = d.get("usage", {}).get("input_tokens", 0)
        tout = d.get("usage", {}).get("output_tokens", 0)
    price = cfg["price"]
    cost = tin / 1e6 * price[0] + tout / 1e6 * price[1]
    return {"text": text, "tin": tin, "tout": tout, "cost": cost, "wall": time.time() - t0}


def parse_json_block(text):
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def norm(s):
    return re.sub(r"\s+", " ", s.strip().lower())


# ---------------- логика ----------------
def build_variants(seed, tokens):
    """Одиночная замена токена. Возвращает [(variant, sub_word, orig_word)] в порядке появления."""
    words = seed.split()
    out = []
    for tk in tokens:
        if tk.get("role") in FROZEN_ROLES:
            continue
        w = tk.get("word", "")
        if w not in words:
            continue
        idx = words.index(w)
        for sub in tk.get("subs", [])[:3]:
            sub = str(sub).strip()
            if not sub or norm(sub) == norm(w):
                continue
            v = words.copy()
            v[idx] = sub
            out.append((" ".join(v), sub, w))
    return out


async def process_seed(client, seed, gen_model, gen_thinking, ver_model, ver_thinking):
    seed = norm(seed)
    stats = {"gen": {"tin": 0, "tout": 0, "cost": 0.0, "wall": 0.0},
             "ver": {"tin": 0, "tout": 0, "cost": 0.0, "wall": 0.0}}

    # 3 прогона генерации параллельно
    gen_prompt = GEN_PROMPT.format(seed=seed)
    runs = await asyncio.gather(
        *[call_llm(client, gen_model, gen_thinking, gen_prompt) for _ in range(GEN_RUNS)],
        return_exceptions=True)

    candidates = {}   # norm_variant -> {"variant","votes","positions","runs":[...],"sub","orig"}
    run_tables = []
    for i, res in enumerate(runs):
        if isinstance(res, Exception):
            run_tables.append({"error": str(res)})
            continue
        stats["gen"]["tin"] += res["tin"]; stats["gen"]["tout"] += res["tout"]
        stats["gen"]["cost"] += res["cost"]; stats["gen"]["wall"] = max(stats["gen"]["wall"], res["wall"])
        data = parse_json_block(res["text"])
        if not data or "tokens" not in data:
            run_tables.append({"error": "parse_fail", "raw": res["text"][:500]})
            continue
        run_tables.append({"tokens": data["tokens"]})
        variants = build_variants(seed, data["tokens"])
        for pos, (variant, sub, orig) in enumerate(variants, start=1):
            k = norm(variant)
            if k == seed:
                continue
            c = candidates.setdefault(k, {"variant": variant, "votes": 0, "positions": [],
                                          "sub": sub, "orig": orig})
            c["votes"] += 1
            c["positions"].append(pos)

    # верификатор — параллельно по всем кандидатам
    keys = list(candidates.keys())

    async def verify(k):
        res = await call_llm(client, ver_model, ver_thinking,
                             VER_PROMPT.format(seed=seed, variant=candidates[k]["variant"]))
        stats["ver"]["tin"] += res["tin"]; stats["ver"]["tout"] += res["tout"]
        stats["ver"]["cost"] += res["cost"]
        verdict = 1 if res["text"].strip().startswith("1") else 0
        return k, verdict

    t0 = time.time()
    verdicts = {}
    for i in range(0, len(keys), VER_BATCH):
        chunk = keys[i:i + VER_BATCH]
        results = await asyncio.gather(*[verify(k) for k in chunk], return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                continue
            verdicts[r[0]] = r[1]
    stats["ver"]["wall"] = time.time() - t0

    # ранжирование: голоса desc, средняя позиция asc
    ranked = []
    for k, c in candidates.items():
        avg_pos = sum(c["positions"]) / len(c["positions"])
        ranked.append({"variant": c["variant"], "sub": c["sub"], "orig": c["orig"],
                       "votes": c["votes"], "avg_pos": round(avg_pos, 2),
                       "verdict": verdicts.get(k, -1)})
    ranked.sort(key=lambda x: (-x["votes"], x["avg_pos"]))
    final = [r["variant"] for r in ranked if r["verdict"] == 1][:TOP_N]

    return {"seed": seed, "runs": run_tables, "candidates": ranked,
            "final": final, "stats": stats}


class TestRequest(BaseModel):
    seeds: list
    gen_model: str = "gemini-flash-lite"
    gen_thinking: str = "low"
    ver_model: str = "gemini-flash-lite"
    ver_thinking: str = "low"


@router.get("/api/relevant-test/config")
async def get_config():
    return {"models": {k: v["model"] for k, v in MODELS.items()}, "thinking": THINKING}


@router.post("/api/relevant-test")
async def relevant_test(req: TestRequest):
    if req.gen_model not in MODELS or req.ver_model not in MODELS:
        return {"error": "unknown model"}
    seeds = [s for s in (norm(x) for x in req.seeds) if s][:30]
    t0 = time.time()
    async with httpx.AsyncClient() as client:
        results = []
        for seed in seeds:  # сиды последовательно, внутри сида всё параллельно
            try:
                results.append(await process_seed(client, seed, req.gen_model,
                                                  req.gen_thinking, req.ver_model,
                                                  req.ver_thinking))
            except Exception as e:
                results.append({"seed": seed, "error": str(e)})
    total = {"cost": sum(r["stats"]["gen"]["cost"] + r["stats"]["ver"]["cost"]
                         for r in results if "stats" in r),
             "wall": time.time() - t0}
    return {"results": results, "total": total,
            "config": {"gen": f"{MODELS[req.gen_model]['model']} / {req.gen_thinking}",
                       "ver": f"{MODELS[req.ver_model]['model']} / {req.ver_thinking}"}}
