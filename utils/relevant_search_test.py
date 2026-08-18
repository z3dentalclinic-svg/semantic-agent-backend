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
import pymorphy3
from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

router = APIRouter()

GEN_RUNS = 3          # прогонов генерации
TOP_N = 3             # потолок вариантов в работу
VER_BATCH = 20        # параллельность верификатора
VER_RUNS = 3          # голосов верификатора на пару, вердикт большинством

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

GEN_B_PROMPT = """Запрос пользователя в Google: "{seed}"

Напиши 4-7 запросов, которыми реальный человек ищет
ТО ЖЕ САМОЕ другими словами. Полные фразы целиком,
как их вводят в поиск.

Используй разные формы:
- через глагол (что человек хочет сделать)
- через название самой услуги или предмета
- через того, кто эту услугу оказывает или где
  этот предмет берут

Не менять: города, бренды, модели, числа.
Не добавлять: оценки и модификаторы (дешево, лучший,
срочно, рядом).
Не конкретизируй предмет: если в запросе не сказано,
что именно доставляют/ремонтируют/ищут — в твоих
запросах этого тоже нет.

Давай только запросы, которые сам видел в реальном
поиске или похожие на них по форме.

Ответ строго JSON без пояснений:
{{"queries":["...","..."]}}"""

VER_PROMPT = """Два запроса в Google:
A: "{seed}"
B: "{variant}"

Ищут ли их одни и те же люди с одной и той же целью?
Покажет ли Google по ним практически одинаковую выдачу?

Внимание на предлоги и падежи: если они меняют
место или направление действия — это другая цель.

1 — если B это тот же запрос другими словами.
0 — если B ищут другие люди или с другой целью.

Ответ строго: 1 или 0"""

FROZEN_ROLES = {"geo", "brand", "num", "func"}
MORPH = pymorphy3.MorphAnalyzer()
FUNC_POS = {"PREP", "CONJ", "PRCL"}


def content_tokens(text):
    """Мультимножество значимых токенов: служебные (предлог/союз/частица) отброшены."""
    out = {}
    for w in norm(text).split():
        pos = MORPH.parse(w)[0].tag.POS
        if pos in FUNC_POS:
            continue
        out[w] = out.get(w, 0) + 1
    return out


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

    # 3 прогона класса A (токенные замены) + 3 прогона класса B (свободные фразы), все параллельно
    gen_prompt = GEN_PROMPT.format(seed=seed)
    gen_b_prompt = GEN_B_PROMPT.format(seed=seed)
    all_runs = await asyncio.gather(
        *[call_llm(client, gen_model, gen_thinking, gen_prompt) for _ in range(GEN_RUNS)],
        *[call_llm(client, gen_model, gen_thinking, gen_b_prompt) for _ in range(GEN_RUNS)],
        return_exceptions=True)
    runs, runs_b = all_runs[:GEN_RUNS], all_runs[GEN_RUNS:]

    candidates = {}   # norm_variant -> {"variant","votes","positions","sub","orig","cls"}
    run_tables = []

    seed_content = content_tokens(seed)

    def add_candidate(variant, pos, cls, sub="", orig=""):
        k = norm(variant)
        if k == seed:
            return
        if content_tokens(variant) == seed_content:
            return  # сид с предлогом/перестановкой — не вариант
        c = candidates.setdefault(k, {"variant": variant, "votes": 0, "positions": [],
                                      "sub": sub, "orig": orig, "cls": set()})
        c["votes"] += 1
        c["positions"].append(pos)
        c["cls"].add(cls)
        if cls == "A" and sub:      # токенная замена информативнее для колонки
            c["sub"], c["orig"] = sub, orig

    for res in runs:
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
        for pos, (variant, sub, orig) in enumerate(build_variants(seed, data["tokens"]), start=1):
            add_candidate(variant, pos, "A", sub, orig)

    for res in runs_b:
        if isinstance(res, Exception):
            run_tables.append({"error": str(res)})
            continue
        stats["gen"]["tin"] += res["tin"]; stats["gen"]["tout"] += res["tout"]
        stats["gen"]["cost"] += res["cost"]; stats["gen"]["wall"] = max(stats["gen"]["wall"], res["wall"])
        data = parse_json_block(res["text"])
        if not data or "queries" not in data:
            run_tables.append({"error": "parse_fail_b", "raw": res["text"][:500]})
            continue
        qs = [str(q) for q in data["queries"]][:7]
        run_tables.append({"queries": qs})
        for pos, q in enumerate(qs, start=1):
            add_candidate(q, pos, "B")

    # верификатор — параллельно по всем кандидатам
    keys = list(candidates.keys())

    async def verify(k):
        prompt = VER_PROMPT.format(seed=seed, variant=candidates[k]["variant"])
        results = await asyncio.gather(
            *[call_llm(client, ver_model, ver_thinking, prompt) for _ in range(VER_RUNS)],
            return_exceptions=True)
        ones = zeros = 0
        for res in results:
            if isinstance(res, Exception):
                continue
            stats["ver"]["tin"] += res["tin"]; stats["ver"]["tout"] += res["tout"]
            stats["ver"]["cost"] += res["cost"]
            if res["text"].strip().startswith("1"):
                ones += 1
            else:
                zeros += 1
        verdict = 1 if (ones == VER_RUNS and zeros == 0) else 0  # в работу только единогласный 3-0
        return k, verdict, f"{ones}-{zeros}"

    t0 = time.time()
    verdicts = {}
    per_batch = max(1, VER_BATCH // VER_RUNS)
    for i in range(0, len(keys), per_batch):
        chunk = keys[i:i + per_batch]
        results = await asyncio.gather(*[verify(k) for k in chunk], return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                continue
            verdicts[r[0]] = (r[1], r[2])
    stats["ver"]["wall"] = time.time() - t0

    # ранжирование: голоса desc, средняя позиция asc
    ranked = []
    for k, c in candidates.items():
        avg_pos = sum(c["positions"]) / len(c["positions"])
        ranked.append({"variant": c["variant"], "sub": c["sub"], "orig": c["orig"],
                       "cls": "".join(sorted(c["cls"])),
                       "votes": c["votes"], "avg_pos": round(avg_pos, 2),
                       "verdict": verdicts.get(k, (-1, ""))[0],
                       "score": verdicts.get(k, (-1, ""))[1]})
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

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>Релевантный поиск — калибровка</title>
<style>
  body{font-family:-apple-system,Segoe UI,sans-serif;background:#FAF9F6;color:#1a1a1a;margin:0;padding:24px;max-width:1100px;margin:auto}
  h1{font-size:20px}
  textarea{width:100%;height:140px;font-size:14px;padding:10px;border:1px solid #ccc;border-radius:8px;box-sizing:border-box}
  select,button{font-size:14px;padding:8px 12px;border-radius:8px;border:1px solid #ccc;margin-right:8px}
  button{background:#059669;color:#fff;border:none;cursor:pointer;padding:10px 22px}
  button:disabled{background:#9ca3af}
  .row{margin:12px 0;display:flex;flex-wrap:wrap;gap:10px;align-items:center}
  .lbl{font-size:12px;color:#666;margin-right:4px}
  .seed-block{background:#fff;border:1px solid #e5e5e5;border-radius:10px;padding:16px;margin:16px 0}
  .seed-title{font-weight:600;font-size:16px;margin-bottom:8px}
  table{border-collapse:collapse;width:100%;font-size:13px;margin:8px 0}
  th,td{border:1px solid #e5e5e5;padding:5px 8px;text-align:left}
  th{background:#f3f4f6}
  .ok{color:#059669;font-weight:600}
  .cut{color:#dc2626;font-weight:600}
  .final{background:#ecfdf5;border-radius:8px;padding:10px;margin-top:8px;font-size:14px}
  .stats{font-size:12px;color:#666;margin-top:8px}
  .total{background:#111;color:#fff;border-radius:10px;padding:14px;margin:20px 0;font-size:14px}
  .err{color:#dc2626;font-size:13px}
  details{margin:6px 0}
  summary{cursor:pointer;font-size:13px;color:#555}
</style>
</head>
<body>
<h1>Релевантный поиск — калибровочный стенд</h1>

<textarea id="seeds" placeholder="Сиды, по одному в строке (до 30)&#10;аккумулятор на скутер&#10;установка кондиционеров в киеве цена"></textarea>

<div class="row">
  <span class="lbl">Генератор:</span>
  <select id="gen_model">
    <option value="gemini-flash-lite">gemini-3.1-flash-lite</option>
    <option value="gemini-flash">gemini-3.6-flash</option>
    <option value="gpt">gpt-5.5</option>
    <option value="claude-sonnet">claude-sonnet-4-6</option>
  </select>
  <select id="gen_thinking">
    <option value="off">thinking: off</option>
    <option value="low" selected>thinking: low</option>
    <option value="medium">thinking: medium</option>
  </select>
  <span class="lbl">Верификатор:</span>
  <select id="ver_model">
    <option value="gemini-flash-lite">gemini-3.1-flash-lite</option>
    <option value="gemini-flash">gemini-3.6-flash</option>
    <option value="gpt">gpt-5.5</option>
    <option value="claude-sonnet">claude-sonnet-4-6</option>
  </select>
  <select id="ver_thinking">
    <option value="off">thinking: off</option>
    <option value="low" selected>thinking: low</option>
    <option value="medium">thinking: medium</option>
  </select>
  <button id="run" onclick="run()">Запустить</button>
</div>

<div id="warn" style="display:none;background:#fef3c7;border:1px solid #f59e0b;border-radius:8px;padding:10px;font-size:13px;margin:10px 0"></div>
<div id="out"></div>

<script>
// переключатели статичны в разметке, JS для них не нужен

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

async function run(){
  const seeds = document.getElementById('seeds').value.split('\n').map(s=>s.trim()).filter(Boolean);
  if(!seeds.length){alert('Введи сиды');return}
  const base = 'https://semantic-agent-backend.onrender.com';
  const btn = document.getElementById('run');
  btn.disabled = true; btn.textContent = 'Работаю...';
  document.getElementById('out').innerHTML = '';
  try{
    const r = await fetch(base + '/api/relevant-test',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        seeds,
        gen_model:document.getElementById('gen_model').value,
        gen_thinking:document.getElementById('gen_thinking').value,
        ver_model:document.getElementById('ver_model').value,
        ver_thinking:document.getElementById('ver_thinking').value
      })});
    const data = await r.json();
    render(data);
  }catch(e){
    document.getElementById('out').innerHTML = '<p class="err">Ошибка: '+esc(e.message)+'</p>';
  }
  btn.disabled = false; btn.textContent = 'Запустить';
}

function render(data){
  let h = '';
  if(data.error){document.getElementById('out').innerHTML='<p class="err">'+esc(data.error)+'</p>';return}
  for(const res of data.results){
    h += '<div class="seed-block">';
    h += '<div class="seed-title">'+esc(res.seed)+'</div>';
    if(res.error){h+='<p class="err">'+esc(res.error)+'</p></div>';continue}

    // таблицы токенов по прогонам
    for(let i=0;i<res.runs.length;i++){
      const run = res.runs[i];
      const rlbl = (run.queries||((res.runs[i]||{}).error==='parse_fail_b')) ? 'B'+(i-2) : (i<3?'A'+(i+1):'B'+(i-2));
      h += '<details><summary>Прогон '+rlbl+(run.error?' — ОШИБКА':'')+'</summary>';
      if(run.error){h+='<p class="err">'+esc(run.error)+(run.raw?'<br>'+esc(run.raw):'')+'</p>'}
      else if(run.queries){
        h += '<div style="font-size:13px;padding:4px 0">' + run.queries.map(esc).join('<br>') + '</div>';
      }
      else{
        h += '<table><tr><th>Слово</th><th>Роль</th><th>Замены</th></tr>';
        for(const tk of run.tokens){
          h += '<tr><td>'+esc(tk.word)+'</td><td>'+esc(tk.role)+'</td><td>'+esc((tk.subs||[]).join(', '))+'</td></tr>';
        }
        h += '</table>';
      }
      h += '</details>';
    }

    // кандидаты
    h += '<table><tr><th>Вариант</th><th>Класс</th><th>Замена</th><th>Голоса</th><th>Ср. позиция</th><th>Верификатор</th></tr>';
    for(const c of res.candidates){
      const v = c.verdict===1?'<span class="ok">'+(c.score||'1')+'</span>':(c.verdict===0?'<span class="cut">'+(c.score||'0')+'</span>':'—');
      const rep = c.orig ? esc(c.orig)+' → '+esc(c.sub) : '—';
      h += '<tr><td>'+esc(c.variant)+'</td><td>'+esc(c.cls||'A')+'</td><td>'+rep+'</td><td>'+c.votes+'</td><td>'+c.avg_pos+'</td><td>'+v+'</td></tr>';
    }
    h += '</table>';

    h += '<div class="final"><b>В работу ('+res.final.length+'):</b> '+(res.final.map(esc).join(' • ')||'—')+'</div>';

    const s = res.stats;
    h += '<div class="stats">Генерация: '+s.gen.tin+'/'+s.gen.tout+' ток, $'+s.gen.cost.toFixed(5)+', '+s.gen.wall.toFixed(1)+'s'
       + ' | Верификатор: '+s.ver.tin+'/'+s.ver.tout+' ток, $'+s.ver.cost.toFixed(5)+', '+s.ver.wall.toFixed(1)+'s</div>';
    h += '</div>';
  }
  h += '<div class="total">Конфиг: генератор '+esc(data.config.gen)+' | верификатор '+esc(data.config.ver)
     + '<br>Итого: $'+data.total.cost.toFixed(4)+' | '+data.total.wall.toFixed(1)+'s</div>';
  document.getElementById('out').innerHTML = h;
}
</script>
</body>
</html>
"""


@router.get("/relevant-test", response_class=HTMLResponse)
async def relevant_test_page():
    return HTML_PAGE
