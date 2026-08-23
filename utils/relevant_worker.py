# relevant_worker.py
# Воркер релевантного поиска — НОВЫЙ режим рядом со старым, старый /api/light-search не трогается.
#
# Шаг 1 (этот файл): сид → сырой парсинг ‖ генерация вариантов → варианты парсятся ПО ОЧЕРЕДИ
#   → общий контейнер с дедупликацией и пометкой источника. Фильтры НЕ применяются (filters=none):
#   подключение цепочки pre…L3 к объединённому сырью — следующий шаг, отдельно.
#
# Подключение в main.py (после определения light_search_endpoint):
#   from relevant_worker import register_relevant_worker
#   register_relevant_worker(app, light_search_endpoint)
#
# Эндпоинты: GET /api/relevant-search (воркер), GET /relevant-search (промежуточный HTML).

import time
import asyncio
import logging
import httpx
from fastapi import Query
from fastapi.responses import HTMLResponse

try:
    from utils.relevant_search import relevant_variants, BUILD as RS_BUILD   # файл в utils/
except ImportError:
    from relevant_search import relevant_variants, BUILD as RS_BUILD         # файл в корне

logger = logging.getLogger(__name__)
WORKER_BUILD = "relevant_worker 0.1 (raw merge, no filters)"


def _norm_key(kw):
    return " ".join(str(kw).lower().split())


def register_relevant_worker(app, light_search_fn):
    """light_search_fn — функция light_search_endpoint из main.py (вызов напрямую, минуя HTTP),
    чтобы сырьё вариантов совпадало с сырьём index/autopilot."""

    async def _raw_parse(seed, country, region_id, language, use_numbers, parallel_limit, operator):
        """Сырой парсинг одного запроса тем же путём, что light-search с filters=none."""
        t0 = time.time()
        res = await light_search_fn(
            seed=seed, country=country, region_id=region_id, language=language,
            use_numbers=use_numbers, parallel_limit=parallel_limit, source="google",
            filters="none", operator=operator,
            l2_pmi_valid=None, l2_centroid_valid=None, l2_centroid_trash=None,
        )
        res["_wall"] = round(time.time() - t0, 2)
        return res

    @app.get("/api/relevant-search")
    async def relevant_search_endpoint(
        seed: str = Query(..., description="Базовый запрос"),
        country: str = Query("ua"),
        region_id: int = Query(143),
        language: str = Query("auto"),
        use_numbers: bool = Query(False),
        parallel_limit: int = Query(5),
        operator: str = Query("купить"),
    ):
        t_total = time.time()
        seed_in = " ".join(seed.strip().split())
        stages = {}

        # ── Этап 1: сид в парсер ‖ генерация вариантов ──────────────────
        t0 = time.time()
        async with httpx.AsyncClient() as client:
            seed_res, rel = await asyncio.gather(
                _raw_parse(seed_in, country, region_id, language, use_numbers, parallel_limit, operator),
                relevant_variants(seed_in, client),
                return_exceptions=True,
            )
        stages["seed_parse_and_gen"] = round(time.time() - t0, 2)

        if isinstance(seed_res, Exception):
            logger.error(f"[RELEVANT] seed parse error: {seed_res}")
            return {"error": f"seed parse: {seed_res}", "seed": seed_in}

        rel_error = None
        if isinstance(rel, Exception):
            rel_error = str(rel)
            logger.error(f"[RELEVANT] generation error: {rel}")
            rel = {"seed": seed_in, "final": [], "families": [], "candidates": [], "stats": {}}

        seed_used = seed_res.get("seed", seed_in)   # спеллер бэкенда мог поправить сид
        variants = [v for v in rel.get("final", []) if _norm_key(v) != _norm_key(seed_used)]

        # ── Контейнер: norm_key -> {"keyword", "sources"} ─────────────────
        container = {}

        def absorb(res, src):
            new = 0
            for kw in res.get("keywords") or []:
                k = _norm_key(kw)
                if not k:
                    continue
                e = container.get(k)
                if e is None:
                    container[k] = {"keyword": kw, "sources": [src]}
                    new += 1
                elif src not in e["sources"]:
                    e["sources"].append(src)
            return new

        seed_new = absorb(seed_res, "seed")
        sources = [{"id": "seed", "query": seed_used, "raw": len(seed_res.get("keywords") or []),
                    "new": seed_new, "wall": seed_res.get("_wall", 0),
                    "suffix": seed_res.get("suffix_count", 0), "prefix": seed_res.get("prefix_count", 0),
                    "infix": seed_res.get("infix_count", 0)}]

        # ── Этап 2: варианты ПО ОЧЕРЕДИ ────────────────────────────────
        t0 = time.time()
        for i, v in enumerate(variants, start=1):
            src = f"v{i}"
            try:
                vres = await _raw_parse(v, country, region_id, language, use_numbers, parallel_limit, operator)
            except Exception as e:
                logger.error(f"[RELEVANT] variant '{v}' parse error: {e}")
                sources.append({"id": src, "query": v, "error": str(e), "raw": 0, "new": 0, "wall": 0})
                continue
            n_new = absorb(vres, src)
            sources.append({"id": src, "query": vres.get("seed", v), "raw": len(vres.get("keywords") or []),
                            "new": n_new, "wall": vres.get("_wall", 0),
                            "suffix": vres.get("suffix_count", 0), "prefix": vres.get("prefix_count", 0),
                            "infix": vres.get("infix_count", 0)})
        stages["variants_parse"] = round(time.time() - t0, 2)

        merged = sorted(container.values(), key=lambda e: e["keyword"].lower())
        elapsed = round(time.time() - t_total, 2)
        logger.info(
            f"[RELEVANT] seed='{seed_used}' | variants={len(variants)} | "
            f"seed_raw={sources[0]['raw']} merged={len(merged)} | {elapsed}s"
        )

        return {
            "seed": seed_used,
            "original_seed": seed_in if seed_used != seed_in else None,
            "method": "relevant_search",
            "build": {"worker": WORKER_BUILD, "relevant": RS_BUILD},
            "keywords": [e["keyword"] for e in merged],          # формат light-search: список строк
            "keywords_sources": {e["keyword"]: e["sources"] for e in merged},
            "count": len(merged),
            "sources": sources,
            "relevant": {"final": rel.get("final", []), "families": rel.get("families", []),
                         "candidates": rel.get("candidates", []), "stats": rel.get("stats", {}),
                         "error": rel_error},
            "stages": stages,
            "elapsed_time": elapsed,
        }

    @app.get("/relevant-search", response_class=HTMLResponse)
    async def relevant_search_page():
        return HTML_PAGE


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>Релевантный поиск — воркер</title>
<style>
  body{font-family:-apple-system,Segoe UI,sans-serif;background:#FAF9F6;color:#1a1a1a;margin:0;padding:24px;max-width:1100px;margin:auto}
  h1{font-size:20px}
  input,select,button{font-size:14px;padding:8px 12px;border-radius:8px;border:1px solid #ccc;margin-right:8px}
  input#seed{width:420px}
  button{background:#059669;color:#fff;border:none;cursor:pointer;padding:10px 22px}
  button:disabled{background:#9ca3af}
  .row{margin:12px 0;display:flex;flex-wrap:wrap;gap:10px;align-items:center}
  .block{background:#fff;border:1px solid #e5e5e5;border-radius:10px;padding:16px;margin:16px 0}
  table{border-collapse:collapse;width:100%;font-size:13px;margin:8px 0}
  th,td{border:1px solid #e5e5e5;padding:5px 8px;text-align:left}
  th{background:#f3f4f6}
  .ok{color:#059669;font-weight:600}
  .err{color:#dc2626;font-size:13px}
  .total{background:#111;color:#fff;border-radius:10px;padding:14px;margin:20px 0;font-size:14px}
  .src{font-size:11px;color:#666}
  details{margin:6px 0} summary{cursor:pointer;font-size:13px;color:#555}
  #status{font-size:13px;color:#555;margin:8px 0}
</style>
</head>
<body>
<h1>Релевантный поиск — воркер (сырьё, без фильтров)</h1>

<div class="row">
  <input id="seed" placeholder="сид, например: доставка цветов киев">
  <select id="country"><option value="ua" selected>ua</option><option value="ru">ru</option><option value="kz">kz</option><option value="by">by</option></select>
  <select id="language"><option value="auto" selected>auto</option><option value="ru">ru</option><option value="uk">uk</option></select>
  <button id="run" onclick="run()">Запустить</button>
</div>
<div id="status"></div>
<div id="out"></div>

<script>
function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
var base = 'https://semantic-agent-backend.onrender.com';

async function run(){
  var seed = document.getElementById('seed').value.trim();
  if(!seed){alert('Введи сид');return}
  var btn = document.getElementById('run');
  btn.disabled = true; btn.textContent = 'Работаю...';
  document.getElementById('out').innerHTML = '';
  document.getElementById('status').textContent = 'Сид в парсер ‖ генерация вариантов, затем варианты по очереди...';
  var t0 = Date.now();
  try{
    var r = await fetch(base + '/api/relevant-search?seed=' + encodeURIComponent(seed)
      + '&country=' + document.getElementById('country').value
      + '&language=' + document.getElementById('language').value);
    if(!r.ok) throw new Error('HTTP ' + r.status);
    var data = await r.json();
    render(data, (Date.now()-t0)/1000);
  }catch(e){
    document.getElementById('out').innerHTML = '<p class="err">Ошибка: '+esc(e.message)+'</p>';
  }
  document.getElementById('status').textContent = '';
  btn.disabled = false; btn.textContent = 'Запустить';
}

function render(d, clientSec){
  if(d.error){document.getElementById('out').innerHTML='<p class="err">'+esc(d.error)+'</p>';return}
  var h = '';
  var rel = d.relevant || {};
  var st = rel.stats || {};

  h += '<div class="block"><b>Сид:</b> '+esc(d.seed)+(d.original_seed?' <span class="src">(исправлен из «'+esc(d.original_seed)+'»)</span>':'');
  h += '<br><b>Варианты в работу ('+(rel.final||[]).length+'):</b> '+((rel.final||[]).map(esc).join(' • ')||'—');
  if(rel.error) h += '<p class="err">Генерация: '+esc(rel.error)+'</p>';
  if(st.gen) h += '<div class="src">Генерация: '+st.gen.tin+'/'+st.gen.tout+' ток, $'+(st.gen.cost||0).toFixed(5)+', '+(st.gen.wall||0).toFixed(1)+'s'
      + ' | Кластер: '+st.ver.tin+'/'+st.ver.tout+' ток, $'+(st.ver.cost||0).toFixed(5)+', '+(st.ver.wall||0).toFixed(1)+'s'
      + (st.ver.fallback_json?' | откат на JSON':'')+' | цепочка '+(st.total_wall||0).toFixed(1)+'s | '+esc(st.build||'')+'</div>';
  h += '</div>';

  if(rel.families && rel.families.length){
    h += '<div class="block"><details><summary>Семьи ('+rel.families.length+')</summary><table><tr><th>Представитель</th><th>Члены</th></tr>';
    for(var f of rel.families) h += '<tr><td><b>'+esc(f.rep)+'</b></td><td>'+f.members.map(esc).join('<br>')+'</td></tr>';
    h += '</table></details>';
    if(rel.candidates && rel.candidates.length){
      h += '<details><summary>Все кандидаты ('+rel.candidates.length+')</summary><table><tr><th>Вариант</th><th>Класс</th><th>Голоса</th><th>Семья</th></tr>';
      for(var c of rel.candidates) h += '<tr><td>'+esc(c.variant)+'</td><td>'+esc(c.cls)+'</td><td>'+c.votes+'</td><td>'+(c.verdict===1?'<span class="ok">'+esc(c.score)+'</span>':esc(c.score))+'</td></tr>';
      h += '</table></details>';
    }
    h += '</div>';
  }

  h += '<div class="block"><b>Источники сырья</b><table><tr><th>Источник</th><th>Запрос</th><th>suffix/prefix/infix</th><th>Сырых</th><th>Новых в контейнер</th><th>Время</th></tr>';
  for(var s of d.sources||[]){
    h += '<tr><td>'+esc(s.id)+'</td><td>'+esc(s.query)+(s.error?'<br><span class="err">'+esc(s.error)+'</span>':'')+'</td><td>'
       + (s.error?'—':(s.suffix+'/'+s.prefix+'/'+s.infix))+'</td><td>'+s.raw+'</td><td><b>'+s.new+'</b></td><td>'+s.wall+'s</td></tr>';
  }
  h += '</table></div>';

  var kws = d.keywords||[], srcs = d.keywords_sources||{};
  h += '<div class="block"><details open><summary>Объединённый список ('+kws.length+')</summary><table><tr><th>#</th><th>Ключ</th><th>Источники</th></tr>';
  for(var i=0;i<kws.length;i++) h += '<tr><td>'+(i+1)+'</td><td>'+esc(kws[i])+'</td><td class="src">'+esc((srcs[kws[i]]||[]).join(', '))+'</td></tr>';
  h += '</table></details></div>';

  var sg = d.stages||{};
  h += '<div class="total">Сид от парсера: '+(d.sources&&d.sources[0]?d.sources[0].raw:0)+' → контейнер: '+d.count
     + '<br>Этапы: сид ‖ генерация '+(sg.seed_parse_and_gen||0)+'s | варианты по очереди '+(sg.variants_parse||0)+'s | всего '+d.elapsed_time+'s (клиент '+clientSec.toFixed(1)+'s)'
     + '<br><span class="src">'+esc((d.build||{}).worker)+' | '+esc((d.build||{}).relevant)+'</span></div>';
  document.getElementById('out').innerHTML = h;
}
</script>
</body>
</html>
"""
