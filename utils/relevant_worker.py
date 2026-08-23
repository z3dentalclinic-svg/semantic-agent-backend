# relevant_worker.py
# Воркер релевантного поиска — НОВЫЙ режим рядом со старым, старый /api/light-search не трогается.
#
# Шаг 1: сид → сырой парсинг ‖ генерация вариантов → варианты парсятся ПО ОЧЕРЕДИ → контейнер
#   с дедупликацией и меткой источника на каждом ключе (filters=none — только сбор сырья).
# Шаг 2 (конвейер с инкрементальным дедупом): цепочка фильтров seed-специфична (L0/L1.5/L2 —
#   ключ относительно сида), поэтому каждый источник фильтруется СО СВОИМ сидом, как отдельный
#   прямой поиск. Источник спарсился → ключи, уже виденные у предыдущих источников, выкинуты
#   (ключ фильтруется один раз — сидом, который принёс его первым; двух вердиктов не бывает) →
#   остаток уходит в фильтры в фоне, пока парсится следующий источник. Слияние VALID/GREY/blocked
#   после фильтров. Замеры экономии дедупа и перекрытия фильтрации с парсингом — в ответе.
#
# Подключение в main.py (после apply_filters_endpoint; старый путь не меняется):
#   from relevant_worker import register_relevant_worker
#   register_relevant_worker(app, light_search_endpoint,
#       filter_ctx={"apply": apply_filters_traced, "l2": _build_l2_config,
#                   "l25": _build_l2_5_config, "l3": _build_l3_config})
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
# WORKER_BUILD = "relevant_worker 0.1 (raw merge, no filters)"
# WORKER_BUILD = "relevant_worker 0.2 (pipeline: per-source filters + incremental dedup)"
# WORKER_BUILD = "relevant_worker 0.2.1 (pipeline; filter crash -> keys to GREY as filter_error)"
WORKER_BUILD = "relevant_worker 0.3 (+ cross L2.5: variant VALID vs original seed, 3.7-flash b512)"
# Перекрёстный L2.5: VALID варианта прошёл цепочку со СВОИМ сидом, но исходного сида не видел —
# «доставка букетов из конфет» валиден для сида «доставка букетов» и мусор для «доставка цветов».
# Поэтому VALID каждого варианта дополнительно гонится через тот же L2.5 с ИСХОДНЫМ сидом.
# VALID сида не гонится: его фильтр-сид и есть исходный, второй прогон дал бы тот же ответ.
CROSS_L25_MODEL = "gemini-3.7-flash"
CROSS_L25_BUDGET = 512
CROSS_L25_PRICE = (0.75, 3.75)   # $/1M in, out — как в relevant_search.PRICE
DEFAULT_FILTERS = "pre,geo,bpf,rel,l0,l15v2,l2,l25,l3"   # полная цепочка как в autopilot/index


def _norm_key(kw):
    return " ".join(str(kw).lower().split())


def _cost(stats):
    """cost_usd из stats-объекта стадии (терпимо к типу и отсутствию) — как stageCost в autopilot."""
    if not isinstance(stats, dict) or stats.get("error"):
        return 0.0
    v = stats.get("cost_usd", stats.get("cost"))
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def register_relevant_worker(app, light_search_fn, filter_ctx=None):
    """light_search_fn — функция light_search_endpoint из main.py (вызов напрямую, минуя HTTP),
    чтобы сырьё вариантов совпадало с сырьём index/autopilot.
    filter_ctx — {"apply": apply_filters_traced, "l2": _build_l2_config,
                  "l25": _build_l2_5_config, "l3": _build_l3_config} из main.py.
    Без filter_ctx воркер умеет только filters=none (сбор сырья)."""

    try:
        from filters.l2_5_filter import apply_l2_5_filter, L2_5Config
    except ImportError:
        apply_l2_5_filter = L2_5Config = None   # без модуля перекрёстный L2.5 отключён

    # Цепочки фильтров не пересекаются между собой (parser.tracer и
    # parser.skip_relevance_filter — глобальные), но перекрываются с парсингом следующего источника.
    filter_lock = asyncio.Lock()

    def _filter_sync(keywords, seed, country, language, filters, l2, l3_model, l3_effort, cross_seed=None):
        """Один прогон цепочки фильтров для одного источника — зеркало apply_filters_endpoint.
        cross_seed задан (источник-вариант) → после цепочки его VALID гонится через
        перекрёстный L2.5 с исходным сидом; срезы уходят в anchors с reason L2_5_TRASH."""
        result = {"seed": seed, "method": "relevant-search", "keywords": list(keywords),
                  "anchors": [], "count": len(keywords), "anchors_count": 0}
        l2_config = filter_ctx["l2"](*l2)
        l2_5_config = filter_ctx["l25"]()
        l3_config = filter_ctx["l3"](model=l3_model, effort=l3_effort)
        result = filter_ctx["apply"](result, seed=seed, country=country, method="relevant-search",
                                     language=language, enabled_filters=filters,
                                     l2_config=l2_config, l2_5_config=l2_5_config, l3_config=l3_config)
        if cross_seed and apply_l2_5_filter and result.get("keywords"):
            t0 = time.time()
            cfg = L2_5Config(region=country, language=language,
                             model=CROSS_L25_MODEL, thinking_budget=CROSS_L25_BUDGET,
                             price_in=CROSS_L25_PRICE[0], price_out=CROSS_L25_PRICE[1])
            n_before = len(result["keywords"])
            # apply_l2_5_filter пишет свои stats/trace в те же ключи result — сохранить цепочные,
            # иначе стоимость цепочного L2.5 теряется, а перекрёстная считается дважды
            chain_stats, chain_trace = result.get("l2_5_stats"), result.get("_l2_5_trace")
            n_anchors_before = len(result.get("anchors") or [])
            try:
                # тот же промпт V3, тот же интерфейс — меняется только сид (исходный) и модель
                result = apply_l2_5_filter(result, seed=cross_seed, enable_l2_5=True, config=cfg)
                xs = result.get("l2_5_stats") or {}
                result["cross_l2_5_stats"] = xs
                result["cross_l2_5_stats"]["cut"] = n_before - len(result["keywords"])
                result["_cross_l2_5_trace"] = result.get("_l2_5_trace")
                # срезы перекрёстного прогона помечаются отдельной причиной,
                # чтобы в Blocked их было видно отдельно от цепочного L2.5
                for a in result["anchors"][n_anchors_before:]:
                    if isinstance(a, dict) and a.get("anchor_reason") == "L2_5_TRASH":
                        a["anchor_reason"] = "L2_5_CROSS_TRASH"
                result["cross_l2_5_cut_keys"] = [a["keyword"] for a in result["anchors"][n_anchors_before:]
                                                 if isinstance(a, dict)]
            except Exception as e:
                logger.error(f"[RELEVANT] cross L2.5 error for '{seed}': {e}")
                result["cross_l2_5_stats"] = {"error": str(e), "input": n_before}
            result["cross_l2_5_stats"]["wall"] = round(time.time() - t0, 2)
            result["l2_5_stats"], result["_l2_5_trace"] = chain_stats, chain_trace
        return result

    async def _filter_source(src_entry, keywords, seed, country, language, filters, l2, l3_model, l3_effort, cross_seed=None):
        """При падении всей цепочки исключением (не сбой L2.5/L3 — те ловятся внутри цепочки)
        ключи источника уходят в GREY с пометкой filter_error, чтобы не пропадать из результата."""
        async with filter_lock:
            src_entry["filter_start"] = time.time()
            try:
                res = await asyncio.to_thread(_filter_sync, keywords, seed, country, language,
                                              filters, l2, l3_model, l3_effort, cross_seed)
            except Exception as e:
                logger.error(f"[RELEVANT] filter error for '{seed}': {e}")
                src_entry["filter_error"] = str(e)
                res = {"keywords": [], "keywords_grey": list(keywords), "anchors": [],
                       "_trace": {"blocked_keywords": {}},
                       "_filter_error": str(e), "_filter_timings": {}}
            src_entry["filter_end"] = time.time()
            src_entry["filter_wall"] = round(src_entry["filter_end"] - src_entry["filter_start"], 2)
            return res

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
        filters: str = Query(DEFAULT_FILTERS, description="none = только сбор сырья"),
        l3_model: str = Query(None),
        l3_effort: str = Query(None),
        l2_pmi_valid: float = Query(None),
        l2_centroid_valid: float = Query(None),
        l2_centroid_trash: float = Query(None),
    ):
        t_total = time.time()
        seed_in = " ".join(seed.strip().split())
        stages = {}
        do_filter = filters.lower().strip() != "none"
        if do_filter and not filter_ctx:
            return {"error": "filter_ctx не передан в register_relevant_worker — фильтры недоступны"}
        l2 = (l2_pmi_valid, l2_centroid_valid, l2_centroid_trash)
        filter_tasks = []   # (src_entry, task) — фильтрация в фоне по конвейеру

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
            """Инкрементальный дедуп: в фильтры уходят только ключи, не виденные у предыдущих источников."""
            fresh = []
            for kw in res.get("keywords") or []:
                k = _norm_key(kw)
                if not k:
                    continue
                e = container.get(k)
                if e is None:
                    container[k] = {"keyword": kw, "sources": [src]}
                    fresh.append(kw)
                elif src not in e["sources"]:
                    e["sources"].append(src)
            return fresh

        def start_filter(entry, fresh, src_seed, cross_seed=None):
            if do_filter and fresh:
                entry["to_filter"] = len(fresh)
                task = asyncio.create_task(_filter_source(entry, fresh, src_seed, country, language,
                                                          filters, l2, l3_model, l3_effort, cross_seed))
                filter_tasks.append((entry, task))
            else:
                entry["to_filter"] = len(fresh) if do_filter else 0

        seed_fresh = absorb(seed_res, "seed")
        sources = [{"id": "seed", "query": seed_used, "raw": len(seed_res.get("keywords") or []),
                    "new": len(seed_fresh), "wall": seed_res.get("_wall", 0),
                    "suffix": seed_res.get("suffix_count", 0), "prefix": seed_res.get("prefix_count", 0),
                    "infix": seed_res.get("infix_count", 0)}]
        start_filter(sources[0], seed_fresh, seed_used)

        # ── Этап 2: варианты ПО ОЧЕРЕДИ ────────────────────────────────
        t0 = time.time()
        for i, v in enumerate(variants, start=1):
            src = f"v{i}"
            t_parse = time.time()
            try:
                vres = await _raw_parse(v, country, region_id, language, use_numbers, parallel_limit, operator)
            except Exception as e:
                logger.error(f"[RELEVANT] variant '{v}' parse error: {e}")
                sources.append({"id": src, "query": v, "error": str(e), "raw": 0, "new": 0, "wall": 0})
                continue
            fresh = absorb(vres, src)
            v_seed = vres.get("seed", v)
            entry = {"id": src, "query": v_seed, "raw": len(vres.get("keywords") or []),
                     "new": len(fresh), "wall": vres.get("_wall", 0),
                     "parse_start": round(t_parse, 3), "parse_end": round(time.time(), 3),
                     "suffix": vres.get("suffix_count", 0), "prefix": vres.get("prefix_count", 0),
                     "infix": vres.get("infix_count", 0)}
            sources.append(entry)
            start_filter(entry, fresh, v_seed, cross_seed=seed_used)   # перекрёстный L2.5 с исходным сидом
        stages["variants_parse"] = round(time.time() - t0, 2)

        merged = sorted(container.values(), key=lambda e: e["keyword"].lower())
        out = {
            "seed": seed_used,
            "original_seed": seed_in if seed_used != seed_in else None,
            "method": "relevant_search",
            "build": {"worker": WORKER_BUILD, "relevant": RS_BUILD},
            "keywords": [e["keyword"] for e in merged],          # сырьё; при фильтрации ниже заменяется на VALID
            "keywords_sources": {e["keyword"]: e["sources"] for e in merged},
            "count": len(merged),
            "raw_count": len(merged),
            "filters": filters,
            "sources": sources,
            "relevant": {"final": rel.get("final", []), "families": rel.get("families", []),
                         "candidates": rel.get("candidates", []), "stats": rel.get("stats", {}),
                         "error": rel_error},
            "stages": stages,
        }

        # ── Этап 3: дождаться хвоста конвейера и слить результаты фильтров ──
        if do_filter:
            t0 = time.time()
            parse_done = time.time()
            valid, grey, anchors, blocked_trace = [], [], [], {}
            seen_v, seen_g = set(), set()
            cost25 = cost3 = 0.0
            per_source_stats = {}
            for entry, task in filter_tasks:
                res = await task
                if res.get("_filter_error"):
                    entry["grey_reason"] = "filter_error"
                for kw in res.get("keywords") or []:
                    k = _norm_key(kw if isinstance(kw, str) else kw.get("query", kw.get("keyword", "")))
                    if k and k not in seen_v:
                        seen_v.add(k); valid.append(kw)
                for kw in res.get("keywords_grey") or []:
                    k = _norm_key(kw if isinstance(kw, str) else kw.get("query", kw.get("keyword", "")))
                    if k and k not in seen_g:
                        seen_g.add(k); grey.append(kw)
                anchors.extend(res.get("anchors") or [])
                tr = res.get("_trace") or {}
                if isinstance(tr, dict):
                    blocked_trace.update(tr.get("blocked_keywords") or {})
                s25 = res.get("l2_5_stats") or res.get("l25_stats") or {}
                s3 = res.get("l3_stats") or {}
                c25 = _cost(s25); c3 = _cost(s3)
                cost25 += c25; cost3 += c3
                entry["valid"] = len(res.get("keywords") or [])
                entry["grey"] = len(res.get("keywords_grey") or [])
                entry["blocked"] = len(res.get("anchors") or [])
                entry["cost_l25"] = round(c25, 5); entry["cost_l3"] = round(c3, 5)
                xs = res.get("cross_l2_5_stats") or {}
                if xs:
                    cx = _cost(xs)
                    cost25 += cx
                    entry["cross_cut"] = xs.get("cut", 0)
                    entry["cross_cut_keys"] = res.get("cross_l2_5_cut_keys") or []
                    entry["cost_cross"] = round(cx, 5)
                    entry["cross_wall"] = xs.get("wall", 0)
                    if xs.get("error"):
                        entry["cross_error"] = xs["error"]
                entry["filter_timings"] = res.get("_filter_timings", {})
                per_source_stats[entry["id"]] = {"l2_5_stats": s25, "l3_stats": s3,
                                                 "cross_l2_5_stats": xs or None}
            stages["filters_tail_wait"] = round(time.time() - t0, 2)   # сколько ждали хвост после парсинга

            # Перекрытие: время фильтрации, прошедшее, пока ещё шёл парсинг (= сэкономлено конвейером)
            overlap = 0.0
            for entry in sources:
                fs, fe = entry.get("filter_start"), entry.get("filter_end")
                if fs and fe:
                    overlap += max(0.0, min(fe, parse_done) - fs)
            stages["filters_overlap_with_parse"] = round(overlap, 2)
            stages["dedup_removed"] = sum(e["raw"] - e["new"] for e in sources)

            out["keywords"] = valid
            out["keywords_grey"] = grey
            out["anchors"] = anchors
            out["count"] = len(valid)
            out["anchors_count"] = len(anchors)
            out["_trace"] = {"blocked_keywords": blocked_trace}
            out["l2_5_stats"] = {"cost_usd": round(cost25, 6), "per_source": {k: v["l2_5_stats"] for k, v in per_source_stats.items()}}
            out["l3_stats"] = {"cost_usd": round(cost3, 6), "per_source": {k: v["l3_stats"] for k, v in per_source_stats.items()}}

        elapsed = round(time.time() - t_total, 2)
        out["elapsed_time"] = elapsed
        logger.info(
            f"[RELEVANT] seed='{seed_used}' | variants={len(variants)} | "
            f"seed_raw={sources[0]['raw']} raw_merged={len(merged)} valid={out['count']} | "
            f"filters={filters} | {elapsed}s"
        )
        return out

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
