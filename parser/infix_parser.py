"""
Infix Parser v2.0

Changes vs v1.0:
  - _is_garbage_keyword() фильтр: спецсимволы, одиночные буквы не предлоги/союзы
  - Delay 0.5s локально, 0.3s на сервере
  - Batch 5 (антибан)
"""

import asyncio
import httpx
import time
import random
import json
import re
import os
import logging
import argparse
from typing import Set, List, Dict, Optional
from dataclasses import dataclass, field, asdict
from collections import Counter, defaultdict

try:
    from parser.infix_generator import InfixGenerator, InfixQuery, ALL_GROUPS
except ImportError:
    from infix_generator import InfixGenerator, InfixQuery, ALL_GROUPS

logger = logging.getLogger(__name__)

UA_CHROME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
UA_FIREFOX = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"

DELAY_LOCAL  = 0.3
DELAY_SERVER = 0.3
BATCH_SIZE   = 5

# Два отдельных IP: Chrome и Firefox идут на разные прокси
# Если INFIX_PROXY_CHROME/FF не заданы — fallback на GOOGLE_PROXY_URL
try:
    from utils.proxy_pool import ProxyPool
    _proxy_chrome  = ProxyPool.get("infix_chrome")
    _proxy_firefox = ProxyPool.get("infix_firefox")
    _proxy_safari  = ProxyPool.get("infix_safari")
except ImportError:
    try:
        from proxy_pool import ProxyPool
        _proxy_chrome  = ProxyPool.get("infix_chrome")
        _proxy_firefox = ProxyPool.get("infix_firefox")
        _proxy_safari  = ProxyPool.get("infix_safari")
    except ImportError:
        _proxy_chrome  = os.getenv("INFIX_PROXY_CHROME") or os.getenv("GOOGLE_PROXY_URL") or None
        _proxy_firefox = os.getenv("INFIX_PROXY_FF")     or os.getenv("GOOGLE_PROXY_URL") or None
        _proxy_safari  = None

_google_proxy = _proxy_chrome  # для обратной совместимости
DELAY = DELAY_SERVER if (_proxy_chrome or _proxy_firefox) else DELAY_LOCAL

try:
    from utils.geo_uule import get_uule
except ImportError:
    try:
        from geo_uule import get_uule
    except ImportError:
        get_uule = lambda cc, city=None: None

# Предлоги и союзы — одиночные буквы из этого списка НЕ мусор
PREP_UNION = {"в","во","на","с","со","к","ко","о","у","и","а","б","я"}


# ══════════════════════════════════════════════
# ФИЛЬТР МУСОРА
# ══════════════════════════════════════════════

def _is_garbage_keyword(kw: str) -> bool:
    """
    Возвращает True если ключ — мусор:
    1. Содержит спецсимволы запроса (* : & | \)
    2. Содержит одиночную букву которая не предлог/союз
    """
    # Спецсимволы — Google вернул запрос обратно
    if re.search(r'[*:|&\\]', kw):
        return True
    # Одиночная буква не предлог/союз — вставка не раскрылась
    for w in kw.lower().split():
        if len(w) == 1 and w not in PREP_UNION:
            return True
    return False


# ══════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════

@dataclass
class InfixTraceEntry:
    gap_index: int
    w1: str
    w2: str
    group: str
    struct: str
    insert_val: str
    insert_type: str
    orientation: str
    query_sent: str
    cp: int
    cp_note: str
    agent: str
    results_count: int = 0
    results: List[str] = field(default_factory=list)
    unique: List[str] = field(default_factory=list)
    time_ms: float = 0.0
    status: str = "pending"
    error: Optional[str] = None
    letter: Optional[str] = None
    http_status: int = 0


@dataclass
class InfixParseResult:
    seed: str
    country: str
    language: str
    groups_used: List[str]
    all_keywords: Dict[str, List[str]] = field(default_factory=dict)
    alt_seed_keywords: Set[str] = field(default_factory=set)
    exclusive_keywords: Dict[str, str] = field(default_factory=dict)
    total_queries: int = 0
    with_results: int = 0
    empty_queries: int = 0
    error_queries: int = 0
    blocked_queries: int = 0   # 429/500/503 от Google
    timeout_queries: int = 0   # сеть/таймаут
    status_counts: Dict = field(default_factory=dict)  # {"ok": N, "blocked_500": N, ...}
    total_keywords: int = 0
    exclusive_count: int = 0
    total_time_ms: float = 0.0
    trace: List[Dict] = field(default_factory=list)
    summary_by_gap: Dict = field(default_factory=dict)
    summary_by_group: Dict = field(default_factory=dict)
    timestamp: str = ""
    stage_stats: Dict = field(default_factory=dict)  # тайминги по stages (e_chrome/e_firefox/addon_research/non_e_chrome/etc)
    trace_log: List[Dict] = field(default_factory=list)  # подробный пошаговый лог (START/MATRIX_OK/STAGE_STATS/etc) для анализа


# ══════════════════════════════════════════════
# PARSER
# ══════════════════════════════════════════════

class InfixParser:

    def __init__(self, lang: str = "ru"):
        self.generator = InfixGenerator()
        self.lang = lang

    def _clean_suggestion(self, text: str) -> str:
        return re.sub(r'<[^>]+>', '', text).strip()

    async def fetch_suggestions(self, query, country, language, client,
                                 google_client="firefox", cursor_position=None,
                                 uule=None):
        url = "https://www.google.com/complete/search"
        params = {"q": query, "client": google_client,
                  "gl": country, "ie": "utf-8", "oe": "utf-8", "hl": language}
        if uule:
            params["uule"] = uule
        if cursor_position == -1:
            pass
        elif cursor_position is not None:
            params["cp"] = cursor_position
        else:
            params["cp"] = len(query)

        ua = UA_FIREFOX if google_client == "firefox" else UA_CHROME
        headers = {"User-Agent": ua}
        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)
            http_status = response.status_code
            if http_status == 429:
                return [], 429
            if http_status == 200:
                text = response.text.strip()
                try:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 1:
                        raw = data[1]
                        if isinstance(raw, list):
                            result = []
                            for item in raw:
                                if isinstance(item, str):
                                    result.append(self._clean_suggestion(item))
                                elif isinstance(item, list) and len(item) > 0 and isinstance(item[0], str):
                                    result.append(self._clean_suggestion(item[0]))
                                elif isinstance(item, dict):
                                    s = item.get("suggestion") or item.get("value") or item.get("text", "")
                                    if s:
                                        result.append(self._clean_suggestion(str(s)))
                            return result, 200
                except Exception:
                    pass
                if text.startswith(")]}'"):
                    text = text[4:].strip()
                    try:
                        data = json.loads(text)
                        if isinstance(data, list) and len(data) > 1:
                            raw = data[1]
                            if isinstance(raw, list):
                                result = []
                                for item in raw:
                                    if isinstance(item, str):
                                        result.append(self._clean_suggestion(item))
                                    elif isinstance(item, list) and len(item) > 0 and isinstance(item[0], str):
                                        result.append(self._clean_suggestion(item[0]))
                                return result, 200
                    except Exception:
                        pass
            return [], http_status
        except Exception:
            pass
        return [], 0

    async def parse(self, seed, country="ua", language="ru",
                    groups=None, progress_callback=None,
                    city=None) -> InfixParseResult:
        from datetime import datetime
        total_start = time.time()
        timestamp = datetime.utcnow().isoformat() + "Z"

        # ═══ INFIX_TRACE: подробное логирование каждого шага в trace_log ═══
        # Все события парсинга копятся в список, потом отдаются в JSON ответе.
        # Никаких print в stdout (Render логи не нужны).
        trace_log: List[Dict] = []
        _t_zero = total_start
        def _trace(stage: str, **kwargs):
            """Запись события в trace_log. t_ms = от старта parse() в миллисекундах."""
            trace_log.append({
                "stage": stage,
                "t_ms": round((time.time() - _t_zero) * 1000, 1),
                **{k: (str(v) if isinstance(v, (tuple, set)) else v) for k, v in kwargs.items()},
            })
        # ════════════════════════════════════════════════════════

        _trace("START", country=country, language=language, city=city, groups=groups)

        try:
            # uule: city=None → столица страны, city="Lviv" → конкретный город
            _uule = get_uule(country, city)
            _trace("UULE_OK", uule=str(_uule)[:30])
        except Exception as _e:
            _trace("UULE_FAIL", error=repr(_e))
            raise

        try:
            matrix: List[InfixQuery] = self.generator.generate(seed=seed, groups=groups or ALL_GROUPS)
            _trace("MATRIX_OK", n_queries=len(matrix))
        except Exception as _e:
            _trace("MATRIX_FAIL", error=repr(_e))
            raise

        kw_map: Dict[str, List[str]] = {}
        alt_seed_set: Set[str] = set()
        trace_entries: List[InfixTraceEntry] = []
        lock = asyncio.Lock()
        done_count = [0]

        # Старые семафоры (non_e_sem, addon_sem) удалены при рефакторинге v3.0 —
        # теперь единый global_sem = N_IP × CONC_PER_IP создаётся ниже после получения
        # размера research pool.

        async def fetch_one(iq: InfixQuery, client: httpx.AsyncClient):
            agent = iq.agents[0]
            await asyncio.sleep(DELAY)
            t0 = time.time()
            try:
                raw_results, http_status = await self.fetch_suggestions(
                    query=iq.query, country=country, language=language,
                    client=client, google_client=agent, cursor_position=iq.cp,
                    uule=_uule,
                )
                elapsed = (time.time() - t0) * 1000

                # Фильтр мусора
                results = [kw for kw in raw_results if not _is_garbage_keyword(kw)]

                if http_status == 200:
                    status = "ok" if results else "empty"
                elif http_status in (429, 500, 503):
                    status = f"blocked_{http_status}"
                elif http_status == 0:
                    status = "timeout"
                else:
                    status = f"http_{http_status}"

                entry = InfixTraceEntry(
                    gap_index=iq.gap_index, w1=iq.w1, w2=iq.w2,
                    group=iq.group, struct=iq.struct,
                    insert_val=iq.insert_val, insert_type=iq.insert_type,
                    orientation=iq.orientation,
                    query_sent=iq.query, cp=iq.cp, cp_note=iq.cp_note,
                    agent=agent, results=results, results_count=len(results),
                    time_ms=round(elapsed, 1), status=status, letter=iq.letter,
                    http_status=http_status,
                )
                async with lock:
                    for kw in results:
                        k = kw.lower().strip()
                        if not k:
                            continue
                        if k not in kw_map:
                            kw_map[k] = []
                        kw_map[k].append(f"{agent}:{iq.struct}")
                        if seed.lower() not in k:
                            alt_seed_set.add(k)

            except Exception as e:
                elapsed = (time.time() - t0) * 1000
                entry = InfixTraceEntry(
                    gap_index=iq.gap_index, w1=iq.w1, w2=iq.w2,
                    group=iq.group, struct=iq.struct,
                    insert_val=iq.insert_val, insert_type=iq.insert_type,
                    orientation=iq.orientation,
                    query_sent=iq.query, cp=iq.cp, cp_note=iq.cp_note,
                    agent=agent, time_ms=round(elapsed, 1),
                    status="error", error=str(e), letter=iq.letter,
                )

            async with lock:
                trace_entries.append(entry)
                done_count[0] += 1

            if progress_callback:
                await progress_callback(done_count[0], len(matrix), entry)

        # ═══ STAGE TIMING TRACKERS ═══
        # Каждая stage отдельно учитывает свое время и запросы
        # stages: e_chrome, e_safari, e_firefox, non_e_chrome, non_e_firefox, addon_research, addon_chrome, addon_firefox
        stage_stats = defaultdict(lambda: {
            'requests': 0, 'time_ms': 0.0,
            'started_at': None, 'finished_at': None,
            'ok': 0, 'empty': 0, 'error': 0, 'blocked': 0, 'timeout': 0,
        })
        stage_lock = asyncio.Lock()

        async def fetch_with_stage(iq, client, stage):
            """Wrapper: засекает время в рамках конкретной stage."""
            async with stage_lock:
                if stage_stats[stage]['started_at'] is None:
                    stage_stats[stage]['started_at'] = time.time()
            t0 = time.time()
            await fetch_one(iq, client)
            elapsed_ms = (time.time() - t0) * 1000
            async with stage_lock:
                stage_stats[stage]['requests'] += 1
                stage_stats[stage]['time_ms'] += elapsed_ms
                stage_stats[stage]['finished_at'] = time.time()

        # Legacy run_letter/run_non_e удалены в v3.0 — заменены на run_one внутри
        # секции диспатча после получения research pool.

        # ═══ INFIX RESEARCH POOL — 10 IP × conc=6 = 60 параллельных ═══
        # Сафари не используем. Базовые infix_chrome/infix_firefox освобождены —
        # все запросы идут через research pool.
        #
        # Бета-архитектура (2 батча × 25 IP = 50):
        #   suffix 10 + infix 10 + prefix 5 = 25 на батч
        #   2 юзера одновременно → 2 × 10 = 20 IP на инфикс из 50 пула
        #
        # Прогноз (avg latency ~700ms при conc=6):
        #   1 pair (~284 reqs):  ~5с
        #   2 pair (~568 reqs):  ~9с
        #   3 pair (~660 reqs):  ~11с
        _N_INFIX_RESEARCH = 10
        _CONC_PER_IP = 6

        try:
            n_pairs = len(set(iq.gap_index for iq in matrix)) or 1
        except Exception:
            n_pairs = 1

        _research_proxies: list = []
        _pool_source = "none"
        try:
            from utils.proxy_pool import ProxyPool as _PP
            _research_proxies = [_PP.get("infix_research") for _ in range(_N_INFIX_RESEARCH)]
            _research_proxies = [p for p in _research_proxies if p]
            _pool_source = "utils.proxy_pool"
        except Exception as _e1:
            _trace("RESEARCH_POOL_TRY1_FAIL", error=repr(_e1)[:120])
            try:
                from proxy_pool import ProxyPool as _PP
                _research_proxies = [_PP.get("infix_research") for _ in range(_N_INFIX_RESEARCH)]
                _research_proxies = [p for p in _research_proxies if p]
                _pool_source = "proxy_pool"
            except Exception as _e2:
                _trace("RESEARCH_POOL_TRY2_FAIL", error=repr(_e2)[:120])
                _research_proxies = []
                _pool_source = "fallback_empty"
        _trace("RESEARCH_POOL_READY",
               n_proxies=len(_research_proxies),
               target=_N_INFIX_RESEARCH,
               n_pairs=n_pairs,
               conc_per_ip=_CONC_PER_IP,
               source=_pool_source,
               sample=str(_research_proxies[0])[:35] if _research_proxies else "EMPTY")
        # ════════════════════════════════════════════════════════════════════════

        try:
            # Открываем по 1 httpx-клиенту на каждый IP в пуле.
            # Базовые infix_chrome/firefox/safari клиенты НЕ создаются — выкинуты.
            _research_clients = []
            if _research_proxies:
                try:
                    _research_clients = [httpx.AsyncClient(proxy=p) for p in _research_proxies]
                    _trace("RESEARCH_CLIENTS_OPENED", n=len(_research_clients))
                except Exception as _e:
                    _trace("RESEARCH_CLIENTS_FAIL", error=repr(_e)[:200])
                    _research_clients = []

            # Если пул не получили — fallback: открываем 1 клиент без прокси.
            # Это аварийный режим, такого происходить не должно.
            if not _research_clients:
                _research_clients = [httpx.AsyncClient()]
                _trace("FALLBACK_NO_PROXY", n_clients=1)

            try:
                # ═══ ЕДИНЫЙ СЕМАФОР для всех запросов ═══
                # Все запросы инфикса (E + non_e + addon) идут через research_clients
                # с единым семафором = n_ip * conc_per_ip.
                global_sem = asyncio.Semaphore(len(_research_clients) * _CONC_PER_IP)
                n_clients = len(_research_clients)

                async def run_one(iq, client_idx, stage):
                    async with global_sem:
                        await fetch_with_stage(iq, _research_clients[client_idx], stage)

                # Разбиваем матрицу на 3 bucket'а только для трекинга stage:
                # e_pool, non_e_pool, addon_pool — все идут через общий пул round-robin.
                e_tasks = []
                non_e_tasks = []
                addon_tasks = []
                idx = 0
                for iq in matrix:
                    if iq.group == "E" and iq.letter:
                        stage = "e_pool"
                        e_tasks.append(run_one(iq, idx % n_clients, stage))
                    elif getattr(iq, 'is_new_research', False):
                        stage = "addon_pool"
                        addon_tasks.append(run_one(iq, idx % n_clients, stage))
                    else:
                        stage = "non_e_pool"
                        non_e_tasks.append(run_one(iq, idx % n_clients, stage))
                    idx += 1

                _trace("BUCKETS_BUILT",
                       e=len(e_tasks),
                       non_e=len(non_e_tasks),
                       addon=len(addon_tasks),
                       total=len(matrix),
                       global_sem=n_clients * _CONC_PER_IP)
                _trace("GATHER_START", total_tasks=len(matrix), n_ip=n_clients)

                await asyncio.gather(*e_tasks, *non_e_tasks, *addon_tasks)

                elapsed_total = time.time() - total_start
                _trace("GATHER_DONE", elapsed_s=round(elapsed_total, 2))

                # ═══ Детальные таймы по stage ═══
                for stage, st in sorted(stage_stats.items()):
                    if st['requests'] == 0:
                        continue
                    wall_s = (st['finished_at'] - st['started_at']) if st['started_at'] else 0
                    avg_ms = st['time_ms'] / st['requests'] if st['requests'] else 0
                    req_per_s = st['requests'] / wall_s if wall_s > 0 else 0
                    _trace("STAGE_STATS",
                           stage_name=stage,
                           requests=st['requests'],
                           wall_s=round(wall_s, 2),
                           req_per_s=round(req_per_s, 1),
                           avg_ms=round(avg_ms, 0))
            except Exception as _e:
                import traceback as _tb
                _trace("GATHER_FAIL", error=repr(_e)[:200], tb=_tb.format_exc()[-500:])
                raise
            finally:
                # Закрываем все research-клиенты
                for c in _research_clients:
                    try:
                        await c.aclose()
                    except Exception:
                        pass
                _trace("RESEARCH_CLIENTS_CLOSED")
        except Exception as _e:
            import traceback as _tb
            _trace("TOP_FAIL", error=repr(_e)[:200], tb=_tb.format_exc()[-500:])
            raise

        total_time = (time.time() - total_start) * 1000

        # Ротируем батч IP после каждого прогона
        try:
            from utils.proxy_pool import ProxyPool
            ProxyPool.rotate()
        except ImportError:
            try:
                from proxy_pool import ProxyPool
                ProxyPool.rotate()
            except ImportError:
                pass

        for entry in trace_entries:
            entry.unique = [
                kw for kw in entry.results
                if len(kw_map.get(kw.lower().strip(), [])) == 1
            ]

        gaps = sorted(set(e.gap_index for e in trace_entries))
        summary_by_gap = {}
        for gi in gaps:
            ge = [e for e in trace_entries if e.gap_index == gi]
            kws: set = set()
            for e in ge:
                kws.update(k.lower().strip() for k in e.results)
            summary_by_gap[str(gi)] = {
                "gap_index": gi, "w1": ge[0].w1, "w2": ge[0].w2,
                "total_queries": len(ge),
                "with_results": sum(1 for e in ge if e.status == "ok"),
                "empty": sum(1 for e in ge if e.status == "empty"),
                "unique_keywords": len(kws),
                "exclusive": sum(len(e.unique) for e in ge),
            }

        summary_by_group = {}
        for g in ALL_GROUPS:
            ge = [e for e in trace_entries if e.group == g]
            if not ge:
                continue
            kws_g: set = set()
            for e in ge:
                kws_g.update(k.lower().strip() for k in e.results)
            summary_by_group[g] = {
                "total_queries": len(ge),
                "with_results": sum(1 for e in ge if e.status == "ok"),
                "empty": sum(1 for e in ge if e.status == "empty"),
                "unique_keywords": len(kws_g),
                "exclusive": sum(len(e.unique) for e in ge),
                "avg_time_ms": round(sum(e.time_ms for e in ge) / max(len(ge), 1), 1),
            }

        exclusive_kw = {kw: structs[0] for kw, structs in kw_map.items() if len(structs) == 1}

        return InfixParseResult(
            seed=seed, country=country, language=language,
            groups_used=list(set(e.group for e in trace_entries)),
            all_keywords=kw_map, alt_seed_keywords=alt_seed_set,
            exclusive_keywords=exclusive_kw,
            total_queries=len(matrix),
            with_results=sum(1 for e in trace_entries if e.status == "ok"),
            empty_queries=sum(1 for e in trace_entries if e.status == "empty"),
            error_queries=sum(1 for e in trace_entries if e.status == "error"),
            blocked_queries=sum(1 for e in trace_entries if e.status.startswith("blocked_")),
            timeout_queries=sum(1 for e in trace_entries if e.status == "timeout"),
            status_counts=dict(Counter(e.status for e in trace_entries)),
            total_keywords=len(kw_map), exclusive_count=len(exclusive_kw),
            total_time_ms=round(total_time, 1),
            trace=[asdict(e) for e in trace_entries],
            summary_by_gap=summary_by_gap, summary_by_group=summary_by_group,
            timestamp=timestamp,
            stage_stats={
                stage: {
                    'requests': st['requests'],
                    'wall_s': round((st['finished_at'] - st['started_at']) if st['started_at'] else 0, 3),
                    'sum_time_ms': round(st['time_ms'], 1),
                    'avg_ms': round(st['time_ms'] / max(1, st['requests']), 1),
                    'req_per_s': round(st['requests'] / max(0.001, (st['finished_at'] - st['started_at']) if st['started_at'] else 0.001), 1),
                }
                for stage, st in stage_stats.items() if st['requests'] > 0
            },
            trace_log=trace_log,
        )


# ══════════════════════════════════════════════
# LOCAL TEST MODE
# ══════════════════════════════════════════════

async def _local_run(seed, groups, country, language, output_file=None):
    print(f"\n{'='*60}")
    print(f"  Infix Parser v2.0 — LOCAL TEST")
    print(f"  Seed:    {seed}")
    print(f"  Groups:  {groups or 'ALL'}")
    print(f"  Delay:   {DELAY}s | Batch: {BATCH_SIZE}")
    print(f"{'='*60}\n")

    parser = InfixParser()
    matrix = parser.generator.generate(seed=seed, groups=groups or ALL_GROUPS)
    stats = parser.generator.summary(matrix)
    print(f"  Матрица: {stats['total_queries']} запросов (было ~1367, -{round((1-stats['total_queries']/1367)*100)}%)")
    for gi, gd in stats['by_gap'].items():
        print(f"  gap[{gi}]: '{gd['w1']}' ↔ '{gd['w2']}' — {gd['total']} запросов")
    est = stats['total_queries'] * DELAY / BATCH_SIZE
    print(f"  Оценка: ~{est:.0f}s (~{est/60:.1f} мин)\n")

    def make_progress():
        async def cb(done, total, entry):
            icon = "✅" if entry.status == "ok" else ("○" if entry.status == "empty" else "❌")
            cnt = f"{entry.results_count} кл" if entry.status == "ok" else entry.status
            print(f"  [{done:>4}/{total}] {icon} gap[{entry.gap_index}] {entry.group:<4} {entry.struct:<35} {entry.agent:<8} {cnt}")
        return cb

    result = await parser.parse(seed=seed, country=country, language=language,
                                 groups=groups, progress_callback=make_progress())

    print(f"\n{'='*60}")
    print(f"  РЕЗУЛЬТАТЫ")
    print(f"{'='*60}")
    print(f"  Запросов:       {result.total_queries}")
    print(f"  С результатом:  {result.with_results}")
    print(f"  Уникальных кл.: {result.total_keywords}")
    print(f"  Эксклюзивных:   {result.exclusive_count}")
    print(f"  Время:          {result.total_time_ms:.0f}ms")
    print(f"\n  По gap'ам:")
    for gi, s in result.summary_by_gap.items():
        pct = round(s['with_results']/max(s['total_queries'],1)*100)
        print(f"    gap[{gi}] '{s['w1']}' ↔ '{s['w2']}': {s['with_results']}/{s['total_queries']} ({pct}%) | {s['unique_keywords']} кл | {s['exclusive']} эксклюз.")
    print(f"\n  По группам:")
    for g, s in result.summary_by_group.items():
        pct = round(s['with_results']/max(s['total_queries'],1)*100)
        print(f"    {g:<6} {s['with_results']:>4}/{s['total_queries']:<5} ({pct:>3}%) | {s['unique_keywords']:>5} кл | {s['exclusive']:>4} эксклюз.")

    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fn = output_file or f"infix_{seed.replace(' ','_')}_{ts}.json"
    export = {
        "meta": {"seed": result.seed, "country": result.country, "language": result.language,
                 "groups": result.groups_used, "timestamp": result.timestamp,
                 "total_time_ms": result.total_time_ms, "delay": DELAY, "batch_size": BATCH_SIZE},
        "summary": {"total_queries": result.total_queries, "with_results": result.with_results,
                    "empty": result.empty_queries, "errors": result.error_queries,
                    "total_keywords": result.total_keywords, "exclusive_count": result.exclusive_count},
        "summary_by_gap": result.summary_by_gap, "summary_by_group": result.summary_by_group,
        "kw_map": result.all_keywords, "exclusive_keywords": result.exclusive_keywords,
        "trace": result.trace,
    }
    with open(fn, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 Сохранено: {fn}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description="Infix Parser v2.0")
    cli.add_argument("--seed",    default="ремонт пылесосов")
    cli.add_argument("--groups",  default="all")
    cli.add_argument("--country", default="ua")
    cli.add_argument("--lang",    default="ru")
    cli.add_argument("--out",     default=None)
    args = cli.parse_args()
    grp = None if args.groups == "all" else args.groups.split(",")
    asyncio.run(_local_run(args.seed, grp, args.country, args.lang, args.out))
