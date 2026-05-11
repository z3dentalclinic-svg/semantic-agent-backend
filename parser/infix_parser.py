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

try:
    from parser.infix_generator import InfixGenerator, InfixQuery, ALL_GROUPS
except ImportError:
    from infix_generator import InfixGenerator, InfixQuery, ALL_GROUPS

logger = logging.getLogger(__name__)

UA_CHROME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
UA_FIREFOX = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
UA_SAFARI = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"

DELAY_LOCAL  = 0.3
DELAY_SERVER = 0.15   # Снижен с 0.3 до 0.15 — 5 IP × Semaphore(5) = 25 concurrent
BATCH_SIZE   = 5      # Семафор на каждый IP (как в суффиксе)

# 5 IP: по одному из каждого батча (index 0 каждого батча × 5 батчей = 5 разных IP)
# ProxyPool.get("infix_chrome") всегда возвращает index 0 ТЕКУЩЕГО батча — один и тот же IP.
# Правильно: взять get_all_proxies() и взять index 0 из каждого батча.
def _get_5_infix_proxies(PP) -> list:
    all_p = PP.get_all_proxies()
    ips = [all_p[i * 10] for i in range(min(5, len(all_p) // 10))]
    if not ips:
        return [None] * 5
    while len(ips) < 5:
        ips.append(ips[-1])
    return ips

try:
    from utils.proxy_pool import ProxyPool
    _proxies_infix = _get_5_infix_proxies(ProxyPool)
except ImportError:
    try:
        from proxy_pool import ProxyPool
        _proxies_infix = _get_5_infix_proxies(ProxyPool)
    except ImportError:
        _fb = os.getenv("INFIX_PROXY_CHROME") or os.getenv("GOOGLE_PROXY_URL") or None
        _proxies_infix = [_fb] * 5

_proxy_chrome  = _proxies_infix[0]
_proxy_firefox = _proxies_infix[1]
_proxy_safari  = os.getenv("INFIX_PROXY_SAFARI") or os.getenv("GOOGLE_PROXY_URL") or None

_google_proxy = _proxy_chrome  # для обратной совместимости
DELAY = DELAY_SERVER if (_proxy_chrome or _proxy_firefox or _proxy_safari) else DELAY_LOCAL

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
    total_keywords: int = 0
    exclusive_count: int = 0
    total_time_ms: float = 0.0
    trace: List[Dict] = field(default_factory=list)
    summary_by_gap: Dict = field(default_factory=dict)
    summary_by_group: Dict = field(default_factory=dict)
    timestamp: str = ""


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

        if google_client == "firefox":
            ua = UA_FIREFOX
        elif google_client == "safari":
            ua = UA_SAFARI
        else:
            ua = UA_CHROME
        headers = {"User-Agent": ua}
        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)
            if response.status_code == 429:
                return []
            if response.status_code == 200:
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
                            return result
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
                                return result
                    except Exception:
                        pass
        except Exception:
            pass
        return []

    async def parse(self, seed, country="ua", language="ru",
                    groups=None, progress_callback=None,
                    city=None) -> InfixParseResult:
        from datetime import datetime
        total_start = time.time()
        timestamp = datetime.utcnow().isoformat() + "Z"

        # uule: city=None → столица страны, city="Lviv" → конкретный город
        _uule = get_uule(country, city)

        matrix: List[InfixQuery] = self.generator.generate(seed=seed, groups=groups or ALL_GROUPS)

        kw_map: Dict[str, List[str]] = {}
        alt_seed_set: Set[str] = set()
        trace_entries: List[InfixTraceEntry] = []
        lock = asyncio.Lock()
        done_count = [0]

        non_e_sem = asyncio.Semaphore(BATCH_SIZE)  # fallback если один клиент

        # 5-IP семафоры: 3 Chrome + 2 Firefox
        chr_sems = [asyncio.Semaphore(BATCH_SIZE) for _ in range(3)]
        ff_sems  = [asyncio.Semaphore(BATCH_SIZE) for _ in range(2)]

        # Для финального лога
        _gather_elapsed = 0.0
        unique_proxies = len(set(p for p in _proxies_infix if p))

        async def fetch_one(iq: InfixQuery, client: httpx.AsyncClient):
            agent = iq.agents[0]
            await asyncio.sleep(DELAY)
            t0 = time.time()
            # Addon-запросы получают __agent суффикс в struct чтобы отличаться от боевых
            struct_label = f"{iq.struct}__{agent}" if getattr(iq, 'is_new_research', False) else iq.struct
            try:
                raw_results = await self.fetch_suggestions(
                    query=iq.query, country=country, language=language,
                    client=client, google_client=agent, cursor_position=iq.cp,
                    uule=_uule,
                )
                elapsed = (time.time() - t0) * 1000

                # Фильтр мусора
                results = [kw for kw in raw_results if not _is_garbage_keyword(kw)]

                status = "ok" if results else "empty"
                entry = InfixTraceEntry(
                    gap_index=iq.gap_index, w1=iq.w1, w2=iq.w2,
                    group=iq.group, struct=struct_label,
                    insert_val=iq.insert_val, insert_type=iq.insert_type,
                    orientation=iq.orientation,
                    query_sent=iq.query, cp=iq.cp, cp_note=iq.cp_note,
                    agent=agent, results=results, results_count=len(results),
                    time_ms=round(elapsed, 1), status=status, letter=iq.letter,
                )
                async with lock:
                    for kw in results:
                        k = kw.lower().strip()
                        if not k:
                            continue
                        if k not in kw_map:
                            kw_map[k] = []
                        kw_map[k].append(f"{agent}:{struct_label}")
                        if seed.lower() not in k:
                            alt_seed_set.add(k)

            except Exception as e:
                elapsed = (time.time() - t0) * 1000
                entry = InfixTraceEntry(
                    gap_index=iq.gap_index, w1=iq.w1, w2=iq.w2,
                    group=iq.group, struct=struct_label,
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

        async def run_letter(letter_queries, client):
            for iq in letter_queries:
                await fetch_one(iq, client)

        async def run_non_e(iq, client):
            async with non_e_sem:
                await fetch_one(iq, client)

        # ═══════════════════════════════════════════════════════════════════
        # RESEARCH POOL — для is_new_research=True запросов
        # 1-в-1 архитектура suffix-research: пул httpx-клиентов на все IP
        # из get_research_pool, семафор max(5, len/2), 3 агента одного запроса
        # идут параллельно через тот же IP (3 отдельных таска).
        # ═══════════════════════════════════════════════════════════════════
        research_clients: List[httpx.AsyncClient] = []
        research_queries = [iq for iq in matrix if iq.is_new_research]
        if research_queries:
            # combined_parser.html запускает suffix+prefix+infix параллельно.
            # Под research отдаём всё что НЕ занято в активном батче.
            _research_excludes = {
                "infix_chrome", "infix_firefox", "infix_safari",
                "prefix_chrome", "prefix_firefox", "prefix_nonpa",
                "suffix",
            }
            try:
                from utils.proxy_pool import ProxyPool as _PP
                _research_proxies = _PP.get_research_pool(exclude_roles=_research_excludes)
            except ImportError:
                try:
                    from proxy_pool import ProxyPool as _PP
                    _research_proxies = _PP.get_research_pool(exclude_roles=_research_excludes)
                except ImportError:
                    _research_proxies = []
            except Exception:
                _research_proxies = []

            if _research_proxies:
                for proxy_url in _research_proxies:
                    research_clients.append(httpx.AsyncClient(proxy=proxy_url))
                logger.info(
                    f"[Infix Research] Pool: {len(research_clients)} httpx-клиентов "
                    f"для {len(research_queries)} запросов × агенты "
                    f"= {sum(len(iq.agents) for iq in research_queries)} реальных запросов "
                    f"(~{sum(len(iq.agents) for iq in research_queries) // max(len(research_clients), 1)} запросов на IP)"
                )

        # Семафор для research — точно как в суффиксе
        research_sem = asyncio.Semaphore(max(5, len(research_clients) // 2)) if research_clients else None

        async def run_research_one(iq: InfixQuery, agent: str, idx: int):
            """
            Один research-запрос на одном из IP пула (round-robin по индексу запроса).
            agent — chrome/firefox/safari. Зеркало suffix-research run_research_one.
            """
            if not research_clients:
                return
            client = research_clients[idx % len(research_clients)]
            async with research_sem:
                t0 = time.time()
                try:
                    raw_results = await self.fetch_suggestions(
                        query=iq.query, country=country, language=language,
                        client=client, google_client=agent, cursor_position=iq.cp,
                        uule=_uule,
                    )
                    elapsed = (time.time() - t0) * 1000
                    results = [kw for kw in raw_results if not _is_garbage_keyword(kw)]
                    status = "ok" if results else "empty"
                    struct_label = f"{iq.struct}__{agent}"
                    entry = InfixTraceEntry(
                        gap_index=iq.gap_index, w1=iq.w1, w2=iq.w2,
                        group=iq.group, struct=struct_label,
                        insert_val=iq.insert_val, insert_type=iq.insert_type,
                        orientation=iq.orientation,
                        query_sent=iq.query, cp=iq.cp, cp_note=iq.cp_note,
                        agent=agent, results=results, results_count=len(results),
                        time_ms=round(elapsed, 1), status=status, letter=iq.letter,
                    )
                    async with lock:
                        for kw in results:
                            k = kw.lower().strip()
                            if not k:
                                continue
                            if k not in kw_map:
                                kw_map[k] = []
                            kw_map[k].append(f"{agent}:{struct_label}")
                            if seed.lower() not in k:
                                alt_seed_set.add(k)
                except Exception as e:
                    elapsed = (time.time() - t0) * 1000
                    struct_label = f"{iq.struct}__{agent}"
                    entry = InfixTraceEntry(
                        gap_index=iq.gap_index, w1=iq.w1, w2=iq.w2,
                        group=iq.group, struct=struct_label,
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

        async def run_research_all():
            """Запускает все research-запросы × агенты параллельно. Зеркало suffix."""
            if not research_clients:
                return
            tasks = []
            for i, iq in enumerate(research_queries):
                for agent in iq.agents:
                    tasks.append(run_research_one(iq, agent, i))
            await asyncio.gather(*tasks)

        # 5 IP: 3 Chrome + 2 Firefox (как в суффиксе)
        # 3×Semaphore(5)=15 concurrent Chrome, 2×Semaphore(5)=10 concurrent FF = 25 total
        async with \
            httpx.AsyncClient(proxy=_proxies_infix[0]) as chr_c1, \
            httpx.AsyncClient(proxy=_proxies_infix[1]) as chr_c2, \
            httpx.AsyncClient(proxy=_proxies_infix[2]) as chr_c3, \
            httpx.AsyncClient(proxy=_proxies_infix[3]) as ff_c1,  \
            httpx.AsyncClient(proxy=_proxies_infix[4]) as ff_c2,  \
            httpx.AsyncClient(proxy=_proxy_safari)     as safari_client:

            chr_clients = [chr_c1, chr_c2, chr_c3]
            ff_clients  = [ff_c1, ff_c2]

            from collections import defaultdict
            e_by_letter_chr = defaultdict(list)
            e_by_letter_ff  = defaultdict(list)
            non_e_chr = []
            non_e_ff  = []
            for iq in matrix:
                if iq.is_new_research:
                    continue
                is_ff = "firefox" in iq.agents
                if iq.group == "E" and iq.letter:
                    (e_by_letter_ff if is_ff else e_by_letter_chr)[iq.letter].append(iq)
                else:
                    (non_e_ff if is_ff else non_e_chr).append(iq)

            # Addon-запросы (is_new_research=True) через те же клиенты
            addon_chr = [iq for iq in matrix if iq.is_new_research and "firefox" not in iq.agents]
            addon_ff  = [iq for iq in matrix if iq.is_new_research and "firefox" in iq.agents]

            async def run_non_e(iq, client):
                """Боевые non-E запросы — оригинальный путь через non_e_sem (как в v1.0)."""
                async with non_e_sem:
                    await fetch_one(iq, client)

            async def run_chr(iq, idx):
                """Addon-запросы chrome — round-robin по 3 IP."""
                slot = idx % 3
                async with chr_sems[slot]:
                    await fetch_one(iq, chr_clients[slot])

            async def run_ff(iq, idx):
                """Addon-запросы firefox — round-robin по 2 IP."""
                slot = idx % 2
                async with ff_sems[slot]:
                    await fetch_one(iq, ff_clients[slot])

            try:
                t_gather_start = time.time()
                unique_proxies = len(set(p for p in _proxies_infix if p))
                await asyncio.gather(
                    # Боевые — оригинальный путь (даёт 4с как раньше)
                    *[run_letter(qs, chr_c1)    for qs in e_by_letter_chr.values()],
                    *[run_letter(qs, ff_c1)     for qs in e_by_letter_ff.values()],
                    *[run_non_e(iq, chr_c1)     for iq in non_e_chr],
                    *[run_non_e(iq, ff_c1)      for iq in non_e_ff],
                    # Addon — round-robin по 5 IP
                    *[run_chr(iq, i)            for i, iq in enumerate(addon_chr)],
                    *[run_ff(iq, i)             for i, iq in enumerate(addon_ff)],
                    run_research_all(),
                )
                pass  # итоговый лог — ниже после сборки результатов
                _gather_elapsed = time.time() - t_gather_start
            finally:
                for c in research_clients:
                    try:
                        await c.aclose()
                    except Exception:
                        pass

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

        # Сборка trace без asdict — он рекурсивно копирует все поля и на 80k+
        # entries даёт 400-600 MB пик памяти (OOM на Render 2GB).
        # Для research-entries (которых обычно >95% от всего trace) results и unique
        # уже учтены в kw_map / exclusive_kw, поэтому НЕ дублируем их в trace.
        trace_list = []
        for e in trace_entries:
            d = {
                "gap_index": e.gap_index, "w1": e.w1, "w2": e.w2,
                "group": e.group, "struct": e.struct,
                "insert_val": e.insert_val, "insert_type": e.insert_type,
                "orientation": e.orientation,
                "query_sent": e.query_sent, "cp": e.cp, "cp_note": e.cp_note,
                "agent": e.agent,
                "results_count": e.results_count,
                "time_ms": e.time_ms, "status": e.status,
                "letter": e.letter,
            }
            if e.error:
                d["error"] = e.error
            # results/unique только для боевой матрицы (struct без __agent суффикса)
            # research-entries имеют struct вида "0_а_rev_plain_cp5__chrome"
            if "__" not in e.struct:
                d["results"] = e.results
                d["unique"] = e.unique
            trace_list.append(d)

        # Освобождаем тяжёлые объекты до return — GC соберёт raw dataclasses,
        # results/unique строки уже скопированы по ссылке (shared, без дубликата)
        trace_entries.clear()
        import gc
        gc.collect()

        print(
            f"[Infix] DONE | seed='{seed}' | "
            f"total={round(total_time/1000, 2)}s "
            f"gather={round(_gather_elapsed, 2)}s | "
            f"queries={len(trace_list)} ok={sum(1 for e in trace_list if e['status']=='ok')} "
            f"empty={sum(1 for e in trace_list if e['status']=='empty')} "
            f"err={sum(1 for e in trace_list if e['status']=='error')} | "
            f"keywords={len(kw_map)} | "
            f"unique_ips={unique_proxies}/5 | "
            f"proxies={[p.split('@')[-1][:20] if p and '@' in p else (p[:20] if p else None) for p in _proxies_infix]}"
        )

        return InfixParseResult(
            seed=seed, country=country, language=language,
            groups_used=list(set(e["group"] for e in trace_list)),
            all_keywords=kw_map, alt_seed_keywords=alt_seed_set,
            exclusive_keywords=exclusive_kw,
            total_queries=len(trace_list),  # реальное число fetch'ей (3 агента × research = ×3)
            with_results=sum(1 for e in trace_list if e["status"] == "ok"),
            empty_queries=sum(1 for e in trace_list if e["status"] == "empty"),
            error_queries=sum(1 for e in trace_list if e["status"] == "error"),
            total_keywords=len(kw_map), exclusive_count=len(exclusive_kw),
            total_time_ms=round(total_time, 1),
            trace=trace_list,
            summary_by_gap=summary_by_gap, summary_by_group=summary_by_group,
            timestamp=timestamp,
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
