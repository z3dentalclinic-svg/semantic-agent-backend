"""
Prefix Parser v2.01 — Dual-agent parallel execution (Chrome + Firefox).

Architecture mirrors suffix_parser.py:
    - Shared fetch_suggestions() method (identical to suffix_parser)
    - Parallel execution via asyncio.Semaphore (batch=10)
    - Per-query tracer: hits, empty, timing, unique tracking
    - PrefixParseResult dataclass → JSON-serialisable

LOCAL MODE (запуск без сервера):
    python prefix_parser.py --seed "имплантация зубов" --op "купить"
    → Прогоняет всю матрицу, сохраняет prefix_trace_<seed>_<ts>.json

SERVER INTEGRATION (FastAPI):
    from parser.prefix_parser import PrefixParser, register_prefix_endpoint
    register_prefix_endpoint(app)
    → Добавляет GET /api/prefix-fetch и GET /api/prefix-map

ENDPOINT CONTRACT:
    GET /api/prefix-fetch
        ?seed=   — полный query string (уже содержит оператор и структуру)
        &country=ua &language=ru
        &google_client=chrome
        &cp=     — cursor position (-1 = не передавать)
    → {"results": ["keyword1", "keyword2", ...]}

    GET /api/prefix-map
        ?seed=   — базовый сид (без операторов)
        &operator=купить &groups=G1,G2,PA
        &country=ua &language=ru
    → {"prefix_trace": {...}, "keywords": [...], ...}
"""

import asyncio
import httpx
import time
import random
import json
import re
import logging
import argparse
from typing import Set, List, Dict, Optional
from dataclasses import dataclass, field, asdict


def _brute_seed_variants(seed: str) -> List[str]:
    """
    Генерирует brute-suffix варианты сида для расширения префиксной карты.

    Алгоритм:
      - Находим первое кириллическое слово длиннее 3 букв
      - Отрезаем последний символ (флексия верхнего уровня)
      - Подставляем 8 окончаний: и, ы, а, е, у, ом, ей, о
      - ей = творительный жен.р. (имплантацией), ом = творительный муж.р.

    Пример: "имплантация зубов" → основа "имплантаци" →
      имплантации, имплантациы, имплантациа, имплантацие,
      имплантацию, имплантациом, имплантацией, имплантацио
    """
    ENDINGS = ['и', 'ы', 'а', 'е', 'у', 'ом', 'ей', 'о', 'ю']

    words = seed.lower().strip().split()
    target_idx = None
    for i, w in enumerate(words):
        if re.match(r'^[а-яёА-ЯЁ]{4,}$', w):
            target_idx = i
            break
    if target_idx is None:
        return []

    word = words[target_idx]
    stem = word[:-1]  # отрезаем последний символ
    original_lower = seed.lower().strip()

    variants = []
    for ending in ENDINGS:
        new_word = stem + ending
        if new_word == word:
            continue
        w = words.copy()
        w[target_idx] = new_word
        variant = ' '.join(w)
        if variant != original_lower:
            variants.append(variant)
    return variants

try:
    from parser.prefix_generator import PrefixGenerator, PrefixQuery, ALL_GROUPS
except ImportError:
    from prefix_generator import PrefixGenerator, PrefixQuery, ALL_GROUPS


logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Batch size for parallel execution
BATCH_SIZE = 5
DELAY_SERVER = 0.3
DELAY_LOCAL  = 0.3

# Proxy — три выделенных IP для prefix (как у infix)
import os as _os
try:
    from utils.proxy_pool import ProxyPool
    _proxy_chrome  = ProxyPool.get("prefix_chrome")
    _proxy_firefox = ProxyPool.get("prefix_firefox")
    _proxy_nonpa   = ProxyPool.get("prefix_nonpa")
except ImportError:
    try:
        from proxy_pool import ProxyPool
        _proxy_chrome  = ProxyPool.get("prefix_chrome")
        _proxy_firefox = ProxyPool.get("prefix_firefox")
        _proxy_nonpa   = ProxyPool.get("prefix_nonpa")
    except ImportError:
        _proxy_chrome  = _os.getenv("PREFIX_PROXY_CHROME")  or _os.getenv("GOOGLE_PROXY_URL", "").strip() or None
        _proxy_firefox = _os.getenv("PREFIX_PROXY_FF")      or _os.getenv("GOOGLE_PROXY_URL", "").strip() or None
        _proxy_nonpa   = _os.getenv("PREFIX_PROXY_NONPA")   or _os.getenv("GOOGLE_PROXY_URL", "").strip() or None

DELAY = DELAY_SERVER if (_proxy_chrome or _proxy_firefox) else DELAY_LOCAL


# ══════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════

@dataclass
class PrefixTraceEntry:
    """Trace data for a single prefix query — mirrors SuffixTraceEntry."""
    group: str
    struct: str
    operator: str
    op_type: str
    query_sent: str
    cp: int
    cp_note: str
    results_count: int = 0
    results: List[str] = field(default_factory=list)
    unique: List[str] = field(default_factory=list)  # ключи только в этой структуре
    time_ms: float = 0.0
    status: str = "pending"   # ok, empty, error
    error: Optional[str] = None
    letter: Optional[str] = None
    agent: str = "chrome"           # "chrome" | "firefox"


@dataclass
class PrefixParseResult:
    """Full result of prefix parse — mirrors SuffixParseResult."""
    seed: str
    operator: str
    country: str
    language: str
    groups_used: List[str]
    # Keywords
    all_keywords: Dict[str, List[str]] = field(default_factory=dict)  # kw → [structs]
    # Ключи где сид НЕ найден — семантические замены (купить зубной имплант и т.п.)
    # Используется при интеграции с pipeline: bypass relevance_filter + l0_filter
    alt_seed_keywords: Set[str] = field(default_factory=set)  # kw где seed не в строке
    exclusive_keywords: Dict[str, str] = field(default_factory=dict)  # kw → struct (only 1)
    # Stats
    total_queries: int = 0
    with_results: int = 0
    empty_queries: int = 0
    error_queries: int = 0
    total_keywords: int = 0
    exclusive_count: int = 0
    total_time_ms: float = 0.0
    # Trace
    trace: List[Dict] = field(default_factory=list)
    summary_by_group: Dict = field(default_factory=dict)
    # Meta
    timestamp: str = ""


# ══════════════════════════════════════════════
# ФИЛЬТР МУСОРА
# ══════════════════════════════════════════════

# Предлоги и союзы — одиночные буквы из этого списка НЕ мусор
_PREP_UNION = {"в","во","на","с","со","к","ко","о","у","и","а","б","я"}

def _is_garbage_keyword(kw: str) -> bool:
    """
    Возвращает True если ключ — мусор:
    1. Содержит спецсимволы запроса (* : & | \\)
    2. Содержит одиночную букву которая не предлог/союз
    """
    if re.search(r'[*:|&\\]', kw):
        return True
    for w in kw.lower().split():
        if len(w) == 1 and w not in _PREP_UNION:
            return True
    return False


# ══════════════════════════════════════════════
# PARSER
# ══════════════════════════════════════════════

class PrefixParser:
    """
    Orchestrates prefix generation + autocomplete fetching + tracing.
    Drop-in parallel to SuffixParser.
    """

    def __init__(self, lang: str = "ru"):
        self.generator = PrefixGenerator()
        self.lang = lang

    def _clean_suggestion(self, text: str) -> str:
        """Strip HTML tags from autocomplete suggestions."""
        return re.sub(r'<[^>]+>', '', text).strip()

    async def fetch_suggestions(
        self,
        query: str,
        country: str,
        language: str,
        client: httpx.AsyncClient,
        google_client: str = "firefox",
        cursor_position: Optional[int] = None,
    ) -> List[str]:
        """
        Google Autocomplete fetch — identical to suffix_parser.fetch_suggestions().
        Supports all client types and cp variants.

        cp = -1  → не передаём cp вообще
        cp = None → cp = len(query) (курсор в конце)
        cp = 0+  → явное значение
        """
        url = "https://www.google.com/complete/search"
        params = {
            "q": query,
            # [FIREFOX-ONLY EXPERIMENT] "client": google_client,
            "client": google_client,
            "hl": language,
            "gl": country,
            "ie": "utf-8",
            "oe": "utf-8",
        }

        if cursor_position == -1:
            pass  # не добавляем cp
        elif cursor_position is not None:
            params["cp"] = cursor_position
        else:
            params["cp"] = len(query)

        # [FIREFOX-ONLY EXPERIMENT] headers = {"User-Agent": random.choice(USER_AGENTS)}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"}

        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)

            if response.status_code == 429:
                return []

            if response.status_code == 200:
                text = response.text.strip()

                # Clean JSON (firefox, chrome, chrome-omni)
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

                # Strip security prefix )]}'  (gws-wiz, psy-ab)
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
                                    elif isinstance(item, dict):
                                        s = item.get("suggestion") or item.get("value") or item.get("text", "")
                                        if s:
                                            result.append(self._clean_suggestion(str(s)))
                                return result
                    except Exception:
                        pass

                # JSONP callback
                jsonp_match = re.search(r'\((\[.+\])\)\s*;?\s*$', text, re.DOTALL)
                if jsonp_match:
                    try:
                        data = json.loads(jsonp_match.group(1))
                        if isinstance(data, list) and len(data) > 1:
                            suggestions = data[1]
                            if isinstance(suggestions, list):
                                result = []
                                for item in suggestions:
                                    if isinstance(item, str):
                                        result.append(self._clean_suggestion(item))
                                    elif isinstance(item, list) and len(item) > 0:
                                        result.append(self._clean_suggestion(str(item[0])))
                                    elif isinstance(item, dict):
                                        s = item.get("suggestion") or item.get("value") or item.get("text", "")
                                        if s:
                                            result.append(self._clean_suggestion(str(s)))
                                return result
                    except Exception:
                        pass

        except Exception:
            pass
        return []

    async def parse(
        self,
        seed: str,
        operator: str = "купить",
        country: str = "ua",
        language: str = "ru",
        groups: Optional[List[str]] = None,
        google_client: str = "firefox",  # firefox-only (одиночный вызов)
        progress_callback=None,  # async callable(done, total, entry) for live updates
    ) -> PrefixParseResult:
        """
        Main parse method — letter-parallel execution (архитектура как у infix).

        Три клиента на трёх IP:
            prefix_chrome  — PA Chrome: 30 буквенных треков параллельно,
                             внутри каждой буквы запросы последовательные.
            prefix_nonpa   — G1-G9 + PC Chrome: через asyncio.Semaphore(BATCH_SIZE).
            prefix_firefox — PA FF + G FF: через asyncio.Semaphore(BATCH_SIZE).

        Все три клиента стартуют одновременно через asyncio.gather.
        Целевое время: ~5 секунд при 165 запросах.
        """
        from datetime import datetime
        total_start = time.time()
        timestamp = datetime.utcnow().isoformat() + "Z"

        # Generate matrix
        matrix: List[PrefixQuery] = self.generator.generate(
            seed=seed,
            operator=operator,
            groups=groups or ALL_GROUPS,
        )

        # Shared state
        kw_map: Dict[str, List[str]] = {}
        alt_seed_set: Set[str] = set()
        trace_entries: List[PrefixTraceEntry] = []
        lock = asyncio.Lock()
        done = 0
        total_tasks = sum(len(pq.agents) for pq in matrix)

        async def fetch_one(pq: PrefixQuery, agent: str, client: httpx.AsyncClient):
            nonlocal done
            await asyncio.sleep(DELAY)
            t0 = time.time()
            try:
                results = await self.fetch_suggestions(
                    query=pq.query,
                    country=country,
                    language=language,
                    client=client,
                    google_client=agent,
                    cursor_position=pq.cp,
                )
                elapsed = (time.time() - t0) * 1000
                results = [kw for kw in results if not _is_garbage_keyword(kw)]
                status = "ok" if results else "empty"
                entry = PrefixTraceEntry(
                    group=pq.group, struct=pq.struct,
                    operator=pq.operator, op_type=pq.op_type,
                    query_sent=pq.query, cp=pq.cp, cp_note=pq.cp_note,
                    results=results, results_count=len(results),
                    time_ms=round(elapsed, 1), status=status,
                    letter=pq.letter, agent=agent,
                )
                async with lock:
                    for kw in results:
                        k = kw.lower().strip()
                        if k not in kw_map:
                            kw_map[k] = []
                        kw_map[k].append(f"{agent}:{pq.struct}")
                        if seed.lower() not in k:
                            alt_seed_set.add(k)
            except Exception as e:
                elapsed = (time.time() - t0) * 1000
                entry = PrefixTraceEntry(
                    group=pq.group, struct=pq.struct,
                    operator=pq.operator, op_type=pq.op_type,
                    query_sent=pq.query, cp=pq.cp, cp_note=pq.cp_note,
                    time_ms=round(elapsed, 1), status="error",
                    error=str(e), letter=pq.letter, agent=agent,
                )
            async with lock:
                trace_entries.append(entry)
                done += 1
            if progress_callback:
                await progress_callback(done, total_tasks, entry)

        async def run_pa_letter(letter_queries: List[PrefixQuery],
                                client: httpx.AsyncClient, agent: str):
            """Буквенный трек — запросы одной буквы последовательно (как run_letter в infix)."""
            for pq in letter_queries:
                await fetch_one(pq, agent, client)

        async def run_semaphore(queries: List[PrefixQuery],
                                client: httpx.AsyncClient, agent: str,
                                sem: asyncio.Semaphore):
            """Non-PA запросы через semaphore."""
            async def _one(pq):
                async with sem:
                    await fetch_one(pq, agent, client)
            await asyncio.gather(*[_one(pq) for pq in queries])

        # Разбиваем матрицу на три группы
        from collections import defaultdict
        pa_chr_by_letter: Dict = defaultdict(list)
        pa_ff_by_letter:  Dict = defaultdict(list)
        nonpa_chr: List[PrefixQuery] = []
        nonpa_ff:  List[PrefixQuery] = []

        for pq in matrix:
            is_ff = "firefox" in pq.agents and "chrome" not in pq.agents
            if pq.group == "PA" and pq.letter:
                (pa_ff_by_letter if is_ff else pa_chr_by_letter)[pq.letter].append(pq)
            else:
                (nonpa_ff if is_ff else nonpa_chr).append(pq)

        nonpa_sem = asyncio.Semaphore(BATCH_SIZE)
        ff_sem    = asyncio.Semaphore(BATCH_SIZE)

        async with httpx.AsyncClient(proxy=_proxy_chrome)  as chrome_client, \
                   httpx.AsyncClient(proxy=_proxy_nonpa)   as nonpa_client,  \
                   httpx.AsyncClient(proxy=_proxy_firefox) as ff_client:

            await asyncio.gather(
                # PA Chrome: каждая буква = отдельный параллельный трек
                *[run_pa_letter(qs, chrome_client, "chrome")
                  for qs in pa_chr_by_letter.values()],
                # G+PC Chrome: через semaphore на отдельном IP
                run_semaphore(nonpa_chr, nonpa_client, "chrome", nonpa_sem),
                # FF (PA + G): через semaphore на третьем IP
                *[run_pa_letter(qs, ff_client, "firefox")
                  for qs in pa_ff_by_letter.values()],
                run_semaphore(nonpa_ff, ff_client, "firefox", ff_sem),
            )

        total_time = (time.time() - total_start) * 1000

        # Ротируем батч IP после каждого прогона (как в infix)
        try:
            from utils.proxy_pool import ProxyPool as _PP
            _PP.rotate()
        except ImportError:
            try:
                from proxy_pool import ProxyPool as _PP
                _PP.rotate()
            except ImportError:
                pass

        # Post-process: mark unique per entry
        for entry in trace_entries:
            entry.unique = [
                kw for kw in entry.results
                if len(kw_map.get(kw.lower().strip(), [])) == 1
            ]

        # Summary by group
        summary_by_group = {}
        for g in ALL_GROUPS:
            ge = [e for e in trace_entries if e.group == g]
            if not ge:
                continue
            kws_in_group: set = set()
            for e in ge:
                kws_in_group.update(k.lower().strip() for k in e.results)
            ge_chrome  = [e for e in ge if e.agent == "chrome"]
            ge_firefox = [e for e in ge if e.agent == "firefox"]
            summary_by_group[g] = {
                "total_queries":   len(ge),
                "with_results":    sum(1 for e in ge if e.status == "ok"),
                "empty":           sum(1 for e in ge if e.status == "empty"),
                "errors":          sum(1 for e in ge if e.status == "error"),
                "unique_keywords": len(kws_in_group),
                "exclusive":       sum(len(e.unique) for e in ge),
                "avg_time_ms":     round(sum(e.time_ms for e in ge) / max(len(ge), 1), 1),
                "by_agent": {
                    "chrome":  {"queries": len(ge_chrome),  "hits": sum(1 for e in ge_chrome  if e.status == "ok")},
                    "firefox": {"queries": len(ge_firefox), "hits": sum(1 for e in ge_firefox if e.status == "ok")},
                },
            }

        # Build result
        exclusive_kw = {kw: structs[0] for kw, structs in kw_map.items() if len(structs) == 1}

        return PrefixParseResult(
            seed=seed,
            operator=operator,
            country=country,
            language=language,
            groups_used=list({e.group for e in trace_entries}),
            all_keywords=kw_map,
            alt_seed_keywords=alt_seed_set,
            exclusive_keywords=exclusive_kw,
            total_queries=len(trace_entries),
            with_results=sum(1 for e in trace_entries if e.status == "ok"),
            empty_queries=sum(1 for e in trace_entries if e.status == "empty"),
            error_queries=sum(1 for e in trace_entries if e.status == "error"),
            total_keywords=len(kw_map),
            exclusive_count=len(exclusive_kw),
            total_time_ms=round(total_time, 1),
            trace=[asdict(e) for e in trace_entries],
            summary_by_group=summary_by_group,
            timestamp=timestamp,
        )


# ══════════════════════════════════════════════
# FASTAPI INTEGRATION
# Server-side: добавить в main.py:
#   from parser.prefix_parser import register_prefix_endpoint
#   register_prefix_endpoint(app)
# ══════════════════════════════════════════════

def register_prefix_endpoint(app):
    """
    Register two endpoints on the FastAPI app.

    GET /api/prefix-fetch  — single query fetch (для HTML матричного прогона)
    GET /api/prefix-map    — full matrix run (весь прогон с трейсером)
    """
    try:
        from fastapi import Query as FQuery
    except ImportError:
        raise ImportError("fastapi not installed — for local testing use __main__ below")

    _parser = PrefixParser()

    @app.get("/api/prefix-fetch")
    async def prefix_fetch(
        seed: str = FQuery(..., description="Полный query string (уже с оператором и структурой)"),
        country: str = FQuery("ua"),
        language: str = FQuery("ru"),
        google_client: str = FQuery("firefox"),
        cp: Optional[int] = FQuery(None, description="Cursor position (-1 = не передавать)"),
    ):
        """
        Single Google Autocomplete fetch.
        HTML-страница вызывает этот endpoint для каждой строки матрицы.
        """
        async with httpx.AsyncClient() as client:
            results = await _parser.fetch_suggestions(
                query=seed,
                country=country,
                language=language,
                client=client,
                google_client=google_client,
                cursor_position=cp,
            )
        return {"query": seed, "cp": cp, "results": results}

    @app.get("/api/prefix-map")
    async def prefix_map(
        seed: str = FQuery(..., description="Базовый сид"),
        operator: str = FQuery("купить"),
        groups: str = FQuery("all", description="Группы: all или G1,G2,PA,PC"),
        country: str = FQuery("ua"),
        language: str = FQuery("ru"),
        google_client: str = FQuery("firefox"),
    ):
        """
        Full prefix matrix run — server-side batch.
        Возвращает полный трейсер + агрегированные ключи.
        """
        grp = None if groups == "all" else groups.split(",")
        result = await _parser.parse(
            seed=seed,
            operator=operator,
            country=country,
            language=language,
            groups=grp,
            google_client=google_client,
        )
        return {
            "method": "prefix-map",
            "seed": result.seed,
            "operator": result.operator,
            "total_queries": result.total_queries,
            "total_keywords": result.total_keywords,
            "exclusive_count": result.exclusive_count,
            "time": f"{result.total_time_ms:.0f}ms",
            "prefix_trace": {
                "summary_by_group": result.summary_by_group,
                "all_keywords": result.all_keywords,
                "exclusive_keywords": result.exclusive_keywords,
                "trace": result.trace,
                "total_time_ms": result.total_time_ms,
                "timestamp": result.timestamp,
            },
        }


# ══════════════════════════════════════════════
# LOCAL TEST MODE
# python prefix_parser.py --seed "имплантация зубов" --op "купить" --groups G1,G7,PA
# ══════════════════════════════════════════════

async def _local_run(seed: str, operator: str, groups: Optional[List[str]],
                     country: str, language: str, google_client: str,
                     output_file: Optional[str] = None):
    """Run prefix matrix locally and print results + save JSON."""
    print(f"\n{'='*60}")
    print(f"  Prefix Parser — LOCAL TEST")
    print(f"  Seed:     {seed}")
    print(f"  Operator: {operator}")
    print(f"  Groups:   {groups or 'ALL'}")
    print(f"  Country:  {country} | Lang: {language} | Client: {google_client}")
    print(f"{'='*60}\n")

    parser = PrefixParser()

    # Progress callback
    done_count = [0]
    def make_progress():
        async def cb(done, total, entry):
            done_count[0] = done
            icon = "✅" if entry.status == "ok" else ("○" if entry.status == "empty" else "❌")
            cnt  = f"{entry.results_count} ключей" if entry.status == "ok" else entry.status
            print(f"  [{done:>4}/{total}] {icon} {entry.group:<4} {entry.struct:<30} cp={entry.cp:<5} {cnt}")
        return cb

    result = await parser.parse(
        seed=seed,
        operator=operator,
        country=country,
        language=language,
        groups=groups,
        google_client=google_client,
        progress_callback=make_progress(),
    )

    # Summary
    print(f"\n{'='*60}")
    print(f"  РЕЗУЛЬТАТЫ")
    print(f"{'='*60}")
    print(f"  Запросов:        {result.total_queries}")
    print(f"  С результатами:  {result.with_results}")
    print(f"  Пустых:          {result.empty_queries}")
    print(f"  Ошибок:          {result.error_queries}")
    print(f"  Уникальных кл.:  {result.total_keywords}")
    print(f"  Эксклюзивных:    {result.exclusive_count} (только 1 структура)")
    print(f"  Время:           {result.total_time_ms:.0f}ms")
    print(f"\n  По группам:")
    for g, s in result.summary_by_group.items():
        pct = round(s['with_results']/max(s['total_queries'],1)*100)
        print(f"    {g:<6} {s['with_results']:>3}/{s['total_queries']:<4} ({pct:>3}%) "
              f"| {s['unique_keywords']:>4} кл. | {s['exclusive']:>3} эксклюз.")

    if result.exclusive_keywords:
        print(f"\n  Топ-30 эксклюзивных ключей:")
        for kw, struct in list(result.exclusive_keywords.items())[:30]:
            print(f"    [{struct}] {kw}")

    # Save JSON
    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fn = output_file or f"prefix_trace_{seed.replace(' ','_')}_{ts}.json"

    export = {
        "meta": {
            "seed": result.seed, "operator": result.operator,
            "country": result.country, "language": result.language,
            "groups": result.groups_used, "timestamp": result.timestamp,
            "total_time_ms": result.total_time_ms,
        },
        "summary": {
            "total_queries": result.total_queries,
            "with_results":  result.with_results,
            "empty":         result.empty_queries,
            "errors":        result.error_queries,
            "total_keywords": result.total_keywords,
            "exclusive_count": result.exclusive_count,
        },
        "summary_by_group": result.summary_by_group,
        "kw_map":            result.all_keywords,
        "exclusive_keywords": result.exclusive_keywords,
        "trace": result.trace,
    }

    with open(fn, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    print(f"\n  💾 Сохранено: {fn}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser_cli = argparse.ArgumentParser(description="Prefix Parser — local test")
    parser_cli.add_argument("--seed",    default="имплантация зубов", help="Base seed")
    parser_cli.add_argument("--op",      default="купить",            help="Operator")
    parser_cli.add_argument("--groups",  default="all",               help="Groups: all or G1,G2,PA")
    parser_cli.add_argument("--country", default="ua",                help="Country code")
    parser_cli.add_argument("--lang",    default="ru",                help="Language code")
    parser_cli.add_argument("--client",  default="firefox",            help="Google client")
    parser_cli.add_argument("--out",     default=None,                help="Output JSON file")
    args = parser_cli.parse_args()

    grp = None if args.groups == "all" else args.groups.split(",")

    asyncio.run(_local_run(
        seed=args.seed,
        operator=args.op,
        groups=grp,
        country=args.country,
        language=args.lang,
        google_client=args.client,
        output_file=args.out,
    ))
