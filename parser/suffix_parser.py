"""
Suffix Parser v1.0 — Sends generated suffix queries to Google Autocomplete.

Features:
- Parallel execution via asyncio.Semaphore  
- AdaptiveDelay from existing codebase
- Per-suffix tracer: tracks hits, empty, timing
- Aggregated stats for suffix map optimization
"""

import asyncio
import httpx
import time
import random
import json
import re
import logging
import os
from typing import List, Dict, Optional
from dataclasses import dataclass, field, asdict

from parser.suffix_generator import SuffixGenerator, SuffixQuery, SeedAnalysis


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


class AdaptiveDelay:
    """Copied from main.py — auto-tuning delay between requests"""

    def __init__(self, initial_delay: float = 0.2, min_delay: float = 0.1, max_delay: float = 1.0):
        self.delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay

    def get_delay(self) -> float:
        return self.delay

    def on_success(self):
        self.delay = max(self.min_delay, self.delay * 0.95)

    def on_rate_limit(self):
        self.delay = min(self.max_delay, self.delay * 1.5)


@dataclass
class SuffixTraceEntry:
    """Trace data for a single suffix query"""
    suffix_val: str
    suffix_label: str
    suffix_type: str  # A, B, C, D, E
    priority: int
    query_sent: str
    cp_override: Optional[int] = None   # ← actual cp sent to Google
    results_count: int = 0
    results: List[str] = field(default_factory=list)
    time_ms: float = 0.0
    status: str = "pending"  # ok, empty, error, blocked, rate_limit


@dataclass 
class SuffixParseResult:
    """Full result of suffix parsing"""
    seed: str
    analysis: Dict
    all_keywords: List[Dict] = field(default_factory=list)  # [{keyword, sources, weight, is_suffix_expanded}]
    total_queries: int = 0
    successful_queries: int = 0
    empty_queries: int = 0
    error_queries: int = 0
    blocked_queries: int = 0
    total_time_ms: float = 0.0
    trace: List[Dict] = field(default_factory=list)
    summary_by_type: Dict = field(default_factory=dict)
    summary_by_suffix: Dict = field(default_factory=dict)


class SuffixParser:
    """
    Orchestrates suffix generation + autocomplete fetching + tracing.
    """

    def __init__(self, lang: str = "ru", geo_db: dict = None):
        self.generator = SuffixGenerator(lang=lang)
        self.adaptive_delay = AdaptiveDelay()
        self.geo_db: dict = geo_db or {}

    def get_fingerprint(self, text: str) -> str:
        """
        Normalize phrase to intent fingerprint for deduplication.
        "купить аккумулятор на скутер" == "аккумулятор скутер купить" → same fingerprint.
        """
        text = text.lower()
        text = re.sub(r'[^а-яёa-z0-9\s]', '', text)
        words = text.split()
        # Убираем короткие предлоги (кроме значимых)
        words = [w for w in words if len(w) > 2 or w in {"до", "из"}]
        # Грубый стемминг для схлопывания падежей
        stemmed = []
        for w in words:
            if len(w) > 6:
                stemmed.append(w[:-3])
            elif len(w) > 4:
                stemmed.append(w[:-2])
            else:
                stemmed.append(w)
        return "|".join(sorted(set(stemmed)))

    def _clean_suggestion(self, text: str) -> str:
        """Strip HTML tags (<b>, </b> etc) from autocomplete suggestions."""
        return re.sub(r'<[^>]+>', '', text).strip()

    async def fetch_suggestions(self, query: str, country: str, language: str,
                                 client: httpx.AsyncClient, google_client: str = "chrome",
                                 cursor_position: int = None) -> List[str]:
        """Google Autocomplete with multi-client support."""
        url = "https://www.google.com/complete/search"
        params = {
            "q": query,
            "client": google_client,
            "hl": language,
            "gl": country,
            "ie": "utf-8",
            "oe": "utf-8",
        }
        # cp = cursor position
        # None → auto: cp=len(query) — tells Google "cursor is at the end"
        # -1 → don't send cp at all (old behavior)
        # 0+ → explicit value (e.g. 0 = cursor at start for prefix discovery)
        if cursor_position is not None and cursor_position == -1:
            pass  # don't add cp to params
        elif cursor_position is not None:
            params["cp"] = cursor_position
        else:
            params["cp"] = len(query)
        headers = {"User-Agent": random.choice(USER_AGENTS)}

        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)

            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []

            self.adaptive_delay.on_success()

            if response.status_code == 200:
                text = response.text.strip()
                
                # Try clean JSON first (firefox, chrome, chrome-omni)
                try:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 1:
                        raw = data[1]
                        if isinstance(raw, list):
                            # Handles all known formats:
                            # firefox:  ["s1", "s2"]
                            # chrome:   [["s1",0,[512]], ["s2",0,[512]]]  
                            # chrome cp=0: [{"suggestion":"s1","relevance":...}, ...]
                            result = []
                            for item in raw:
                                if isinstance(item, str):
                                    result.append(self._clean_suggestion(item))
                                elif isinstance(item, list) and len(item) > 0 and isinstance(item[0], str):
                                    result.append(self._clean_suggestion(item[0]))
                                elif isinstance(item, dict):
                                    # Chrome with cp=0 may return dicts
                                    s = item.get("suggestion") or item.get("value") or item.get("text", "")
                                    if s:
                                        result.append(self._clean_suggestion(str(s)))
                            return result
                        return []
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
                
                # Strip JSONP callback: window.google.ac.h(...) or callback(...)
                jsonp_match = re.search(r'\((\[.+\])\)\s*;?\s*$', text, re.DOTALL)
                if jsonp_match:
                    try:
                        data = json.loads(jsonp_match.group(1))
                        if isinstance(data, list) and len(data) > 1:
                            # psy-ab/gws-wiz sometimes nests suggestions differently
                            suggestions = data[1]
                            if isinstance(suggestions, list):
                                # Could be list of strings or list of [string, ...]
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

    async def fetch_suggestions_yandex(self, query: str, language: str,
                                        client: httpx.AsyncClient) -> List[str]:
        """Yandex Suggest — suggest-ff.cgi endpoint."""
        url = "https://suggest.yandex.ru/suggest-ff.cgi"
        params = {
            "part": query,
            "uil": language,
            "srv": "suggest_smart",
            "yu": "1",
        }
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)
            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []
            self.adaptive_delay.on_success()
            if response.status_code == 200:
                data = response.json()
                # Яндекс возвращает [query, [suggestion1, suggestion2, ...]]
                if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                    return [str(s) for s in data[1] if s]
        except Exception:
            pass
        return []

    async def fetch_suggestions_bing(self, query: str, language: str, country: str,
                                      client: httpx.AsyncClient) -> List[str]:
        """Bing Autosuggest — AS/Suggestions endpoint."""
        url = "https://www.bing.com/AS/Suggestions"
        params = {
            "q": query,
            "mkt": f"{language}-{country.upper()}",
            "cvid": "0",
            "qry": query,
        }
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)
            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []
            self.adaptive_delay.on_success()
            if response.status_code == 200:
                data = response.json()
                results = []
                for group in data.get("AS", {}).get("Results", []):
                    for item in group.get("Suggests", []):
                        text = item.get("Txt", "")
                        if text:
                            results.append(text)
                return results
        except Exception:
            pass
        return []

    async def parse(self, seed: str, country: str = "ua", language: str = "ru",
                    parallel_limit: int = 5, include_numbers: bool = False,
                    echelon: int = 0, google_client: str = "chrome",
                    cursor_position: int = None,
                    include_letters: bool = False) -> SuffixParseResult:
        """
        Main parse method.

        Architecture:
        - A/B/C/D queries: run concurrently with semaphore
        - E queries: grouped by letter; all letters run in parallel,
          each letter's 14 queries are sequential (avoids Google rate limiting)
        """
        total_start = time.time()

        # Determine region from country
        if country in ("ua", "by", "kz"):
            region = "ua"
        elif country in ("ru",):
            region = "ru"
        else:
            region = "ua"  # default to UA for non-RU markets

        # Step 1: Generate queries
        analysis, all_queries = self.generator.generate(
            seed,
            include_numbers=include_numbers,
            include_letters=include_letters,
            region=region,
        )
        analysis_summary = self.generator.summary(analysis, all_queries)

        # Step 2: Filter by echelon
        if echelon == 1:
            queries_to_send = [q for q in all_queries if q.priority == 1]
        elif echelon == 2:
            queries_to_send = [q for q in all_queries if q.priority == 2]
        else:
            queries_to_send = [q for q in all_queries if q.priority > 0]

        blocked_queries = [q for q in all_queries if q.priority == 0]

        # Step 3: Separate E from other types
        e_queries_by_letter: Dict[str, List] = {}
        other_queries = []
        for q in queries_to_send:
            if q.suffix_type == "E":
                letter = q.suffix_val
                if letter not in e_queries_by_letter:
                    e_queries_by_letter[letter] = []
                e_queries_by_letter[letter].append(q)
            else:
                other_queries.append(q)

        # Step 4: Shared state (thread-safe via asyncio — single event loop)
        # all_keywords: keyword → {sources, weight, is_suffix_expanded}
        all_keywords: Dict[str, Dict] = {}
        trace_entries: List[SuffixTraceEntry] = []

        # Add blocked to trace
        for bq in blocked_queries:
            trace_entries.append(SuffixTraceEntry(
                suffix_val=bq.suffix_val,
                suffix_label=bq.suffix_label,
                suffix_type=bq.suffix_type,
                priority=0,
                query_sent=bq.query,
                status=f"blocked:{bq.blocked_by}",
            ))

        # Допустимые одиночные буквы (предлоги/союзы)
        _ALLOWED_SINGLE_LETTERS = {"в", "с", "к", "у", "о", "а", "и", "б"}
        _BAD_SYMBOLS = set("-*|~%\\!:^@#$&+=[]{}()<>/?'\"")
        _seed_words = set(seed.lower().split())

        def _is_garbage_keyword(kw: str) -> bool:
            """Возвращает True если ключ содержит мусорные токены."""
            words = kw.split()
            for word in words:
                # слово целиком из спецсимволов (*, **, ***, -- и т.д.)
                if word and all(c in _BAD_SYMBOLS for c in word):
                    return True
                # одиночная буква не из допустимых
                if len(word) == 1 and word.isalpha() and word.lower() not in _ALLOWED_SINGLE_LETTERS:
                    return True
            # После слов сида должно быть хотя бы одно слово ≥4 букв
            tail = [w for w in words if w.lower() not in _seed_words]
            if tail and not any(len(w) >= 4 and w.isalpha() for w in tail):
                return True
            return False

        def _record_results(sq: "SuffixQuery", results: List[str], elapsed_ms: float):
            """Update shared state after fetch. Called from both code paths."""
            entry = SuffixTraceEntry(
                suffix_val=sq.suffix_val,
                suffix_label=sq.suffix_label,
                suffix_type=sq.suffix_type,
                priority=sq.priority,
                query_sent=sq.query,
                cp_override=sq.cp_override,
                results_count=len(results),
                results=results,
                time_ms=round(elapsed_ms, 1),
                status="ok" if results else "empty",
            )
            trace_entries.append(entry)

            source_info = {
                "suffix_type": sq.suffix_type,
                "suffix_val": sq.suffix_val,
                "suffix_label": sq.suffix_label,
                "priority": sq.priority,
            }
            for kw in results:
                if _is_garbage_keyword(kw):
                    continue
                if kw not in all_keywords:
                    all_keywords[kw] = {
                        "sources": [],
                        "weight": 0,
                        "is_suffix_expanded": True,
                    }
                all_keywords[kw]["sources"].append(source_info)
                all_keywords[kw]["weight"] += 1

        # Step 5a: Other queries (A/B/C/D) — concurrent with semaphore
        semaphore = asyncio.Semaphore(parallel_limit)

        async def fetch_one_tracked(sq, client: httpx.AsyncClient, force_client: str = None):
            """Fetch single query and record results."""
            if sq.cp_override is not None:
                cp = sq.cp_override
            elif cursor_position == -1:
                cp = -1
            else:
                cp = None

            gc = force_client or google_client
            t0 = time.time()
            results = await self.fetch_suggestions(sq.query, country, language, client, gc, cp)
            elapsed = (time.time() - t0) * 1000
            _record_results(sq, results, elapsed)

        # async def fetch_one_firefox(sq, client: httpx.AsyncClient):
        #     """Firefox pass for E — same query, firefox agent, cp not sent."""
        #     # ОТКЛЮЧЕНО для лайт-серча (только Chrome)
        #     from dataclasses import replace as dc_replace
        #     sq_ff = dc_replace(
        #         sq,
        #         suffix_label=sq.suffix_label + "_ff",
        #         suffix_type="E_ff",
        #         cp_override=-1,
        #     )
        #     t0 = time.time()
        #     results = await self.fetch_suggestions(sq.query, country, language, client, "firefox", -1)
        #     elapsed = (time.time() - t0) * 1000
        #     _record_results(sq_ff, results, elapsed)

        # Step 5d: E_simple — алфавитный перебор через Chrome без cp
        # Отсортирован по частотности: высокочастотные первыми — Novelty Threshold
        # срабатывает на хвосте (щ/э/ю/я), а не в середине.
        ALPHABET = list("ксрпмнадтбвгзеиёжлоуфхцчшйщэюя")

        async def run_e_simple(client: httpx.AsyncClient):
            e_simple_sem = asyncio.Semaphore(5)

            async def fetch_simple_char(char: str):
                async with e_simple_sem:
                    await asyncio.sleep(0.3)
                    q = f"{seed} {char}"
                    sq_simple = SuffixQuery(
                        query=q, suffix_val=char, suffix_label=f"simple_{char}",
                        suffix_type="E_simple", priority=1, markers=["e_simple"], cp_override=-1,
                    )
                    t0 = time.time()
                    results = await self.fetch_suggestions(q, country, language, client, "chrome", -1)
                    elapsed = (time.time() - t0) * 1000
                    _record_results(sq_simple, results, elapsed)

            await asyncio.gather(*[fetch_simple_char(c) for c in ALPHABET])

        async def fetch_with_semaphore(sq, client):
            async with semaphore:
                await asyncio.sleep(0.3)
                await fetch_one_tracked(sq, client)

        # Структуры с 97-100% дублями на 5 сидах — убраны из Chrome E (6 шт)
        # hyp_wcL нестабилен в E_ff (66-84% уникальных) — оставляем в Firefox
        CHROME_E_SKIP = set()  # ТЕСТ: все 13 структур включены, определяем что даёт уникальные
        FF_E_SKIP     = {"L_col", "L_hyp", "hyp_Lwc", "sandwich", "plain"}

        # Novelty Threshold — параметры
        # E_simple per-letter threshold:
        # если E_simple для буквы вернул < N результатов — E_chrome для неё бесполезен
        E_SIMPLE_MIN_RESULTS = 3

        # Fingerprint novelty threshold для батчей:
        # если батч из 5 букв принёс < N новых интентов — дальше не идём
        E_NOVELTY_BATCH = 5
        E_NOVELTY_MIN_NEW = 3

        # Семафор для E chrome — не более 5 одновременных запросов
        # (предотвращает пачки из 70 запросов за 0.5 сек)
        e_chrome_sem = asyncio.Semaphore(5)

        async def fetch_e_chrome_one(sq: "SuffixQuery", client: httpx.AsyncClient):
            """Один E chrome запрос через семафор с джиттером."""
            if sq.variant in CHROME_E_SKIP:
                return
            async with e_chrome_sem:
                await asyncio.sleep(random.uniform(0.3, 0.5))
                await fetch_one_tracked(sq, client)

        # Step 5b2: E chrome с per-letter skip + fingerprint novelty threshold
        async def run_e_chrome_with_novelty(client: httpx.AsyncClient):
            # Строим карту: буква → кол-во результатов E_simple
            e_simple_counts: Dict[str, int] = {}
            for t in trace_entries:
                if t.suffix_type == "E_simple" and t.suffix_val:
                    e_simple_counts[t.suffix_val] = t.results_count

            letters = list(e_queries_by_letter.keys())
            skipped_simple = []
            active_letters = []

            # Per-letter skip: убираем буквы где E_simple дал < 3 результатов
            for L in letters:
                if e_simple_counts.get(L, 0) < E_SIMPLE_MIN_RESULTS:
                    skipped_simple.append(L)
                else:
                    active_letters.append(L)

            if skipped_simple:
                import logging
                logging.getLogger(__name__).info(
                    f"[PerLetterSkip] seed={seed!r} "
                    f"skipped {len(skipped_simple)}/{len(letters)} letters "
                    f"(E_simple < {E_SIMPLE_MIN_RESULTS}): {skipped_simple}"
                )

            # Fingerprint novelty: батчи по 5 букв
            # Каждый батч — до 70 запросов через семафор Semaphore(3)
            # Запросы идут плавно, novelty check видит реальные результаты
            def current_fps():
                return {self.get_fingerprint(kw) for kw in all_keywords.keys()}

            for batch_start in range(0, len(active_letters), E_NOVELTY_BATCH):
                batch = active_letters[batch_start:batch_start + E_NOVELTY_BATCH]

                fps_before = current_fps()

                # Собираем все запросы батча и запускаем через семафор
                tasks = []
                for L in batch:
                    for sq in e_queries_by_letter.get(L, []):
                        tasks.append(fetch_e_chrome_one(sq, client))
                await asyncio.gather(*tasks)

                new_intents = len(current_fps() - fps_before)

                import logging
                logging.getLogger(__name__).info(
                    f"[NoveltyCheck] seed={seed!r} batch={batch} "
                    f"new_intents={new_intents}"
                )

                if new_intents < E_NOVELTY_MIN_NEW:
                    skipped = active_letters[batch_start + E_NOVELTY_BATCH:]
                    logging.getLogger(__name__).info(
                        f"[NoveltyThreshold] seed={seed!r} "
                        f"stopped after {batch_start + E_NOVELTY_BATCH}/{len(active_letters)} active letters, "
                        f"skipped={skipped}"
                    )
                    break

        # Step 5c: E firefox — ОТКЛЮЧЕНО для лайт-серча
        # async def run_letter_firefox(letter_queries: List, client: httpx.AsyncClient):
        #     for sq in letter_queries:
        #         if sq.variant in FF_E_SKIP:
        #             continue
        #         await asyncio.sleep(0.3)
        #         await fetch_one_firefox(sq, client)

        async def run_google(client: httpx.AsyncClient):
            # Phase 1: A/B/C/D + E_simple — параллельно (как раньше)
            phase1_tasks = [fetch_with_semaphore(sq, client) for sq in other_queries]
            phase1_tasks.append(run_e_simple(client))
            await asyncio.gather(*phase1_tasks)

            # Phase 2: E chrome — только буквы где E_simple дал >= 3 результатов
            await run_e_chrome_with_novelty(client)

        async def run_yandex(client: httpx.AsyncClient):
            ya_sem = asyncio.Semaphore(5)

            async def fetch_ya(sq_orig, suffix_type, suffix_label):
                async with ya_sem:
                    await asyncio.sleep(0.1)
                    sq_ya = SuffixQuery(
                        query=sq_orig.query, suffix_val=sq_orig.suffix_val,
                        suffix_label=suffix_label, suffix_type=suffix_type,
                        priority=sq_orig.priority, markers=sq_orig.markers, cp_override=-1,
                    )
                    results = await self.fetch_suggestions_yandex(sq_orig.query, language, client)
                    _record_results(sq_ya, results, 0)

            ya_tasks = []
            for sq in other_queries:
                if sq.suffix_type not in ("B", "C", "D"):
                    continue
                ya_tasks.append(fetch_ya(sq, sq.suffix_type + "_ya", sq.suffix_label + "_ya"))

            for char in ALPHABET:
                q = f"{seed} {char}"
                sq_char = SuffixQuery(
                    query=q, suffix_val=char,
                    suffix_label=f"simple_{char}",
                    suffix_type="E_simple", priority=1,
                    markers=[], cp_override=-1,
                )
                ya_tasks.append(fetch_ya(sq_char, "E_simple_ya", f"simple_{char}_ya"))

            await asyncio.gather(*ya_tasks)

        async def run_bing(client: httpx.AsyncClient):
            if language == "ru":
                return  # Bing returns 0 results for Russian queries on all tested seeds
            bi_sem = asyncio.Semaphore(5)

            async def fetch_bi(sq_orig, suffix_type, suffix_label):
                async with bi_sem:
                    await asyncio.sleep(0.1)
                    sq_bi = SuffixQuery(
                        query=sq_orig.query, suffix_val=sq_orig.suffix_val,
                        suffix_label=suffix_label, suffix_type=suffix_type,
                        priority=sq_orig.priority, markers=sq_orig.markers, cp_override=-1,
                    )
                    results = await self.fetch_suggestions_bing(sq_orig.query, language, country, client)
                    _record_results(sq_bi, results, 0)

            bi_tasks = []
            for sq in other_queries:
                if sq.suffix_type not in ("B", "C", "D"):
                    continue
                bi_tasks.append(fetch_bi(sq, sq.suffix_type + "_bi", sq.suffix_label + "_bi"))

            for char in ALPHABET:
                q = f"{seed} {char}"
                sq_char = SuffixQuery(
                    query=q, suffix_val=char,
                    suffix_label=f"simple_{char}",
                    suffix_type="E_simple", priority=1,
                    markers=[], cp_override=-1,
                )
                bi_tasks.append(fetch_bi(sq_char, "E_simple_bi", f"simple_{char}_bi"))

            await asyncio.gather(*bi_tasks)

        _google_proxy = os.getenv("GOOGLE_PROXY_URL") or None
        async with httpx.AsyncClient(proxy=_google_proxy) as g_http:
            await asyncio.gather(
                run_google(g_http),
                # run_yandex(ya_client),  # ОТКЛЮЧЕНО для лайт-серча
                # run_bing(bi_client),    # ОТКЛЮЧЕНО для лайт-серча
            )
            # ── Phase 2: candidate expansion (как в старом парсере) ──────
            # Собираем слова которые встречаются 2+ раз в результатах
            # Исключаем слова сида, делаем запрос сид + кандидат без cp
            seed_words_set = set(seed.lower().split())
            from collections import Counter
            import re as _re
            word_counter: Counter = Counter()
            for kw in all_keywords:
                for word in kw.lower().split():
                    if word not in seed_words_set and len(word) >= 4:
                        # только кириллица, без символов и латиницы
                        if _re.match(r'^[а-яёіїєґ]+$', word):
                            word_counter[word] += 1

            # Лемматизация для проверки гео (москве→москва, краснодаре→краснодар)
            try:
                import sys
                main_mod = sys.modules.get("main") or sys.modules.get("__main__")
                _morph = getattr(getattr(main_mod, "parser", None), "morph_ru", None) if main_mod else None
            except Exception:
                _morph = None

            def _is_geo(word: str) -> bool:
                if word in self.geo_db:
                    return True
                if _morph:
                    lemma = _morph.parse(word)[0].normal_form
                    if lemma in self.geo_db:
                        return True
                return False

            candidates = sorted(
                w for w, cnt in word_counter.items()
                if cnt >= 2 and not _is_geo(w)
            )

            p2_semaphore = asyncio.Semaphore(10)

            async def fetch_candidate(cand: str):
                async with p2_semaphore:
                    await asyncio.sleep(0.1)
                    q = f"{seed.lower()} {cand}"
                    sq_cand = SuffixQuery(
                        query=q,
                        suffix_val=cand,
                        suffix_label=f"cand_{cand}",
                        suffix_type="P2",
                        priority=1,
                        markers=["candidate"],
                        cp_override=-1,
                    )
                    t0 = time.time()
                    results = await self.fetch_suggestions(q, country, language, g_http, google_client, -1)
                    elapsed = (time.time() - t0) * 1000
                    _record_results(sq_cand, results, elapsed)

            # P2 временно отключён для замера времени
            # if candidates:
            #     await asyncio.gather(*[fetch_candidate(c) for c in candidates])

        total_time = (time.time() - total_start) * 1000

        # Step 6: Build result
        ok_count = sum(1 for t in trace_entries if t.status == "ok")
        empty_count = sum(1 for t in trace_entries if t.status == "empty")
        blocked_count = sum(1 for t in trace_entries if t.status.startswith("blocked"))

        # Summary by suffix type
        summary_by_type = {}
        for stype in ["A_ua", "A_ru", "B", "C", "D", "E", "E_ff", "E_simple",
                      "B_ya", "C_ya", "D_ya", "E_simple_ya",
                      "B_bi", "C_bi", "D_bi", "E_simple_bi", "P2"]:
            type_entries = [t for t in trace_entries if t.suffix_type == stype and not t.status.startswith("blocked")]
            summary_by_type[stype] = {
                "queries_sent": len(type_entries),
                "with_results": sum(1 for t in type_entries if t.status == "ok"),
                "empty": sum(1 for t in type_entries if t.status == "empty"),
                "total_keywords": sum(t.results_count for t in type_entries),
                "avg_time_ms": round(
                    sum(t.time_ms for t in type_entries) / max(len(type_entries), 1), 1
                ),
            }

        # Summary by individual suffix
        summary_by_suffix = {}
        for t in trace_entries:
            if not t.status.startswith("blocked"):
                summary_by_suffix[t.suffix_label] = {
                    "type": t.suffix_type,
                    "priority": t.priority,
                    "results_count": t.results_count,
                    "time_ms": t.time_ms,
                    "status": t.status,
                }

        # Build final keyword list sorted by weight desc, then alphabetically
        keywords_list = sorted(
            [{"keyword": kw, **data} for kw, data in all_keywords.items()],
            key=lambda x: (-x["weight"], x["keyword"])
        )

        # Считаем реально отправленные запросы из trace (не заблокированные)
        actual_sent = sum(
            1 for t in trace_entries
            if not t.status.startswith("blocked")
        )

        return SuffixParseResult(
            seed=seed,
            analysis=analysis_summary,
            all_keywords=keywords_list,
            total_queries=actual_sent,
            successful_queries=ok_count,
            empty_queries=empty_count,
            error_queries=0,
            blocked_queries=blocked_count,
            total_time_ms=round(total_time, 1),
            trace=[asdict(t) for t in trace_entries],
            summary_by_type=summary_by_type,
            summary_by_suffix=summary_by_suffix,
        )
