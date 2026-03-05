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
    suffix_type: str  # A, B, C, D
    priority: int
    query_sent: str
    results_count: int = 0
    results: List[str] = field(default_factory=list)
    time_ms: float = 0.0
    status: str = "pending"  # ok, empty, error, blocked, rate_limit


@dataclass 
class SuffixParseResult:
    """Full result of suffix parsing"""
    seed: str
    analysis: Dict
    all_keywords: List[str] = field(default_factory=list)
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

    def __init__(self, lang: str = "ru"):
        self.generator = SuffixGenerator(lang=lang)
        self.adaptive_delay = AdaptiveDelay()

    async def fetch_suggestions(self, query: str, country: str, language: str,
                                 client: httpx.AsyncClient, google_client: str = "firefox") -> List[str]:
        """Google Autocomplete with multi-client support.
        
        Different clients return different formats:
        - firefox/chrome → clean JSON: ["query", ["s1", "s2"]]
        - psy-ab/gws-wiz → JSONP or prefixed: )]}'\\n["query", [...]]
        - safari → may return JSONP callback(...)
        """
        url = "https://www.google.com/complete/search"
        params = {"q": query, "client": google_client, "hl": language, "gl": country}
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
                            # Flatten: could be ["s1", "s2"] or [["s1",0,[512]], ["s2",0,[512]]]
                            result = []
                            for item in raw:
                                if isinstance(item, str):
                                    result.append(item)
                                elif isinstance(item, list) and len(item) > 0 and isinstance(item[0], str):
                                    result.append(item[0])
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
                                        result.append(item)
                                    elif isinstance(item, list) and len(item) > 0 and isinstance(item[0], str):
                                        result.append(item[0])
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
                                        result.append(item)
                                    elif isinstance(item, list) and len(item) > 0:
                                        result.append(str(item[0]))
                                return result
                    except Exception:
                        pass

        except Exception:
            pass
        return []

    async def parse(self, seed: str, country: str = "ua", language: str = "ru",
                    parallel_limit: int = 5, include_numbers: bool = False,
                    echelon: int = 0, google_client: str = "firefox") -> SuffixParseResult:
        """
        Main parse method.
        
        Args:
            seed: Input keyword
            country: Target country code
            language: Language code
            parallel_limit: Max concurrent requests
            include_numbers: Add numeric suffixes
            echelon: 0=all, 1=only priority 1, 2=only priority 2
        """
        total_start = time.time()

        # Step 1: Generate queries
        analysis, all_queries = self.generator.generate(seed, include_numbers=include_numbers)
        analysis_summary = self.generator.summary(analysis, all_queries)

        # Step 2: Filter by echelon
        if echelon == 1:
            queries_to_send = [q for q in all_queries if q.priority == 1]
        elif echelon == 2:
            queries_to_send = [q for q in all_queries if q.priority == 2]
        else:
            queries_to_send = [q for q in all_queries if q.priority > 0]

        blocked_queries = [q for q in all_queries if q.priority == 0]

        # Step 3: Send to autocomplete in parallel
        semaphore = asyncio.Semaphore(parallel_limit)
        all_keywords = set()
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

        async def fetch_one(sq: SuffixQuery, client: httpx.AsyncClient):
            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())

                t0 = time.time()
                results = await self.fetch_suggestions(sq.query, country, language, client, google_client)
                elapsed = (time.time() - t0) * 1000

                entry = SuffixTraceEntry(
                    suffix_val=sq.suffix_val,
                    suffix_label=sq.suffix_label,
                    suffix_type=sq.suffix_type,
                    priority=sq.priority,
                    query_sent=sq.query,
                    results_count=len(results),
                    results=results,
                    time_ms=round(elapsed, 1),
                    status="ok" if results else "empty",
                )
                trace_entries.append(entry)

                if results:
                    all_keywords.update(results)

        async with httpx.AsyncClient() as client:
            tasks = [fetch_one(sq, client) for sq in queries_to_send]
            await asyncio.gather(*tasks)

        total_time = (time.time() - total_start) * 1000

        # Step 4: Build result with trace
        ok_count = sum(1 for t in trace_entries if t.status == "ok")
        empty_count = sum(1 for t in trace_entries if t.status == "empty")
        blocked_count = sum(1 for t in trace_entries if t.status.startswith("blocked"))

        # Summary by suffix type
        summary_by_type = {}
        for stype in ["A", "B", "C", "D"]:
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

        return SuffixParseResult(
            seed=seed,
            analysis=analysis_summary,
            all_keywords=sorted(list(all_keywords)),
            total_queries=len(queries_to_send),
            successful_queries=ok_count,
            empty_queries=empty_count,
            error_queries=0,
            blocked_queries=blocked_count,
            total_time_ms=round(total_time, 1),
            trace=[asdict(t) for t in trace_entries],
            summary_by_type=summary_by_type,
            summary_by_suffix=summary_by_suffix,
        )
