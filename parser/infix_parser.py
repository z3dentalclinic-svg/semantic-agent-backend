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

DELAY_LOCAL  = 0.3
DELAY_SERVER = 0.3
BATCH_SIZE   = 5

_google_proxy = os.getenv("GOOGLE_PROXY_URL") or None
DELAY = DELAY_SERVER if _google_proxy else DELAY_LOCAL

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
                                 google_client="firefox", cursor_position=None):
        url = "https://www.google.com/complete/search"
        params = {"q": query, "client": google_client,
                  "gl": country, "ie": "utf-8", "oe": "utf-8", "hl": language}
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
                    groups=None, progress_callback=None) -> InfixParseResult:
        from datetime import datetime
        total_start = time.time()
        timestamp = datetime.utcnow().isoformat() + "Z"

        matrix: List[InfixQuery] = self.generator.generate(seed=seed, groups=groups or ALL_GROUPS)

        kw_map: Dict[str, List[str]] = {}
        alt_seed_set: Set[str] = set()
        trace_entries: List[InfixTraceEntry] = []
        lock = asyncio.Lock()
        done_count = [0]

        non_e_sem = asyncio.Semaphore(BATCH_SIZE)

        async def fetch_one(iq: InfixQuery, client: httpx.AsyncClient):
            agent = iq.agents[0]
            await asyncio.sleep(DELAY)
            t0 = time.time()
            try:
                raw_results = await self.fetch_suggestions(
                    query=iq.query, country=country, language=language,
                    client=client, google_client=agent, cursor_position=iq.cp,
                )
                elapsed = (time.time() - t0) * 1000

                # Фильтр мусора
                results = [kw for kw in raw_results if not _is_garbage_keyword(kw)]

                status = "ok" if results else "empty"
                entry = InfixTraceEntry(
                    gap_index=iq.gap_index, w1=iq.w1, w2=iq.w2,
                    group=iq.group, struct=iq.struct,
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

        async def run_letter(letter_queries, client):
            for iq in letter_queries:
                await fetch_one(iq, client)

        async def run_non_e(iq, client):
            async with non_e_sem:
                await fetch_one(iq, client)

        async with httpx.AsyncClient(proxy=_google_proxy) as client:
            from collections import defaultdict
            e_by_letter = defaultdict(list)
            non_e = []
            for iq in matrix:
                if iq.group == "E" and iq.letter:
                    e_by_letter[iq.letter].append(iq)
                else:
                    non_e.append(iq)

            await asyncio.gather(
                *[run_letter(qs, client) for qs in e_by_letter.values()],
                *[run_non_e(iq, client) for iq in non_e],
            )

        total_time = (time.time() - total_start) * 1000

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
            total_keywords=len(kw_map), exclusive_count=len(exclusive_kw),
            total_time_ms=round(total_time, 1),
            trace=[asdict(e) for e in trace_entries],
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
