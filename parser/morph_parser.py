"""
Morph Parser v2.0 — Fetch + 4-axis trace for full suffix map × case variants.

Fetch architecture:
- Queries grouped by case_label (up to 12 groups)
- All case groups run in PARALLEL via asyncio.gather
- Within each case group:
    - A/B/C/D queries: all in parallel (asyncio.gather, semaphore=10)
    - E queries (letter sweep): letters are SEQUENTIAL, 0.4s delay per letter
      Each letter: chrome + firefox fetched back-to-back
- Two httpx clients per case group: chrome_client + firefox_client (isolated)

Trace structure (4 axes — for dataset analysis):
  trace[case_label][suffix_label][ua_type] = {
      "query": str,
      "results": [str, ...],
      "count": int,
      "suffix_type": str,    # A/B/C/D/E
      "suffix_val": str,
      "variant": str,
      "priority": int,
  }

  Plus per-case aggregates:
  trace[case_label]["_meta"] = {
      "seed_variant": str,
      "case_display": str,
      "raw_count": int,            # unique raw keywords this case
      "normalized_count": int,
      "unique_count": int,         # exclusive to this case post cross-analysis
      "by_type": { A: {raw, norm, unique}, ... }  # per suffix type breakdown
  }

Post-run analysis fields (after 10 datasets):
  Use trace[case_label][suffix_label][ua_type]["count"] to rank:
    - Which cases add the most unique normalized keywords
    - Which suffix_types per case are redundant (all dupes from other cases)
    - Which UA (chrome vs firefox) adds exclusive results per structure
"""

import asyncio
import httpx
import time
import json
import re
import logging
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict

import pymorphy3

from parser.morph_generator import MorphGenerator, MorphQuery, MorphSeedAnalysis, CASES_RU

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

MORPH_DELAY = 0.4          # seconds between letter groups in E-type sweep
ABCD_SEMAPHORE = 10        # max parallel A/B/C/D requests per case group
CASE_PARALLEL = True       # run all case groups in parallel (asyncio.gather)


def _load_proxy_pool() -> List[Optional[str]]:
    """
    Load proxy pool from env vars.

    Priority:
      1. MORPH_PROXY_1 / MORPH_PROXY_2 / MORPH_PROXY_3  — dedicated morph proxies
      2. GOOGLE_PROXY_URL                                 — fallback (same as suffix_parser)
      3. No proxy                                         — [None]

    Format: "http://user:pass@host:port" or "http://host:port"

    Each case batch gets a proxy by round-robin: proxy_pool[case_index % len(pool)]
    With 3 proxies and 10 cases: each proxy handles ~3-4 cases.
    Peak load per IP: ~90 concurrent requests instead of 280.
    """
    pool: List[Optional[str]] = []

    for key in ["MORPH_PROXY_1", "MORPH_PROXY_2", "MORPH_PROXY_3"]:
        val = os.getenv(key, "").strip()
        if val:
            pool.append(val)

    if not pool:
        fallback = os.getenv("GOOGLE_PROXY_URL", "").strip()
        if fallback:
            pool.append(fallback)
            logger.info("[MORPH] No MORPH_PROXY_* found, using GOOGLE_PROXY_URL as single proxy")

    if pool:
        logger.info(f"[MORPH] Proxy pool: {len(pool)} proxies loaded")
    else:
        logger.warning("[MORPH] No proxies configured — all requests from server IP")

    return pool if pool else [None]

UA_STRINGS = {
    "chrome":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "firefox": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
               "Gecko/20100101 Firefox/121.0",
}

GOOGLE_CLIENTS = {
    "chrome":  "chrome",
    "firefox": "firefox",
}


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class MorphParseResult:
    """Full result of morphology parsing."""
    seed: str
    analysis_summary: Dict
    keywords: List[str] = field(default_factory=list)      # normalized, deduped across all cases
    keywords_raw: List[str] = field(default_factory=list)  # raw pre-normalization, deduped
    trace: Dict = field(default_factory=dict)              # 4-axis trace (see module docstring)
    stats: Dict = field(default_factory=dict)
    total_time_s: float = 0.0


# ── Normalizer ───────────────────────────────────────────────────────────────

class MorphNormalizer:
    """
    Normalizes raw autocomplete results to nominative case.
    Algorithm: session_2026_03_15 §6 (final, agreed by 4 models).

    1. First word must be cyrillic
    2. Find parse: lemma == original_lemma AND POS=NOUN AND score>=0.3
    3. Inflect to nominative, preserve sing/plur
    4. Reconstruct: nom_form + rest_of_words
    5. Dedup via set()
    """

    def __init__(self, lang: str = "ru"):
        self.morph = pymorphy3.MorphAnalyzer(lang=lang)

    def _is_cyrillic(self, word: str) -> bool:
        return bool(re.match(r'^[а-яёА-ЯЁ]+$', word))

    def normalize(self, raw_keywords: List[str], original_lemma: str) -> List[str]:
        normalized: Set[str] = set()

        for kw in raw_keywords:
            kw = kw.strip()
            if not kw:
                continue
            words = kw.split()
            first_word = words[0]

            if not self._is_cyrillic(first_word):
                continue

            best = None
            for p in self.morph.parse(first_word):
                if (p.normal_form == original_lemma and
                        p.tag.POS == 'NOUN' and
                        p.score >= 0.3):
                    best = p
                    break

            if best is None:
                continue

            grammemes = {'nomn'}
            if 'plur' in best.tag:
                grammemes.add('plur')
            elif 'sing' in best.tag:
                grammemes.add('sing')

            inflected = best.inflect(grammemes)
            nom_form = inflected.word if inflected else best.normal_form

            tail = ' '.join(words[1:])
            normalized_key = f"{nom_form} {tail}".strip() if tail else nom_form
            normalized.add(normalized_key)

        return sorted(normalized)


# ── Parser ───────────────────────────────────────────────────────────────────

class MorphParser:
    """
    Orchestrates: morph generation → parallel fetch → 4-axis trace → normalization.
    """

    def __init__(self, lang: str = "ru"):
        self.generator = MorphGenerator(lang=lang)
        self.normalizer = MorphNormalizer(lang=lang)

    # ── HTTP ───────────────────────────────────────────────────────────────

    def _clean(self, text: str) -> str:
        return re.sub(r'<[^>]+>', '', text).strip()

    async def _fetch(
        self,
        query: str,
        ua_type: str,
        cp: Optional[int],
        country: str,
        language: str,
        client: httpx.AsyncClient,
    ) -> List[str]:
        """Single Google Autocomplete fetch. Handles all known response formats."""
        params = {
            "q": query,
            "client": GOOGLE_CLIENTS[ua_type],
            "hl": language,
            "gl": country,
            "ie": "utf-8",
            "oe": "utf-8",
        }
        if cp is not None and cp >= 0:
            params["cp"] = cp
        elif cp is None:
            params["cp"] = len(query)
        # cp == -1 → don't send cp (plain_nocp variant)

        headers = {"User-Agent": UA_STRINGS[ua_type]}

        try:
            resp = await client.get(
                "https://www.google.com/complete/search",
                params=params, headers=headers, timeout=10.0
            )

            if resp.status_code == 429:
                logger.warning(f"[MORPH] 429 rate-limit q='{query}' ua={ua_type}")
                return []
            if resp.status_code != 200:
                return []

            text = resp.text.strip()

            # Format 1: clean JSON (firefox, chrome)
            try:
                data = resp.json()
                if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                    return self._parse_suggestions(data[1])
            except Exception:
                pass

            # Format 2: security prefix  )]}'
            if text.startswith(")]}'"):
                try:
                    data = json.loads(text[4:].strip())
                    if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                        return self._parse_suggestions(data[1])
                except Exception:
                    pass

            # Format 3: JSONP callback
            m = re.search(r'\((\[.+\])\)\s*;?\s*$', text, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                        return self._parse_suggestions(data[1])
                except Exception:
                    pass

        except Exception as e:
            logger.debug(f"[MORPH] fetch error: {e}")

        return []

    def _parse_suggestions(self, raw: list) -> List[str]:
        result = []
        for item in raw:
            if isinstance(item, str):
                result.append(self._clean(item))
            elif isinstance(item, list) and item and isinstance(item[0], str):
                result.append(self._clean(item[0]))
            elif isinstance(item, dict):
                s = item.get("suggestion") or item.get("value") or item.get("text", "")
                if s:
                    result.append(self._clean(str(s)))
        return [r for r in result if r]

    # ── Case batch fetch ───────────────────────────────────────────────────

    async def _fetch_case_batch(
        self,
        case_label: str,
        case_queries: List[MorphQuery],
        country: str,
        language: str,
        proxy: Optional[str] = None,
    ) -> Dict:
        """
        Fetch all queries for one case variant and build the per-case trace.

        Structure returned:
        {
          case_label: str,
          raw_keywords: set[str],
          trace: {
            suffix_label: {
              ua_type: {
                query, results, count, suffix_type, suffix_val, variant, priority
              }
            }
          },
          blocked: [ {suffix_label, suffix_val, blocked_by}, ... ]
        }
        """
        case_trace: Dict = {}   # suffix_label → {ua_type → entry}
        blocked_list = []
        all_raw: Set[str] = set()

        # Separate blocked from active
        active_queries = [q for q in case_queries if q.priority > 0]
        blocked_queries = [q for q in case_queries if q.priority == 0]

        for q in blocked_queries:
            blocked_list.append({
                "suffix_label": q.suffix_label,
                "suffix_val": q.suffix_val,
                "suffix_type": q.suffix_type,
                "blocked_by": q.blocked_by,
            })

        # Split by type: A/B/C/D (parallel) vs E (sequential per letter)
        abcd_queries = [q for q in active_queries if q.suffix_type != "E"]
        e_queries    = [q for q in active_queries if q.suffix_type == "E"]

        async with httpx.AsyncClient(proxy=proxy) as client:

            # ── A/B/C/D: all parallel via semaphore ───────────────────────
            sem = asyncio.Semaphore(ABCD_SEMAPHORE)

            async def fetch_abcd(q: MorphQuery, ua_type: str) -> tuple:
                async with sem:
                    results = await self._fetch(
                        q.query, ua_type, q.cp_override, country, language, client
                    )
                    return q, ua_type, results

            abcd_tasks = [
                fetch_abcd(q, ua_type)
                for q in abcd_queries
                for ua_type in ["chrome", "firefox"]
            ]
            abcd_results = await asyncio.gather(*abcd_tasks, return_exceptions=True)

            for item in abcd_results:
                if isinstance(item, Exception):
                    logger.error(f"[MORPH] ABCD fetch exception: {item}")
                    continue
                q, ua_type, results = item
                all_raw.update(r.lower() for r in results)
                sl = q.suffix_label
                if sl not in case_trace:
                    case_trace[sl] = {}
                case_trace[sl][ua_type] = {
                    "query": q.query,
                    "results": results,
                    "count": len(results),
                    "suffix_type": q.suffix_type,
                    "suffix_val": q.suffix_val,
                    "variant": q.variant,
                    "priority": q.priority,
                }

            # ── E (letters): grouped by letter, sequential ─────────────────
            # Group by letter so we process all structures for one letter together
            # before moving to the next letter (reduces per-letter burst)
            by_letter: Dict[str, List[MorphQuery]] = defaultdict(list)
            for q in e_queries:
                # suffix_label: "а_plain" → letter is suffix_val
                by_letter[q.suffix_val].append(q)

            for letter, letter_queries in by_letter.items():
                # All 14 structures for this letter: fetch chrome+firefox per structure
                # One MORPH_DELAY sleep at the END of the full letter group
                # (not after every fetch — that would multiply delay by 28x per letter)
                for q in letter_queries:
                    for ua_type in ["chrome", "firefox"]:
                        results = await self._fetch(
                            q.query, ua_type, q.cp_override, country, language, client
                        )
                        all_raw.update(r.lower() for r in results)
                        sl = q.suffix_label
                        if sl not in case_trace:
                            case_trace[sl] = {}
                        case_trace[sl][ua_type] = {
                            "query": q.query,
                            "results": results,
                            "count": len(results),
                            "suffix_type": q.suffix_type,
                            "suffix_val": q.suffix_val,
                            "variant": q.variant,
                            "priority": q.priority,
                        }
                # One delay after the full letter group (between letters)
                await asyncio.sleep(MORPH_DELAY)

        return {
            "case_label": case_label,
            "raw_keywords": all_raw,
            "trace": case_trace,
            "blocked": blocked_list,
        }

    # ── Main parse ─────────────────────────────────────────────────────────

    async def parse(
        self,
        seed: str,
        country: str = "ua",
        language: str = "ru",
        region: str = "ua",
        include_numbers: bool = False,
    ) -> MorphParseResult:
        """
        Main parse method.

        Steps:
        1. Analyze seed → MorphSeedAnalysis (noun + case variants)
        2. Generate all MorphQuery objects (full suffix map × all cases)
        3. Group by case_label → parallel asyncio.gather across all case groups
        4. Build 4-axis trace per case
        5. Normalize + cross-case unique analysis
        6. Assemble stats
        """
        total_start = time.time()

        # ── Step 1: Seed analysis ──────────────────────────────────────────
        analysis = self.generator.analyze_seed(seed)
        if analysis is None:
            return MorphParseResult(
                seed=seed,
                analysis_summary={"error": "No suitable noun found in seed"},
                stats={"error": "no_noun"},
            )

        # ── Step 2: Generate all queries ───────────────────────────────────
        all_queries = self.generator.generate_queries(
            analysis,
            region=region,
            include_numbers=include_numbers,
            include_letters=True,
        )
        analysis_summary = self.generator.summary(analysis, all_queries)

        logger.info(
            f"[MORPH] seed='{seed}' noun='{analysis.original_noun}' "
            f"lemma='{analysis.original_lemma}' "
            f"cases={len(analysis.case_variants)} "
            f"total_queries={len(all_queries)}"
        )

        # ── Step 3: Group by case → parallel fetch ─────────────────────────
        by_case: Dict[str, List[MorphQuery]] = defaultdict(list)
        for q in all_queries:
            by_case[q.case_label].append(q)

        # Proxy pool: each case gets a proxy by round-robin
        proxy_pool = _load_proxy_pool()
        case_tasks = [
            self._fetch_case_batch(
                case_label, case_queries, country, language,
                proxy=proxy_pool[i % len(proxy_pool)],
            )
            for i, (case_label, case_queries) in enumerate(by_case.items())
        ]
        batch_results = await asyncio.gather(*case_tasks, return_exceptions=True)

        # ── Step 4: Build trace + collect raw keywords ─────────────────────
        # trace[case_label][suffix_label][ua_type] → entry
        full_trace: Dict = {}
        all_raw_keywords: Set[str] = set()

        case_raw_sets: Dict[str, Set[str]] = {}   # for cross-case unique analysis

        for batch in batch_results:
            if isinstance(batch, Exception):
                logger.error(f"[MORPH] Case batch exception: {batch}")
                continue

            cl = batch["case_label"]
            raw_kws = batch["raw_keywords"]
            all_raw_keywords.update(raw_kws)
            case_raw_sets[cl] = raw_kws

            _case_tag, _number_tag, case_display = CASES_RU[cl]
            full_trace[cl] = {
                "_meta": {
                    "case_display": case_display,
                    "seed_variant": analysis.case_variants.get(cl, ""),
                    "raw_count": len(raw_kws),
                    "blocked_count": len(batch["blocked"]),
                    "blocked": batch["blocked"],
                    # normalized_count + unique_count filled in Step 5
                },
                **batch["trace"],   # suffix_label → {ua_type → entry}
            }

        # ── Step 5: Normalization + cross-case unique analysis ─────────────
        # Global normalization
        all_normalized = self.normalizer.normalize(
            list(all_raw_keywords), analysis.original_lemma
        )

        # Per-case normalization
        case_norm_sets: Dict[str, Set[str]] = {}
        for cl, raw_set in case_raw_sets.items():
            norm = self.normalizer.normalize(list(raw_set), analysis.original_lemma)
            case_norm_sets[cl] = set(norm)
            if cl in full_trace:
                full_trace[cl]["_meta"]["normalized_count"] = len(norm)
                full_trace[cl]["_meta"]["normalized_keywords"] = norm

        # Per-case uniqueness: keywords exclusive to this case only
        all_norm_union = set(all_normalized)
        for cl, my_norm in case_norm_sets.items():
            other_union: Set[str] = set()
            for other_cl, other_norm in case_norm_sets.items():
                if other_cl != cl:
                    other_union.update(other_norm)
            unique_to_case = my_norm - other_union
            if cl in full_trace:
                full_trace[cl]["_meta"]["unique_count"] = len(unique_to_case)
                full_trace[cl]["_meta"]["unique_keywords"] = sorted(unique_to_case)

        # Per-suffix-label uniqueness within each case
        # (which exact suffix_label added keywords not found by any other suffix_label in this case)
        for cl in full_trace:
            case_meta = full_trace[cl].get("_meta", {})
            # Aggregate: per suffix_label → all results combined (chrome+firefox)
            suffix_kw_sets: Dict[str, Set[str]] = {}
            for key, val in full_trace[cl].items():
                if key == "_meta":
                    continue
                # val = {ua_type → entry}
                kws: Set[str] = set()
                for ua_type, entry in val.items():
                    for r in entry.get("results", []):
                        kws.add(r.lower())
                suffix_kw_sets[key] = kws

            for suffix_label, my_kws in suffix_kw_sets.items():
                other_kws: Set[str] = set()
                for other_sl, other_kws_set in suffix_kw_sets.items():
                    if other_sl != suffix_label:
                        other_kws.update(other_kws_set)
                unique_for_suffix = my_kws - other_kws
                # Add unique_count to each ua_type entry for this suffix
                for ua_type in full_trace[cl].get(suffix_label, {}):
                    full_trace[cl][suffix_label][ua_type]["unique_in_case"] = len(unique_for_suffix)

        # ── Step 6: Stats ──────────────────────────────────────────────────
        active_q = [q for q in all_queries if q.priority > 0]
        blocked_q = [q for q in all_queries if q.priority == 0]

        stats = {
            "cases_active": len(analysis.case_variants),
            "cases_skipped": len(analysis.skipped_cases),
            "total_morph_queries_generated": len(all_queries),
            "active_queries": len(active_q),
            "blocked_queries": len(blocked_q),
            "fetch_requests": len(active_q) * 2,   # chrome + firefox per query
            "total_raw_keywords": len(all_raw_keywords),
            "total_normalized": len(all_normalized),
            "by_case": {
                cl: {
                    "raw": full_trace[cl]["_meta"].get("raw_count", 0),
                    "normalized": full_trace[cl]["_meta"].get("normalized_count", 0),
                    "unique": full_trace[cl]["_meta"].get("unique_count", 0),
                }
                for cl in full_trace
            },
        }

        total_time = round(time.time() - total_start, 1)
        logger.info(
            f"[MORPH] Done | raw={len(all_raw_keywords)} "
            f"normalized={len(all_normalized)} "
            f"time={total_time}s"
        )

        return MorphParseResult(
            seed=seed,
            analysis_summary=analysis_summary,
            keywords=all_normalized,
            keywords_raw=sorted(all_raw_keywords),
            trace=full_trace,
            stats=stats,
            total_time_s=total_time,
        )
