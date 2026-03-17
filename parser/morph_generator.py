"""
Morph Generator v2.0 — Full suffix map × all case variants.

Architecture:
- Finds first noun in seed → generates up to 12 case variants (6 cases × 2 numbers)
- Identical case forms are deduplicated (e.g. accs_sing == nomn_sing for inanimate nouns)
- For each unique case variant → runs FULL SuffixGenerator (A + B + C + D + E structures)
- Result: case_variant × suffix_map = ~8,000+ queries per seed

Query count estimate (10 active cases after dedup):
  Type A (symbols):                  1 sym × 2 cp_variants × 10 = 20
  Type B (prepositions + trail):     8 prep × (3 cp + 1 trail) × 10 = 320
  Type C (questions):                5 q × 3 cp × 10 = 150
  Type D (finalizers):               12 fin × 3 cp × 10 = 360
  Type E (26 ltr × 14 struct × 10): 3640 MorphQuery objects
  ─────────────────────────────────────────────────────────────────────
  Total MorphQuery objects (before UA split):                   ~4,490
  After ×2 UA (chrome/firefox) in parser for each query:        ~8,980

Trace axes (for 10-dataset post-run analysis):
  case_label   → which case inflection adds unique results
  suffix_type  → A/B/C/D/E — which suffix class per case
  suffix_label → exact suffix structure (prep_v_v1, а_plain, etc.)
  ua_type      → chrome vs firefox — which UA per structure
"""

import pymorphy3
import re
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field

from parser.suffix_generator import SuffixGenerator, SuffixQuery


# ══════════════════════════════════════════════
# CASE DEFINITIONS
# ══════════════════════════════════════════════

# case_label → (pymorphy3_case_tag, pymorphy3_number_tag, human_display)
CASES_RU: Dict[str, Tuple[str, str, str]] = {
    "nomn_sing": ("nomn", "sing", "Именительный ед.ч."),
    "gent_sing":  ("gent", "sing", "Родительный ед.ч."),
    "datv_sing":  ("datv", "sing", "Дательный ед.ч."),
    "accs_sing":  ("accs", "sing", "Винительный ед.ч."),
    "ablt_sing":  ("ablt", "sing", "Творительный ед.ч."),
    "loct_sing":  ("loct", "sing", "Предложный ед.ч."),
    "nomn_plur":  ("nomn", "plur", "Именительный мн.ч."),
    "gent_plur":  ("gent", "plur", "Родительный мн.ч."),
    "datv_plur":  ("datv", "plur", "Дательный мн.ч."),
    "accs_plur":  ("accs", "plur", "Винительный мн.ч."),
    "ablt_plur":  ("ablt", "plur", "Творительный мн.ч."),
    "loct_plur":  ("loct", "plur", "Предложный мн.ч."),
}


# ══════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════

@dataclass
class MorphQuery:
    """
    Single generated query: full SuffixQuery metadata + case metadata.
    Parser iterates these, fetches Google (chrome + firefox), records trace.
    """
    # ── Case metadata ──────────────────────────────────────────────────────
    case_label: str        # "gent_sing"
    case_display: str      # "Родительный ед.ч."
    seed_variant: str      # Inflected seed: "курса английского языка киев"

    # ── Suffix metadata (mirrors SuffixQuery) ─────────────────────────────
    query: str             # Full query: "курса английского языка киев а"
    suffix_val: str        # "а" / "в *" / ":" / "купить *"
    suffix_label: str      # "а_plain" / "prep_v_v1" / "fin_kupit_v1"
    suffix_type: str       # A / B / C / D / E
    priority: int          # 1 or 2 (0 = blocked — recorded in trace but not fetched)
    cp_override: Optional[int] = None   # cursor position override for Google
    variant: Optional[str] = None       # v1/v2/v3/trail/plain/sandwich/...
    blocked_by: Optional[str] = None    # self-match reason if priority == 0


@dataclass
class MorphSeedAnalysis:
    """Result of morphological seed analysis."""
    original_seed: str
    original_noun: str           # Word that was inflected, e.g. "курсов"
    original_noun_idx: int       # Position in word list (0-based)
    original_lemma: str          # Lemma: "курс"
    case_variants: Dict[str, str]   # case_label → seed_variant (deduped)
    skipped_cases: List[str]        # Reasons for skipped cases


# ══════════════════════════════════════════════
# GENERATOR
# ══════════════════════════════════════════════

class MorphGenerator:
    """
    Generates full suffix map for all unique case variants of the first noun in seed.

    Core idea: for each case_variant string → call SuffixGenerator.generate().
    SuffixGenerator already handles everything:
      A/B/C/D/E structures, cp_override variants, self-match filter,
      trailing space ("в " / "на "), priority matrix, letter sweep.
    We just stamp case_label + case_display + seed_variant on every SuffixQuery.
    """

    def __init__(self, lang: str = "ru"):
        self.lang = lang
        self.morph = pymorphy3.MorphAnalyzer(lang=lang)
        self.suffix_gen = SuffixGenerator(lang=lang)

    # ── Noun detection ─────────────────────────────────────────────────────

    def _is_cyrillic_word(self, word: str) -> bool:
        return bool(re.match(r'^[а-яёА-ЯЁ]+$', word))

    def _find_first_noun(self, words: List[str]) -> Optional[Tuple[int, str, str, object]]:
        """
        Find first noun in word list.
        Pass 1 (strict):   POS=NOUN + score>=0.3 + cyrillic
        Pass 2 (fallback): any POS=NOUN + cyrillic
        Returns (idx, word, lemma, parsed_obj) or None.
        """
        for strict in [True, False]:
            for idx, word in enumerate(words):
                if not self._is_cyrillic_word(word):
                    continue
                for p in self.morph.parse(word):
                    if p.tag.POS == 'NOUN':
                        if not strict or p.score >= 0.3:
                            return idx, word, p.normal_form, p
        return None

    # ── Seed analysis ──────────────────────────────────────────────────────

    def analyze_seed(self, seed: str) -> Optional[MorphSeedAnalysis]:
        """
        Find first noun → build up to 12 case variants (6 cases × 2 numbers).
        Identical forms are automatically deduplicated.

        Fallback chain:
          1. First cyrillic NOUN score>=0.3
          2. Any cyrillic NOUN
          3. First cyrillic word that produces at least 1 inflection
          4. If nothing inflects → use nomn_sing only (seed as-is)
        Returns None only if seed is empty.
        """
        words = seed.lower().strip().split()
        if not words:
            return None

        noun_data = self._find_first_noun(words)

        if noun_data is None:
            # Fallback: try every cyrillic word, pick first NOUN that can be inflected
            for idx, word in enumerate(words):
                if not self._is_cyrillic_word(word):
                    continue  # skip digits, latin (3060, rtx, ртх)
                parses = self.morph.parse(word)
                if not parses:
                    continue
                for p in parses:
                    if p.tag.POS != 'NOUN':
                        continue
                    test = p.inflect({'nomn', 'sing'}) or p.inflect({'nomn', 'plur'})
                    if test:
                        noun_data = (idx, word, p.normal_form, p)
                        break
                if noun_data:
                    break

        if noun_data is None:
            # Last-resort: no inflectable cyrillic word found.
            # Use nomn_sing only (seed as-is) so at least one case runs.
            # This handles "купить ртх 3060" — no noun, but we still sweep letters.
            idx = next(
                (i for i, w in enumerate(words) if self._is_cyrillic_word(w)),
                len(words) - 1
            )
            word = words[idx]
            parses = self.morph.parse(word)
            if not parses:
                return None
            noun_data = (idx, word, parses[0].normal_form, parses[0])
            # Force only nomn_sing — no inflection available
            return MorphSeedAnalysis(
                original_seed=seed.lower().strip(),
                original_noun=word,
                original_noun_idx=idx,
                original_lemma=parses[0].normal_form,
                case_variants={"nomn_sing": seed.lower().strip()},
                skipped_cases=["all_other:no_inflectable_noun_found"],
            )

        idx, word, lemma, parsed = noun_data

        case_variants: Dict[str, str] = {}
        seen_variants: set = set()
        skipped_cases: List[str] = []

        for case_label, (case_tag, number_tag, _display) in CASES_RU.items():
            inflected = parsed.inflect({case_tag, number_tag})
            if inflected is None:
                skipped_cases.append(f"{case_label}:no_inflection")
                continue

            new_words = words.copy()
            new_words[idx] = inflected.word
            seed_variant = " ".join(new_words)

            if seed_variant in seen_variants:
                # Duplicate form (e.g. inanimate: accs_sing == nomn_sing)
                skipped_cases.append(f"{case_label}:dup({inflected.word})")
                continue

            seen_variants.add(seed_variant)
            case_variants[case_label] = seed_variant

        return MorphSeedAnalysis(
            original_seed=seed.lower().strip(),
            original_noun=word,
            original_noun_idx=idx,
            original_lemma=lemma,
            case_variants=case_variants,
            skipped_cases=skipped_cases,
        )

    # ── Query generation ───────────────────────────────────────────────────

    def generate_queries(
        self,
        analysis: MorphSeedAnalysis,
        region: str = "ua",
        include_numbers: bool = False,
        include_letters: bool = True,
    ) -> List[MorphQuery]:
        """
        For each case_variant → run SuffixGenerator.generate() → wrap into MorphQuery.

        Priority-0 (self-match blocked) queries ARE included so the trace shows
        which suffixes were blocked per case — useful for structural analysis.

        Args:
            region:          "ua" / "ru" / "all" — passed to SuffixGenerator
            include_numbers: add numeric suffixes (Type A_num)
            include_letters: add letter sweep (Type E, 26 letters × 14 structures)
        """
        queries: List[MorphQuery] = []

        for case_label, seed_variant in analysis.case_variants.items():
            _case_tag, _number_tag, case_display = CASES_RU[case_label]

            # Full suffix map for this case variant
            _seed_analysis, suffix_queries = self.suffix_gen.generate(
                seed=seed_variant,
                include_numbers=include_numbers,
                include_letters=include_letters,
                region=region,
            )

            for sq in suffix_queries:
                queries.append(MorphQuery(
                    case_label=case_label,
                    case_display=case_display,
                    seed_variant=seed_variant,
                    query=sq.query,
                    suffix_val=sq.suffix_val,
                    suffix_label=sq.suffix_label,
                    suffix_type=sq.suffix_type,
                    priority=sq.priority,
                    cp_override=sq.cp_override,
                    variant=sq.variant,
                    blocked_by=sq.blocked_by,
                ))

        return queries

    # ── Summary ────────────────────────────────────────────────────────────

    def summary(self, analysis: MorphSeedAnalysis, queries: List[MorphQuery]) -> Dict:
        """Human-readable summary for logging and HTML trace display."""
        active = [q for q in queries if q.priority > 0]
        blocked = [q for q in queries if q.priority == 0]

        by_case: Dict = {}
        for case_label in analysis.case_variants:
            case_qs = [q for q in queries if q.case_label == case_label]
            case_active = [q for q in case_qs if q.priority > 0]
            by_case[case_label] = {
                "seed_variant": analysis.case_variants[case_label],
                "total": len(case_qs),
                "active": len(case_active),
                "blocked": len(case_qs) - len(case_active),
                "by_type": {
                    t: len([q for q in case_active if q.suffix_type == t])
                    for t in ["A", "B", "C", "D", "E"]
                },
            }

        return {
            "original_seed": analysis.original_seed,
            "noun": analysis.original_noun,
            "lemma": analysis.original_lemma,
            "noun_position": analysis.original_noun_idx,
            "cases_active": len(analysis.case_variants),
            "cases_skipped": len(analysis.skipped_cases),
            "skipped_detail": analysis.skipped_cases,
            "case_variants": analysis.case_variants,
            "total_morph_queries": len(queries),
            "active_morph_queries": len(active),
            "blocked_morph_queries": len(blocked),
            "by_case": by_case,
        }
