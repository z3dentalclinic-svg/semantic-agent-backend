"""
Suffix Generator v1.0 — Smart suffix expansion based on seed analysis.

Architecture:
- Detects seed markers (Q, T, S, P, L) via pymorphy3
- Generates suffix queries with priority (1=mainstream, 2=deep)
- Self-match filter removes duplicates
- min() rule resolves multi-marker conflicts

Matrix v3.0 (Gemini + Claude corrections):
    Marker  | Type A (depth) | Type B (prep) | Type C (question) | Type D (finalizer)
    Q       |       1        |       1       |     2 (self)      |        2
    T       |       1        |       1       |        2          |     1 (self)
    S       |       1        |       1       |        1          |        1
    L1      |       1        |       1       |        1          |        1
    L2      |       1        |       1       |        1          |        1
    L3      |       1        |       1       |        2          |        1
    L4      |       1        |       2       |        2          |        2
    L5+     |       1        |       2       |        2          |        2
    P       |       1        |    1 (self)   |        1          |        1
    Default |       1        |       1       |        1          |        1
"""

import pymorphy3
from typing import List, Dict, Tuple, Set, Optional
from dataclasses import dataclass, field


# ══════════════════════════════════════════════
# SUFFIX DEFINITIONS (Russian — MVP)
# ══════════════════════════════════════════════

SUFFIXES_RU = {
    # Type A: Depth / Wildcard — always useful
    "A": [
        {"val": "*", "label": "wildcard"},
        {"val": " *", "label": "double_space"},  # deep trigger
        {"val": "_*", "label": "underscore"},     # compound terms
        {"val": "- *", "label": "hyphen"},        # specs/models
        {"val": ".*", "label": "dot"},            # file extensions, abbreviations
        {"val": "? *", "label": "question_mark"}, # conversational long-tail
    ],

    # Type B: Prepositions — contextual
    "B": [
        {"val": "в *", "label": "prep_v"},
        {"val": "на *", "label": "prep_na"},
        {"val": "для *", "label": "prep_dlya"},
        {"val": "с *", "label": "prep_s"},
        {"val": "от *", "label": "prep_ot"},
        {"val": "под *", "label": "prep_pod"},
        {"val": "из *", "label": "prep_iz"},
        {"val": "без *", "label": "prep_bez"},
    ],

    # Type C: Questions — informational intent
    "C": [
        {"val": "как *", "label": "q_kak"},
        {"val": "какой *", "label": "q_kakoy"},
        {"val": "где *", "label": "q_gde"},
        {"val": "сколько *", "label": "q_skolko"},
        {"val": "почему *", "label": "q_pochemu"},
    ],

    # Type D: Finalizers — transaction / reputation
    "D": [
        {"val": "купить *", "label": "fin_kupit"},
        {"val": "цена *", "label": "fin_tsena"},
        {"val": "отзывы *", "label": "fin_otzyvy"},
        {"val": "обзор *", "label": "fin_obzor"},
        {"val": "сравнение *", "label": "fin_sravnenie"},
        {"val": "неисправности *", "label": "fin_neispravnosti"},
        {"val": "характеристики *", "label": "fin_harakteristiki"},
        {"val": "аналоги *", "label": "fin_analogi"},
        {"val": "или *", "label": "fin_ili"},
        {"val": "vs *", "label": "fin_vs"},
        {"val": "вместо *", "label": "fin_vmesto"},
        {"val": "форум *", "label": "fin_forum"},
    ],

    # Numeric (subtype of A, always priority 1)
    "A_num": [
        {"val": f"* {i}", "label": f"num_{i}"} for i in range(10)
    ],
}


# ══════════════════════════════════════════════
# MARKER WORD SETS (for Q, T detection)
# ══════════════════════════════════════════════

Q_WORDS_RU = {"как", "какой", "какая", "какое", "какие", "где", "сколько", "почему",
              "зачем", "когда", "куда", "откуда", "чей", "чья", "чьё", "чьи",
              "который", "которая", "которое", "которые"}

T_ROOTS_RU = {"купить", "купи", "куплю", "покупка", "покупать",
              "цена", "цену", "ценой", "ценах", "ценам",
              "стоимость", "стоит",
              "заказать", "заказ", "заказывать",
              "прайс", "недорого", "дешево", "дёшево", "скидка", "акция"}

PREP_SET_RU = {"в", "во", "на", "для", "с", "со", "от", "под", "из", "без", "над",
               "за", "по", "до", "при", "между", "через", "про", "об", "о", "к", "ко", "у"}


# ══════════════════════════════════════════════
# PRIORITY MATRIX
# ══════════════════════════════════════════════

# Format: MATRIX[marker_name] = {"A": pri, "B": pri, "C": pri, "D": pri}
PRIORITY_MATRIX = {
    "Q":       {"A": 1, "B": 1, "C": 2, "D": 2},
    "T":       {"A": 1, "B": 1, "C": 2, "D": 1},
    "S":       {"A": 1, "B": 1, "C": 1, "D": 1},
    "L1":      {"A": 1, "B": 1, "C": 1, "D": 1},
    "L2":      {"A": 1, "B": 1, "C": 1, "D": 1},
    "L3":      {"A": 1, "B": 1, "C": 2, "D": 1},
    "L4":      {"A": 1, "B": 2, "C": 2, "D": 2},
    "L5+":     {"A": 1, "B": 2, "C": 2, "D": 2},
    "P":       {"A": 1, "B": 1, "C": 1, "D": 1},
    "DEFAULT": {"A": 1, "B": 1, "C": 1, "D": 1},
}


@dataclass
class SeedAnalysis:
    """Result of seed marker detection"""
    seed: str
    words: List[str]
    word_count: int
    markers: Dict[str, bool] = field(default_factory=dict)
    q_words_found: List[str] = field(default_factory=list)
    t_words_found: List[str] = field(default_factory=list)
    s_words_found: List[str] = field(default_factory=list)
    p_words_found: List[str] = field(default_factory=list)
    l_level: str = "L1"


@dataclass
class SuffixQuery:
    """Single generated query with metadata"""
    query: str
    suffix_val: str
    suffix_label: str
    suffix_type: str  # A, B, C, D
    priority: int     # 1 or 2
    markers: List[str]
    blocked_by: Optional[str] = None  # If self-matched, reason


class SuffixGenerator:
    """
    Generates suffix queries for a seed based on marker detection + priority matrix.
    """

    def __init__(self, lang: str = "ru"):
        self.lang = lang
        self.morph = pymorphy3.MorphAnalyzer(lang=lang)
        self.suffixes = SUFFIXES_RU  # MVP: only Russian

    def analyze_seed(self, seed: str) -> SeedAnalysis:
        """Detect all markers in the seed"""
        seed_lower = seed.lower().strip()
        words = seed_lower.split()
        analysis = SeedAnalysis(
            seed=seed_lower,
            words=words,
            word_count=len(words),
        )

        # ── L-marker (length) ──
        if len(words) >= 6:
            analysis.l_level = "L6+"
        elif len(words) >= 5:
            analysis.l_level = "L5+"
        elif len(words) == 4:
            analysis.l_level = "L4"
        elif len(words) == 3:
            analysis.l_level = "L3"
        elif len(words) == 2:
            analysis.l_level = "L2"
        else:
            analysis.l_level = "L1"

        # ── Q-marker (question words) ──
        for w in words:
            if w in Q_WORDS_RU:
                analysis.q_words_found.append(w)
        analysis.markers["Q"] = len(analysis.q_words_found) > 0

        # ── T-marker (transaction words) ──
        for w in words:
            # Check exact match first
            if w in T_ROOTS_RU:
                analysis.t_words_found.append(w)
                continue
            # Check lemma
            parsed = self.morph.parse(w)
            if parsed:
                lemma = parsed[0].normal_form
                if lemma in T_ROOTS_RU:
                    analysis.t_words_found.append(w)

        analysis.markers["T"] = len(analysis.t_words_found) > 0

        # ── P-marker (prepositions in seed) ──
        for w in words:
            if w in PREP_SET_RU:
                analysis.p_words_found.append(w)
        analysis.markers["P"] = len(analysis.p_words_found) > 0

        # ── S-marker (service/deverbal nouns) ──
        for w in words:
            if w in PREP_SET_RU or w in Q_WORDS_RU:
                continue  # skip function words
            if self._is_service_word(w):
                analysis.s_words_found.append(w)
        analysis.markers["S"] = len(analysis.s_words_found) > 0

        return analysis

    def _is_service_word(self, word: str) -> bool:
        """
        Detect if a word is an ACTION/SERVICE noun (deverbal from transitive verb).
        
        Must detect: ремонт, доставка, аренда, обучение, замена, установка, сварка
        Must NOT detect: пылесос (instrument), цветы (object), москва (place), нимесил (name)
        
        Algorithm: Only check words with known deverbal suffixes.
        This avoids false positives like "пылесос" → "пылесосить".
        """
        parsed_list = self.morph.parse(word)
        if not parsed_list:
            return False

        parsed = parsed_list[0]

        # Must be a noun with reasonable confidence
        if parsed.tag.POS != "NOUN":
            return False
        # If pymorphy3 isn't confident it's a noun, skip
        if parsed.score < 0.3:
            return False

        normal = parsed.normal_form

        # Skip short words (< 4 chars rarely are action nouns)
        if len(normal) < 4:
            return False

        # ── Approach 1: Check own lexeme for verb forms ──
        # This works for words like "покупка" whose lexeme includes "покупать"
        for form in parsed.lexeme:
            if form.tag.POS in ("VERB", "INFN"):
                if "tran" in form.tag:
                    return True

        # ── Approach 2: Suffix-gated verb lookup ──
        # ONLY try verb candidates if the noun has a known deverbal suffix.
        # This prevents "пылесос" from triggering via "пылесосить".
        
        verb_candidates = []
        
        if normal.endswith("ка") and len(normal) > 4:
            # доставка → доставить/доставлять, сварка → сварить
            stem = normal[:-2]
            if len(stem) >= 3:
                verb_candidates.extend([stem + "ить", stem + "ять", stem + "лять", stem + "ать", stem + "ивать"])
        elif normal.endswith("ция") or normal.endswith("зия"):
            # дезинфекция → дезинфицировать, инсталляция → инсталлировать
            stem = normal[:-3]
            if len(stem) >= 3:
                verb_candidates.extend([stem + "цировать", stem + "ировать", stem + "овать"])
        elif normal.endswith("ение") or normal.endswith("ание") or normal.endswith("ание"):
            # обучение → обучить, обслуживание → обслуживать
            stem = normal[:-4]
            if len(stem) >= 3:
                verb_candidates.extend([stem + "ить", stem + "ать", stem + "ять", stem + "ивать"])
        elif normal.endswith("ние") and len(normal) > 5:
            # хранение → хранить (but careful: здание ≠ здать)
            stem = normal[:-3]
            if len(stem) >= 3:
                verb_candidates.extend([stem + "ить", stem + "ать", stem + "ять"])
        elif normal.endswith("тие"):
            # взятие → взять
            stem = normal[:-3]
            if len(stem) >= 2:
                verb_candidates.extend([stem + "ять", stem + "ать"])
        elif normal.endswith("мент"):
            # Skip: most -мент words are borrowed (документ, элемент) not deverbal
            pass
        else:
            # Words without deverbal suffixes: only check -ировать form
            # This catches: ремонт → ремонтировать, монтаж → монтировать
            # But won't catch: пылесос → пылесосить (different pattern)
            if len(normal) >= 5:
                verb_candidates.append(normal + "ировать")

        for vc in verb_candidates:
            vc_parsed = self.morph.parse(vc)
            if vc_parsed:
                for p in vc_parsed:
                    # Score >= 0.9 = real dictionary word (not pymorphy3 guess)
                    # pymorphy3 gives ~0.5 for any "X+ировать" nonsense
                    if p.tag.POS in ("VERB", "INFN") and p.score >= 0.9:
                        if "tran" in p.tag:
                            return True

        return False

    def generate(self, seed: str, include_numbers: bool = False) -> Tuple[SeedAnalysis, List[SuffixQuery]]:
        """
        Main method: analyze seed → generate suffix queries with priorities.
        Returns (analysis, queries).
        """
        analysis = self.analyze_seed(seed)
        seed_lower = seed.lower().strip()

        # L6+: only wildcards
        if analysis.l_level == "L6+":
            queries = []
            for s in self.suffixes["A"]:
                q = f"{seed_lower} {s['val']}".strip()
                queries.append(SuffixQuery(
                    query=q, suffix_val=s["val"], suffix_label=s["label"],
                    suffix_type="A", priority=1, markers=[analysis.l_level]
                ))
            return analysis, queries

        # Collect active markers for matrix lookup
        active_markers = []
        if analysis.markers.get("Q"):
            active_markers.append("Q")
        if analysis.markers.get("T"):
            active_markers.append("T")
        if analysis.markers.get("S"):
            active_markers.append("S")
        # P-marker excluded from priority calc — only used for self-match

        # L-marker
        l_key = analysis.l_level
        if l_key in ("L5+", "L6+"):
            l_matrix_key = "L5+"
        elif l_key in PRIORITY_MATRIX:
            l_matrix_key = l_key
        else:
            l_matrix_key = "DEFAULT"
        active_markers.append(l_matrix_key)

        # NOTE: P-marker is NOT added to active_markers for priority calc.
        # It only affects self-match blocking (handled in _check_self_match).
        # Adding P to min() would rescue priorities that L/Q/T correctly downgraded.

        # If no markers at all, use DEFAULT
        if not active_markers:
            active_markers = ["DEFAULT"]

        # Build set of seed words and lemmas for self-match
        seed_words = set(analysis.words)
        seed_lemmas = set()
        for w in analysis.words:
            p = self.morph.parse(w)
            if p:
                seed_lemmas.add(p[0].normal_form)

        results = []

        # Process each suffix type
        for stype in ["A", "B", "C", "D"]:
            suffix_list = self.suffixes.get(stype, [])

            # Calculate priority using min() rule
            priority = self._calc_priority(stype, active_markers)

            for s in suffix_list:
                suffix_val = s["val"]
                suffix_label = s["label"]

                # ── Self-Match Check ──
                blocked = self._check_self_match(suffix_val, seed_words, seed_lemmas, analysis)
                if blocked:
                    results.append(SuffixQuery(
                        query=f"{seed_lower} {suffix_val}".strip(),
                        suffix_val=suffix_val,
                        suffix_label=suffix_label,
                        suffix_type=stype,
                        priority=0,  # 0 = blocked
                        markers=[m for m in active_markers],
                        blocked_by=blocked,
                    ))
                    continue

                query_str = f"{seed_lower} {suffix_val}".strip()

                results.append(SuffixQuery(
                    query=query_str,
                    suffix_val=suffix_val,
                    suffix_label=suffix_label,
                    suffix_type=stype,
                    priority=priority,
                    markers=[m for m in active_markers],
                ))

        # Numeric suffixes (always priority 1, part of type A)
        if include_numbers:
            for s in self.suffixes.get("A_num", []):
                query_str = f"{seed_lower} {s['val']}".strip()
                results.append(SuffixQuery(
                    query=query_str,
                    suffix_val=s["val"],
                    suffix_label=s["label"],
                    suffix_type="A",
                    priority=1,
                    markers=[m for m in active_markers],
                ))

        # Experimental mechanics always get priority 2 (deep dig)
        experimental_labels = {"double_space", "underscore", "hyphen", "dot", "question_mark"}
        for r in results:
            if r.suffix_label in experimental_labels and r.priority == 1:
                r.priority = 2

        return analysis, results

    def _calc_priority(self, suffix_type: str, active_markers: List[str]) -> int:
        """
        Calculate priority for a suffix type using min() rule across all active markers.
        min(1, 2, 1) = 1 → suffix goes to Echelon 1.
        """
        priorities = []
        for marker in active_markers:
            matrix_row = PRIORITY_MATRIX.get(marker, PRIORITY_MATRIX["DEFAULT"])
            pri = matrix_row.get(suffix_type, 1)
            priorities.append(pri)

        return min(priorities) if priorities else 1

    def _check_self_match(self, suffix_val: str, seed_words: Set[str],
                          seed_lemmas: Set[str], analysis: SeedAnalysis) -> Optional[str]:
        """
        Self-Match filter:
        1. Token-level: if suffix keyword already in seed → block
        2. P-level: if suffix preposition already in seed → block
        Returns reason string if blocked, None if OK.
        """
        # Extract first word from suffix (before *)
        suffix_parts = suffix_val.replace("*", "").strip().split()
        if not suffix_parts:
            return None  # pure wildcard — never blocked

        suffix_keyword = suffix_parts[0].lower()

        # P-level: preposition self-match
        if suffix_keyword in PREP_SET_RU:
            if suffix_keyword in analysis.p_words_found:
                return f"prep_self_match:{suffix_keyword}"

        # Token-level: word or lemma already in seed
        if suffix_keyword in seed_words:
            return f"word_self_match:{suffix_keyword}"

        # Lemma check
        suffix_lemma_parsed = self.morph.parse(suffix_keyword)
        if suffix_lemma_parsed:
            suffix_lemma = suffix_lemma_parsed[0].normal_form
            if suffix_lemma in seed_lemmas:
                return f"lemma_self_match:{suffix_keyword}→{suffix_lemma}"

        return None

    def get_queries_by_priority(self, queries: List[SuffixQuery], priority: int) -> List[SuffixQuery]:
        """Filter queries by priority level"""
        return [q for q in queries if q.priority == priority]

    def get_active_queries(self, queries: List[SuffixQuery]) -> List[SuffixQuery]:
        """Get all non-blocked queries (priority > 0)"""
        return [q for q in queries if q.priority > 0]

    def summary(self, analysis: SeedAnalysis, queries: List[SuffixQuery]) -> Dict:
        """Generate summary statistics for tracer"""
        active = [q for q in queries if q.priority > 0]
        blocked = [q for q in queries if q.priority == 0]
        p1 = [q for q in active if q.priority == 1]
        p2 = [q for q in active if q.priority == 2]

        by_type = {}
        for stype in ["A", "B", "C", "D"]:
            type_qs = [q for q in active if q.suffix_type == stype]
            by_type[stype] = {
                "total": len(type_qs),
                "p1": len([q for q in type_qs if q.priority == 1]),
                "p2": len([q for q in type_qs if q.priority == 2]),
            }

        return {
            "seed": analysis.seed,
            "word_count": analysis.word_count,
            "l_level": analysis.l_level,
            "markers": {k: v for k, v in analysis.markers.items() if v},
            "marker_details": {
                "Q": analysis.q_words_found,
                "T": analysis.t_words_found,
                "S": analysis.s_words_found,
                "P": analysis.p_words_found,
            },
            "total_generated": len(queries),
            "active": len(active),
            "blocked": len(blocked),
            "priority_1": len(p1),
            "priority_2": len(p2),
            "by_type": by_type,
            "blocked_details": [
                {"suffix": q.suffix_val, "reason": q.blocked_by}
                for q in blocked
            ],
        }
