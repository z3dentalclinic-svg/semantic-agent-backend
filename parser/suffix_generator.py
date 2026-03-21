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
    # Type A: Symbols — 2 cluster representatives (proven on 4 datasets)
    # UA-cluster: `:` → Kyiv/Kharkiv/Odesa/Allo/Comfy results
    # RU-cluster: `&` → SPb/Moscow/DNS/Novosibirsk results
    # All 16 UA-symbols are identical (exclusive=0), same for 22 RU-symbols.
    # Only 1 representative per cluster needed.
    # v1+v2 only (v3/v4 dropped — 0-3 unique results).
    "A_ua": [
        {"val": ":", "label": "sym_ua"},   # UA-cluster representative
    ],
    "A_ru": [
        {"val": "&", "label": "sym_ru"},   # RU-cluster representative
    ],

    # Type B: Prepositions — contextual
    # v1+v2+v3 only (v4 dropped — 0 results everywhere)
    "B": [
        {"val": "в *",    "label": "prep_v"},
        {"val": "на *",   "label": "prep_na"},
        {"val": "для *",  "label": "prep_dlya"},
        {"val": "с *",    "label": "prep_s"},
        {"val": "от *",   "label": "prep_ot"},
        {"val": "под *",  "label": "prep_pod"},
        {"val": "из *",   "label": "prep_iz"},
        {"val": "без *",  "label": "prep_bez"},
    ],

    # Type C: Questions — informational intent
    # v1+v2+v3 only
    "C": [
        {"val": "как *",     "label": "q_kak"},
        {"val": "какой *",   "label": "q_kakoy"},
        {"val": "где *",     "label": "q_gde"},
        {"val": "сколько *", "label": "q_skolko"},
        {"val": "почему *",  "label": "q_pochemu"},
    ],

    # Type D: Finalizers — transaction / reputation
    # v1+v2+v3 only
    # Removed: неисправности * (stable garbage on all 3 datasets)
    # Added:   и * (union, gives exclusive results like или *)
    "D": [
        {"val": "купить *",        "label": "fin_kupit"},
        {"val": "цена *",          "label": "fin_tsena"},
        {"val": "отзывы *",        "label": "fin_otzyvy"},
        {"val": "обзор *",         "label": "fin_obzor"},
        {"val": "сравнение *",     "label": "fin_sravnenie"},
        {"val": "характеристики *","label": "fin_harakteristiki"},
        {"val": "аналоги *",       "label": "fin_analogi"},
        {"val": "или *",           "label": "fin_ili"},
        {"val": "и *",             "label": "fin_i"},
        {"val": "vs *",            "label": "fin_vs"},
        {"val": "вместо *",        "label": "fin_vmesto"},
        {"val": "форум *",         "label": "fin_forum"},
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
# LETTER SWEEP — full commercial alphabet
# Removed: ъ ы ь (never start words)
#          в с   (covered by Type B prepositions)
#          и     (covered by Type D финализатор)
#          ё     (almost no commercial queries)
# Result: 26 letters
# ══════════════════════════════════════════════
LETTER_SWEEP_RU = list("абгдежзйклмнопртуфхцчшщэюя")


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
    blocked_by: Optional[str] = None   # If self-matched, reason
    cp_override: Optional[int] = None  # explicit cursor position; None = auto (len(query))
    variant: Optional[str] = None      # v1/v2/v3/v4 for tracer


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

    def generate(self, seed: str, include_numbers: bool = False, include_letters: bool = False,
                 region: str = "ua") -> Tuple[SeedAnalysis, List[SuffixQuery]]:
        """
        Main method: analyze seed → generate suffix queries with priorities.
        Returns (analysis, queries).

        Args:
            region: "ua" → use A_ua (:), "ru" → use A_ru (&), "all" → both
        """
        analysis = self.analyze_seed(seed)
        seed_lower = seed.lower().strip()

        # Auto-detect numeric suffixes from seed
        if any(c.isdigit() for c in seed_lower):
            include_numbers = True

        # L6+: only symbols (A_ua + A_ru), v1 only
        if analysis.l_level == "L6+":
            queries = []
            for stype in ["A_ua", "A_ru"]:
                for s in self.suffixes.get(stype, []):
                    q = f"{seed_lower} {s['val']}".strip()
                    queries.append(SuffixQuery(
                        query=q, suffix_val=s["val"], suffix_label=s["label"],
                        suffix_type=stype, priority=1, markers=[analysis.l_level]
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

        def expand_type_a(seed_lower, suffix_val, suffix_label, priority, markers, stype="A"):
            """
            Generate cp variants for suffix queries.
            v1: сид символ   cp = конец строки (после символа)
            v2: сид символ   cp = перед символом
            v3: сид символ   cp = после пробела между сидом и символом

            Type A:     v1 + v2       (v3/v4 дают 0-3 уникальных — убраны)
            Type B/C/D: v1 + v2 + v3  (v4 = 0 везде — убран)
            """
            base = f"{seed_lower} {suffix_val}"
            seed_end = len(seed_lower)           # позиция конца сида
            space_pos = seed_end                 # позиция пробела = len(seed_lower)
            sym_start = seed_end + 1             # позиция начала символа
            sym_end = len(base)                  # позиция конца символа (конец строки)

            all_variants = [
                (base, sym_end,   "v1"),  # курсор в конце, после символа
                (base, sym_start, "v2"),  # курсор перед символом
                (base, space_pos, "v3"),  # курсор на месте пробела
            ]
            # Type A: v1+v2 only; Type B/C/D: v1+v2+v3
            max_v = 2 if stype in ("A", "A_ua", "A_ru") else 3
            variants = all_variants[:max_v]

            out = []
            for q, cp, vname in variants:
                out.append(SuffixQuery(
                    query=q,
                    suffix_val=suffix_val,
                    suffix_label=f"{suffix_label}_{vname}",
                    suffix_type=stype,
                    priority=priority,
                    markers=list(markers),
                    cp_override=cp,
                    variant=vname,
                ))
            return out

        # Process Type A: A_ua and/or A_ru based on region
        a_types = []
        if region in ("ua", "all"):
            a_types.append("A_ua")
        if region in ("ru", "all"):
            a_types.append("A_ru")
        if not a_types:
            a_types = ["A_ua"]  # fallback

        for stype in a_types:
            priority = self._calc_priority("A", active_markers)
            for s in self.suffixes.get(stype, []):
                suffix_val = s["val"]
                suffix_label = s["label"]
                results.extend(expand_type_a(seed_lower, suffix_val, suffix_label, priority, active_markers, stype=stype))

        # Process Types B, C, D
        for stype in ["B", "C", "D"]:
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

                # All types: expand into 4 cp variants for full testing
                # After tracer analysis — keep best variant per suffix, drop rest
                results.extend(expand_type_a(seed_lower, suffix_val, suffix_label, priority, active_markers, stype=stype))

                # Type B trailing space variant — "сид в " (без wildcard)
                # Даёт кластер гео-расширений (районы, локации) которые "в *" не вытаскивает.
                # Пример: "курсы английского киев в " → "в центре", "в оболони", "на троещине"
                # Только для предлогов "в" и "на" — остальные предлоги не дают гео-кластер.
                # Блокируем если последнее слово сида — T-маркер (цена, стоимость...):
                # "ремонт телефонов цена в " — Google игнорирует, бессмысленный запрос.
                last_word_is_t = analysis.words[-1] in T_ROOTS_RU if analysis.words else False
                if stype == "B" and suffix_val in ("в *", "на *") and not last_word_is_t:
                    prep = suffix_val.replace(" *", "")  # "в" или "на"
                    trail_q = f"{seed_lower} {prep} "
                    trail_cp = len(trail_q)
                    results.append(SuffixQuery(
                        query=trail_q,
                        suffix_val=prep,
                        suffix_label=f"prep_{prep}_trail",
                        suffix_type="B",
                        priority=priority,
                        markers=list(active_markers),
                        cp_override=trail_cp,
                        variant="trail",
                    ))

        # Numeric suffixes (always priority 1, part of type A)
        if include_numbers:
            for s in self.suffixes.get("A_num", []):
                results.extend(expand_type_a(seed_lower, s["val"], s["label"], 1, active_markers))

        # Letter sweep (type E) — 21 structures × 10 letters
        if include_letters:
            for letter in LETTER_SWEEP_RU:
                results.extend(self._build_letter_structures(seed_lower, letter))

        # Double-space suffix always gets +1 (but max 2)
        for r in results:
            if r.suffix_label == "double_space" and r.priority == 1:
                r.priority = 2

        return analysis, results

    def _build_letter_structures(self, seed_lower: str, letter: str) -> List["SuffixQuery"]:
        """
        Build 14 test structures for a single letter (was 21, removed 7 zero-result structures).

        Removed (0 unique on all datasets):
          col_B, col_Lwc, col_wcL, hyp_B, Lwc_cpEnd, wcB_cpEnd, wcB_cpStar

        Kept (14):
          plain, trail, sandwich, wcB_cpMid,
          Lwc_cpAL, Lwc_cpBL, col_B_trail, L_col, hyp_B_trail, hyp_Lwc, hyp_wcL, L_hyp
        """
        s = seed_lower
        L = letter
        out = []

        def sq(query: str, cp: int, struct_name: str) -> "SuffixQuery":
            return SuffixQuery(
                query=query,
                suffix_val=L,
                suffix_label=f"{L}_{struct_name}",
                suffix_type="E",
                priority=1,
                markers=["letter_sweep"],
                cp_override=cp,
                variant=struct_name,
            )

        # ── D1: буква без символов ───────────────────────────────────────
        # 1. сид а  (cp = конец)
        q = f"{s} {L}"
        out.append(sq(q, len(q), "plain"))

        # 1b. сид а  (cp не передаётся — точная копия старого алфавитного перебора)
        # Старый main.py: params = {"q": query, "client": "firefox"} без cp
        # Даёт ~10% уникальных ключей которые plain с cp не находит
        out.append(sq(q, -1, "plain_nocp"))

        # 2. сид а  (+ trailing space)
        q = f"{s} {L} "
        out.append(sq(q, len(q), "trail"))

        # ── D2: буква + wildcard ─────────────────────────────────────────
        # УДАЛЕНО: wcB_trail (сид * а ) — 92-94% мусор на 3 датасетах
        # УДАЛЕНО: Lwc_trail (сид а * ) — 92-93% мусор на 3 датасетах

        # 5. сид * а *  (sandwich, cp = конец)
        q = f"{s} * {L} *"
        out.append(sq(q, len(q), "sandwich"))

        # ── D3: cp варианты ──────────────────────────────────────────────
        # wcB_cpStar убран (0 unique). Оставляем wcB_cpMid:
        #   cp между * и буквой (= len("сид * "))
        q = f"{s} * {L}"
        out.append(sq(q, len(s) + 3, "wcB_cpMid"))

        # Lwc cp варианты:
        #   cp после "а " (перед *)
        q = f"{s} {L} *"
        out.append(sq(q, len(s) + 1 + len(L) + 1, "Lwc_cpAL"))
        #   cp перед "а" (после пробела сида)
        out.append(sq(q, len(s) + 1, "Lwc_cpBL"))

        # ── D4: A_local (:) + буква ──────────────────────────────────────
        # col_B убран (0 unique). col_Lwc, col_wcL убраны.
        # 6. сид : а  (+ trailing space)
        q = f"{s} : {L} "
        out.append(sq(q, len(q), "col_B_trail"))

        # 7. сид а :
        q = f"{s} {L} :"
        out.append(sq(q, len(q), "L_col"))

        # ── D5: A_general (-) + буква ────────────────────────────────────
        # hyp_B убран (0 unique).
        # 8. сид - а  (+ trailing space)
        q = f"{s} - {L} "
        out.append(sq(q, len(q), "hyp_B_trail"))

        # 9. сид - а *
        q = f"{s} - {L} *"
        out.append(sq(q, len(q), "hyp_Lwc"))

        # 10. сид - * а
        q = f"{s} - * {L}"
        out.append(sq(q, len(q), "hyp_wcL"))

        # 11. сид а -
        q = f"{s} {L} -"
        out.append(sq(q, len(q), "L_hyp"))

        return out  # 14 entries per letter

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
        Self-Match filter — blocks suffix if any meaningful word overlaps with seed.
        1. P-level: if suffix preposition already in seed → block
        2. Universal word check: any non-wildcard word from suffix in seed → block
           (covers L-marker: "купить айфон про макс" + "купить *" → blocked)
        Returns reason string if blocked, None if OK.
        """
        # Extract all non-wildcard words from suffix
        suffix_parts = suffix_val.replace("*", "").strip().split()
        if not suffix_parts:
            return None  # pure wildcard — never blocked

        for suffix_keyword in suffix_parts:
            suffix_keyword = suffix_keyword.lower()

            # P-level: preposition self-match
            if suffix_keyword in PREP_SET_RU:
                if suffix_keyword in analysis.p_words_found:
                    return f"prep_self_match:{suffix_keyword}"
                continue  # preposition not in seed — OK, skip further checks

            # Token-level: word already in seed
            if suffix_keyword in seed_words:
                return f"word_self_match:{suffix_keyword}"

            # Lemma-level: lemma already in seed
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
        for stype in ["A", "B", "C", "D", "E"]:
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
