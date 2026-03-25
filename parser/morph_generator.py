"""
Morph Generator v3.0 — Самодостаточный файл, не зависит от suffix_generator.py.

Содержит:
  - Все суффиксные константы (A/B/C/D/E) — полностью раскомментированы
  - MorphSuffixGenerator — внутренний генератор суффиксов для морфологии
  - MorphGenerator — склоняет первое существительное сида, генерирует MorphQuery
  - CASES_RU, MorphSeedAnalysis, MorphQuery — все датаклассы

Изменения относительно v2.0:
  - Убран импорт SuffixGenerator из suffix_generator.py
  - Все структуры Type E раскомментированы (13 структур вместо 4)
  - suffix_generator.py больше не влияет на морф-парсер

Architecture:
  morph_generator.py (этот файл) ← morph_parser.py
  suffix_generator.py             ← suffix_parser.py   (независимо)
"""

import pymorphy3
import re
from typing import List, Dict, Tuple, Set, Optional
from dataclasses import dataclass, field


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
    "stem_cut":   ("nomn", "sing", "Усечённая лемма"),
    # ── mixed experiment ──────────────────────────────────────────────────
    "typo_w1":    ("nomn", "sing", "Удвоение первой буквы слова 1"),
    "cyr2lat_w1": ("nomn", "sing", "Замена кириллицы на латиницу в слове 1"),
    "cyr2lat_w2": ("nomn", "sing", "Замена кириллицы на латиницу в слове 2"),
    # ── gemini experiment ─────────────────────────────────────────────────
    "double_space": ("nomn", "sing", "Двойной пробел после wildcard"),
    "ds_it":        ("nomn", "sing", "Параметр ds=it (информационный слой)"),
    "client_yt":    ("nomn", "sing", "Client=youtube"),
    "cp_one":       ("nomn", "sing", "Курсор cp=1 на маске"),
    # ── SEP (Suffix-Ending-Position) — стем + триггер ────────────────────
    "sep_а":  ("nomn", "sing", "SEP триггер: -а"),
    "sep_у":  ("nomn", "sing", "SEP триггер: -у"),
    "sep_е":  ("nomn", "sing", "SEP триггер: -е"),
    "sep_ы":  ("nomn", "sing", "SEP триггер: -ы"),
    "sep_и":  ("nomn", "sing", "SEP триггер: -и"),
    "sep_ов": ("nomn", "sing", "SEP триггер: -ов"),
    "sep_ом": ("nomn", "sing", "SEP триггер: -ом"),
    # ── suffix brute-force experiment ─────────────────────────────────────
    "brute_и":  ("nomn", "sing", "Brute suffix: -и"),
    "brute_а":  ("nomn", "sing", "Brute suffix: -а"),
    "brute_е":  ("nomn", "sing", "Brute suffix: -е"),
    "brute_у":  ("nomn", "sing", "Brute suffix: -у"),
    "brute_ы":  ("nomn", "sing", "Brute suffix: -ы"),
    "brute_ом": ("nomn", "sing", "Brute suffix: -ом (творительный муж.р)"),
    "brute_ей": ("nomn", "sing", "Brute suffix: -ей (творительный жен.р)"),
    "brute_о":  ("nomn", "sing", "Brute suffix: -о"),
    "brute_ю":  ("nomn", "sing", "Brute suffix: -ю (винительный жен.р)"),
}


# ══════════════════════════════════════════════
# SUFFIX DEFINITIONS — внутренние для морфологии
# Полностью независимы от suffix_generator.py
# ══════════════════════════════════════════════

MORPH_SUFFIXES_RU = {
    # Type A: Symbols
    "A_ua": [
        {"val": ":", "label": "sym_ua"},
    ],
    "A_ru": [
        {"val": "&", "label": "sym_ru"},
    ],

    # Type B: Prepositions
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

    # Type C: Questions
    "C": [
        {"val": "как *",     "label": "q_kak"},
        {"val": "какой *",   "label": "q_kakoy"},
        {"val": "где *",     "label": "q_gde"},
        {"val": "сколько *", "label": "q_skolko"},
        {"val": "почему *",  "label": "q_pochemu"},
    ],

    # Type D: Finalizers
    "D": [
        {"val": "купить *",         "label": "fin_kupit"},
        {"val": "цена *",           "label": "fin_tsena"},
        {"val": "отзывы *",         "label": "fin_otzyvy"},
        {"val": "обзор *",          "label": "fin_obzor"},
        {"val": "сравнение *",      "label": "fin_sravnenie"},
        {"val": "характеристики *", "label": "fin_harakteristiki"},
        {"val": "аналоги *",        "label": "fin_analogi"},
        {"val": "или *",            "label": "fin_ili"},
        {"val": "и *",              "label": "fin_i"},
        {"val": "vs *",             "label": "fin_vs"},
        {"val": "вместо *",         "label": "fin_vmesto"},
        {"val": "форум *",          "label": "fin_forum"},
    ],

    # Numeric
    "A_num": [
        {"val": f"* {i}", "label": f"num_{i}"} for i in range(10)
    ],
}

# ── Marker word sets ──────────────────────────────────────────────────────────

MORPH_Q_WORDS = {"как", "какой", "какая", "какое", "какие", "где", "сколько",
                 "почему", "зачем", "когда", "куда", "откуда", "чей", "чья",
                 "чьё", "чьи", "который", "которая", "которое", "которые"}

MORPH_T_ROOTS = {"купить", "купи", "куплю", "покупка", "покупать",
                 "цена", "цену", "ценой", "ценах", "ценам",
                 "стоимость", "стоит",
                 "заказать", "заказ", "заказывать",
                 "прайс", "недорого", "дешево", "дёшево", "скидка", "акция"}

MORPH_PREP_SET = {"в", "во", "на", "для", "с", "со", "от", "под", "из", "без",
                  "над", "за", "по", "до", "при", "между", "через", "про",
                  "об", "о", "к", "ко", "у"}

# ── Letter sweep — все 26 букв ────────────────────────────────────────────────
MORPH_LETTER_SWEEP = list("кпмнрдтбгзжлоауфхцчшйеэюящ")

# ── SEP: срезаем конечные гласные, клеим триггер ─────────────────────────────
_SEP_VOWELS = set("аеёиоуыэюя")
SEP_TRIGGERS = ["а", "у", "е", "ы", "и", "ов", "ом"]

# ── Priority matrix ───────────────────────────────────────────────────────────

MORPH_PRIORITY_MATRIX = {
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


# ══════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════

@dataclass
class MorphSeedAnalysis:
    """Result of morphological seed analysis."""
    original_seed: str
    original_noun: str
    original_noun_idx: int
    original_lemma: str
    case_variants: Dict[str, str]
    skipped_cases: List[str]


@dataclass
class MorphQuery:
    """Single generated query: full suffix metadata + case metadata."""
    case_label: str
    case_display: str
    seed_variant: str
    query: str
    suffix_val: str
    suffix_label: str
    suffix_type: str
    priority: int
    cp_override: Optional[int] = None
    variant: Optional[str] = None
    blocked_by: Optional[str] = None
    ua_filter: Optional[str] = None
    extra_params: Dict = field(default_factory=dict)
    client_override: Optional[str] = None


# ── Internal seed analysis dataclass (used by MorphSuffixGenerator) ──────────

@dataclass
class _SeedAnalysis:
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
class _SuffixQuery:
    query: str
    suffix_val: str
    suffix_label: str
    suffix_type: str
    priority: int
    markers: List[str]
    blocked_by: Optional[str] = None
    cp_override: Optional[int] = None
    variant: Optional[str] = None


# ══════════════════════════════════════════════
# MORPH SUFFIX GENERATOR
# Внутренний — независим от suffix_generator.py
# Type E: все 13 структур активны
# ══════════════════════════════════════════════

class MorphSuffixGenerator:

    def __init__(self, lang: str = "ru"):
        self.lang = lang
        self.morph = pymorphy3.MorphAnalyzer(lang=lang)
        self.suffixes = MORPH_SUFFIXES_RU

    def analyze_seed(self, seed: str) -> _SeedAnalysis:
        seed_lower = seed.lower().strip()
        words = seed_lower.split()
        analysis = _SeedAnalysis(seed=seed_lower, words=words, word_count=len(words))

        if len(words) >= 6:   analysis.l_level = "L6+"
        elif len(words) >= 5: analysis.l_level = "L5+"
        elif len(words) == 4: analysis.l_level = "L4"
        elif len(words) == 3: analysis.l_level = "L3"
        elif len(words) == 2: analysis.l_level = "L2"
        else:                  analysis.l_level = "L1"

        for w in words:
            if w in MORPH_Q_WORDS:
                analysis.q_words_found.append(w)
        analysis.markers["Q"] = len(analysis.q_words_found) > 0

        for w in words:
            if w in MORPH_T_ROOTS:
                analysis.t_words_found.append(w)
                continue
            parsed = self.morph.parse(w)
            if parsed and parsed[0].normal_form in MORPH_T_ROOTS:
                analysis.t_words_found.append(w)
        analysis.markers["T"] = len(analysis.t_words_found) > 0

        for w in words:
            if w in MORPH_PREP_SET:
                analysis.p_words_found.append(w)
        analysis.markers["P"] = len(analysis.p_words_found) > 0

        for w in words:
            if w in MORPH_PREP_SET or w in MORPH_Q_WORDS:
                continue
            if self._is_service_word(w):
                analysis.s_words_found.append(w)
        analysis.markers["S"] = len(analysis.s_words_found) > 0

        return analysis

    def _is_service_word(self, word: str) -> bool:
        parsed_list = self.morph.parse(word)
        if not parsed_list:
            return False
        parsed = parsed_list[0]
        if parsed.tag.POS != "NOUN" or parsed.score < 0.3:
            return False
        normal = parsed.normal_form
        if len(normal) < 4:
            return False
        for form in parsed.lexeme:
            if form.tag.POS in ("VERB", "INFN") and "tran" in form.tag:
                return True
        verb_candidates = []
        if normal.endswith("ка") and len(normal) > 4:
            stem = normal[:-2]
            if len(stem) >= 3:
                verb_candidates += [stem + "ить", stem + "ять", stem + "ать",
                                    stem + "ивать", stem + "лять"]
        if normal.endswith("ние") or normal.endswith("ание") or normal.endswith("ение"):
            stem = normal[:-3]
            if len(stem) >= 3:
                verb_candidates += [stem + "ать", stem + "ить", stem + "ять"]
        for vc in verb_candidates:
            p_list = self.morph.parse(vc)
            for p in p_list:
                if p.tag.POS in ("VERB", "INFN") and "tran" in p.tag:
                    if p.normal_form == vc or p.normal_form.startswith(vc[:4]):
                        return True
        return False

    def generate(
        self,
        seed: str,
        include_numbers: bool = False,
        include_letters: bool = True,
        region: str = "ua",
    ) -> Tuple[_SeedAnalysis, List[_SuffixQuery]]:

        analysis = self.analyze_seed(seed)
        seed_lower = seed.lower().strip()

        seed_words: Set[str] = set(seed_lower.split())
        seed_lemmas: Set[str] = set()
        for w in seed_words:
            p = self.morph.parse(w)
            if p:
                seed_lemmas.add(p[0].normal_form)

        active_markers: List[str] = []
        for marker in ["Q", "T", "S", "P"]:
            if analysis.markers.get(marker):
                active_markers.append(marker)
        active_markers.append(analysis.l_level)
        if not active_markers:
            active_markers = ["DEFAULT"]

        results: List[_SuffixQuery] = []

        def make_sq(query, suffix_val, suffix_label, suffix_type, priority,
                    cp=None, variant=None, blocked_by=None) -> _SuffixQuery:
            return _SuffixQuery(
                query=query, suffix_val=suffix_val, suffix_label=suffix_label,
                suffix_type=suffix_type, priority=priority,
                markers=list(active_markers), cp_override=cp,
                variant=variant, blocked_by=blocked_by,
            )

        def expand_type_a(seed_str, val, label, priority, markers):
            out = []
            blocked = self._check_self_match(val, seed_words, seed_lemmas, analysis)
            eff = 0 if blocked else priority
            q = f"{seed_str} {val}"
            out.append(make_sq(q, val, f"{label}_v1", "A", eff,
                               cp=len(q), variant="v1", blocked_by=blocked))
            q2 = f"{val} {seed_str}"
            out.append(make_sq(q2, val, f"{label}_v2", "A", eff,
                               cp=len(q2), variant="v2", blocked_by=blocked))
            return out

        # ── Type A ───────────────────────────────────────────────────────────
        a_priority = self._calc_priority("A", active_markers)
        if region in ("ua", "all"):
            for s in self.suffixes["A_ua"]:
                results.extend(expand_type_a(seed_lower, s["val"], s["label"],
                                             a_priority, active_markers))
        if region in ("ru", "all"):
            for s in self.suffixes["A_ru"]:
                results.extend(expand_type_a(seed_lower, s["val"], s["label"],
                                             a_priority, active_markers))

        # ── Type B ───────────────────────────────────────────────────────────
        b_priority = self._calc_priority("B", active_markers)
        for s in self.suffixes["B"]:
            blocked = self._check_self_match(s["val"], seed_words, seed_lemmas, analysis)
            eff = 0 if blocked else b_priority
            base_label = s["label"]
            val = s["val"]
            for vi in range(1, 4):
                q = f"{seed_lower} {val}"
                results.append(make_sq(q, val, f"{base_label}_v{vi}", "B", eff,
                                       cp=len(q), variant=f"v{vi}", blocked_by=blocked))
            prep_word = val.split()[0]
            q_trail = f"{seed_lower} {prep_word} "
            results.append(make_sq(q_trail, val, f"prep_{prep_word}_trail", "B", eff,
                                   cp=len(q_trail), variant="trail", blocked_by=blocked))

        # ── Type C ───────────────────────────────────────────────────────────
        c_priority = self._calc_priority("C", active_markers)
        for s in self.suffixes["C"]:
            blocked = self._check_self_match(s["val"], seed_words, seed_lemmas, analysis)
            eff = 0 if blocked else c_priority
            val = s["val"]
            base_label = s["label"]
            for vi in range(1, 4):
                q = f"{seed_lower} {val}"
                results.append(make_sq(q, val, f"{base_label}_v{vi}", "C", eff,
                                       cp=len(q), variant=f"v{vi}", blocked_by=blocked))

        # ── Type D ───────────────────────────────────────────────────────────
        d_priority = self._calc_priority("D", active_markers)
        for s in self.suffixes["D"]:
            blocked = self._check_self_match(s["val"], seed_words, seed_lemmas, analysis)
            eff = 0 if blocked else d_priority
            val = s["val"]
            base_label = s["label"]
            for vi in range(1, 4):
                q = f"{seed_lower} {val}"
                results.append(make_sq(q, val, f"{base_label}_v{vi}", "D", eff,
                                       cp=len(q), variant=f"v{vi}", blocked_by=blocked))

        # ── Type A_num ───────────────────────────────────────────────────────
        if include_numbers:
            for s in self.suffixes["A_num"]:
                results.extend(expand_type_a(seed_lower, s["val"], s["label"],
                                             1, active_markers))

        # ── Type E: Letter sweep — все 13 структур ───────────────────────────
        if include_letters:
            for letter in MORPH_LETTER_SWEEP:
                results.extend(self._build_letter_structures(seed_lower, letter))

        return analysis, results

    def _build_letter_structures(self, seed_lower: str, letter: str) -> List[_SuffixQuery]:
        """
        13 структур для одной буквы.
        В suffix_generator.py активны только 4 (plain, trail, sandwich, wcB_cpMid).
        Здесь все раскомментированы.
        """
        s = seed_lower
        L = letter
        out = []

        def sq(query: str, cp: int, struct_name: str) -> _SuffixQuery:
            return _SuffixQuery(
                query=query, suffix_val=L,
                suffix_label=f"{L}_{struct_name}",
                suffix_type="E", priority=1,
                markers=["letter_sweep"],
                cp_override=cp if cp >= 0 else None,
                variant=struct_name,
            )

        # 1. plain: сид а  (cp = конец)
        q = f"{s} {L}"
        out.append(sq(q, len(q), "plain"))

        # 2. plain_nocp: сид а  (cp не передаётся)
        out.append(sq(q, -1, "plain_nocp"))

        # 3. trail: сид а  (+ trailing space)
        q = f"{s} {L} "
        out.append(sq(q, len(q), "trail"))

        # 4. sandwich: сид * а *
        q = f"{s} * {L} *"
        out.append(sq(q, len(q), "sandwich"))

        # 5. wcB_cpMid: сид * а  (cp между * и буквой)
        q = f"{s} * {L}"
        out.append(sq(q, len(s) + 3, "wcB_cpMid"))

        # 6. Lwc_cpAL: сид а *  (cp после "а ")
        q = f"{s} {L} *"
        out.append(sq(q, len(s) + 1 + len(L) + 1, "Lwc_cpAL"))

        # 7. Lwc_cpBL: сид а *  (cp перед "а")
        out.append(sq(q, len(s) + 1, "Lwc_cpBL"))

        # 8. col_B_trail: сид : а  (+ trailing space)
        q = f"{s} : {L} "
        out.append(sq(q, len(q), "col_B_trail"))

        # 9. L_col: сид а :
        q = f"{s} {L} :"
        out.append(sq(q, len(q), "L_col"))

        # 10. hyp_B_trail: сид - а  (+ trailing space)
        q = f"{s} - {L} "
        out.append(sq(q, len(q), "hyp_B_trail"))

        # 11. hyp_Lwc: сид - а *
        q = f"{s} - {L} *"
        out.append(sq(q, len(q), "hyp_Lwc"))

        # 12. hyp_wcL: сид - * а
        q = f"{s} - * {L}"
        out.append(sq(q, len(q), "hyp_wcL"))

        # 13. L_hyp: сид а -
        q = f"{s} {L} -"
        out.append(sq(q, len(q), "L_hyp"))

        return out  # 13 структур

    def _calc_priority(self, suffix_type: str, active_markers: List[str]) -> int:
        priorities = []
        for marker in active_markers:
            matrix_row = MORPH_PRIORITY_MATRIX.get(marker, MORPH_PRIORITY_MATRIX["DEFAULT"])
            priorities.append(matrix_row.get(suffix_type, 1))
        return min(priorities) if priorities else 1

    def _check_self_match(self, suffix_val: str, seed_words: Set[str],
                           seed_lemmas: Set[str], analysis: _SeedAnalysis) -> Optional[str]:
        suffix_parts = suffix_val.replace("*", "").strip().split()
        if not suffix_parts:
            return None
        for suffix_keyword in suffix_parts:
            suffix_keyword = suffix_keyword.lower()
            if suffix_keyword in MORPH_PREP_SET:
                if suffix_keyword in analysis.p_words_found:
                    return f"prep_self_match:{suffix_keyword}"
                continue
            if suffix_keyword in seed_words:
                return f"word_self_match:{suffix_keyword}"
            p = self.morph.parse(suffix_keyword)
            if p and p[0].normal_form in seed_lemmas:
                return f"lemma_self_match:{suffix_keyword}→{p[0].normal_form}"
        return None


# ══════════════════════════════════════════════
# MORPH GENERATOR
# ══════════════════════════════════════════════

class MorphGenerator:
    """
    Generates full suffix map for all unique case variants of the first noun in seed.
    Использует внутренний MorphSuffixGenerator — не зависит от suffix_generator.py.
    """

    def __init__(self, lang: str = "ru", geo_db: dict = None):
        self.lang = lang
        self.geo_db = geo_db or {}
        self.morph = pymorphy3.MorphAnalyzer(lang=lang)
        self.suffix_gen = MorphSuffixGenerator(lang=lang)  # ← внутренний, не SuffixGenerator

    def _is_cyrillic_word(self, word: str) -> bool:
        return bool(re.match(r'^[а-яёА-ЯЁ]+$', word))

    def _find_first_noun(self, words: List[str]) -> Optional[Tuple[int, str, str, object]]:
        for strict in [True, False]:
            for idx, word in enumerate(words):
                if not self._is_cyrillic_word(word):
                    continue
                for p in self.morph.parse(word):
                    if p.tag.POS == 'NOUN':
                        if not strict or p.score >= 0.3:
                            return idx, word, p.normal_form, p
        return None

    def analyze_seed(self, seed: str) -> Optional[MorphSeedAnalysis]:
        words = seed.lower().strip().split()
        if not words:
            return None

        noun_data = self._find_first_noun(words)

        if noun_data is None:
            for idx, word in enumerate(words):
                if not self._is_cyrillic_word(word):
                    continue
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
            idx = next(
                (i for i, w in enumerate(words) if self._is_cyrillic_word(w)),
                len(words) - 1
            )
            word = words[idx]
            parses = self.morph.parse(word)
            if not parses:
                return None
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

        # ── mixed experiment ─────────────────────────────────────────────
        CYR2LAT = {
            'а': 'a', 'е': 'e', 'о': 'o', 'р': 'p', 'с': 'c',
            'у': 'y', 'х': 'x', 'А': 'A', 'Е': 'E', 'О': 'O',
            'Р': 'P', 'С': 'C', 'У': 'Y', 'Х': 'X',
        }

        def mix_cyr_lat(w):
            for i, ch in enumerate(w):
                if ch in CYR2LAT:
                    return w[:i] + CYR2LAT[ch] + w[i+1:]
            return w

        other_idx = 1 if idx == 0 else 0
        has_other = (other_idx < len(words)
                     and other_idx != idx
                     and re.match(r'^[а-яёА-ЯЁ]+$', words[other_idx])
                     and len(words[other_idx]) > 2)

        # typo_w1
        w1_typo = words[idx][0] + words[idx]
        tw1 = words.copy(); tw1[idx] = w1_typo
        v1 = " ".join(tw1)
        if v1 not in seen_variants:
            case_variants["typo_w1"] = v1
            seen_variants.add(v1)

        # cyr2lat_w1
        w1_lat = mix_cyr_lat(words[idx])
        if w1_lat != words[idx]:
            cl1 = words.copy(); cl1[idx] = w1_lat
            vcl1 = " ".join(cl1)
            if vcl1 not in seen_variants:
                case_variants["cyr2lat_w1"] = vcl1
                seen_variants.add(vcl1)

        # cyr2lat_w2
        if has_other:
            w2_lat = mix_cyr_lat(words[other_idx])
            if w2_lat != words[other_idx]:
                cl2 = words.copy(); cl2[other_idx] = w2_lat
                vcl2 = " ".join(cl2)
                if vcl2 not in seen_variants:
                    case_variants["cyr2lat_w2"] = vcl2
                    seen_variants.add(vcl2)

        # gemini experiments
        original = seed.lower().strip()
        for gcase in ("double_space", "ds_it", "client_yt", "cp_one"):
            case_variants[gcase] = original

        # brute-force окончания
        _BRUTE_ENDINGS = ['и', 'а', 'е', 'у', 'ы', 'ом', 'ей', 'о', 'ю']
        _BRUTE_LABELS  = ['brute_и', 'brute_а', 'brute_е', 'brute_у',
                          'brute_ы', 'brute_ом', 'brute_ей', 'brute_о', 'brute_ю']
        noun_word = words[idx]
        noun_stem = noun_word[:-1]
        for blabel, bending in zip(_BRUTE_LABELS, _BRUTE_ENDINGS):
            new_word = noun_stem + bending
            if new_word == noun_word:
                continue
            bwords = words.copy(); bwords[idx] = new_word
            case_variants[blabel] = " ".join(bwords)

        # SEP (Suffix-Ending-Position) — срезаем конечные гласные, клеим триггер
        sep_noun = words[idx]
        sep_stem = sep_noun.rstrip("аеёиоуыэюя")
        if not sep_stem:
            sep_stem = sep_noun  # всё слово — гласные (маловероятно)
        for trigger in SEP_TRIGGERS:
            new_word = sep_stem + trigger
            if new_word == sep_noun:
                continue  # совпадает с оригиналом — пропускаем
            sep_words = words.copy()
            sep_words[idx] = new_word
            case_variants[f"sep_{trigger}"] = " ".join(sep_words)

        return MorphSeedAnalysis(
            original_seed=seed.lower().strip(),
            original_noun=word,
            original_noun_idx=idx,
            original_lemma=lemma,
            case_variants=case_variants,
            skipped_cases=skipped_cases,
        )

    # ── Query generation ───────────────────────────────────────────────────

    PROVEN_TRIPLETS: List[Tuple[str, str, str]] = [
        ("gent_sing",  "wcB_cpMid",   "chrome"),
        ("accs_sing",  "wcB_cpMid",   "chrome"),
        ("nomn_sing",  "plain",       "firefox"),
        ("nomn_sing",  "wcB_cpMid",   "chrome"),
        ("ablt_sing",  "wcB_cpMid",   "chrome"),
        ("nomn_sing",  "q_kak",       "chrome"),
        ("ablt_plur",  "prep_bez",    "chrome"),
        ("gent_sing",  "q_kakoy",     "chrome"),
        ("gent_sing",  "plain",       "firefox"),
        ("nomn_sing",  "plain",       "chrome"),
        ("nomn_sing",  "prep_s",      "chrome"),
        ("datv_plur",  "plain",       "firefox"),
        ("gent_sing",  "trail",       "firefox"),
        ("ablt_sing",  "trail",       "firefox"),
        ("gent_sing",  "q_kak",       "chrome"),
        ("gent_sing",  "plain",       "chrome"),
        ("gent_sing",  "Lwc_cpBL",    "chrome"),
        ("nomn_sing",  "trail",       "firefox"),
        ("nomn_sing",  "fin_i",       "chrome"),
        ("gent_sing",  "prep_na",     "chrome"),
        ("nomn_sing",  "q_skolko",    "chrome"),
        ("nomn_sing",  "q_gde",       "chrome"),
        ("nomn_sing",  "prep_dlya",   "chrome"),
        ("nomn_sing",  "fin_ili",     "chrome"),
        ("loct_sing",  "plain",       "firefox"),
        ("nomn_sing",  "q_kakoy",     "chrome"),
        ("gent_sing",  "prep_bez",    "chrome"),
        ("gent_sing",  "q_pochemu",   "chrome"),
        ("gent_sing",  "prep_ot",     "chrome"),
        ("ablt_sing",  "sym",         "chrome"),
        ("nomn_plur",  "plain",       "firefox"),
        ("ablt_plur",  "wcB_cpMid",   "chrome"),
        ("nomn_sing",  "plain_nocp",  "chrome"),
        ("nomn_plur",  "q_pochemu",   "chrome"),
        ("nomn_sing",  "q_pochemu",   "chrome"),
        ("gent_sing",  "q_skolko",    "chrome"),
        ("gent_plur",  "plain",       "firefox"),
        ("accs_sing",  "plain",       "firefox"),
        ("datv_plur",  "trail",       "firefox"),
        ("accs_sing",  "prep_na",     "chrome"),
        ("gent_sing",  "prep_s",      "chrome"),
        ("gent_sing",  "prep_dlya",   "chrome"),
        ("gent_sing",  "fin_tsena",   "chrome"),
        ("ablt_plur",  "plain",       "chrome"),
        ("ablt_plur",  "prep_na",     "chrome"),
        ("gent_sing",  "sym",         "chrome"),
        ("gent_sing",  "fin_otzyvy",  "chrome"),
        ("gent_sing",  "fin_i",       "chrome"),
        ("nomn_plur",  "plain_nocp",  "firefox"),
        ("datv_sing",  "wcB_cpMid",   "chrome"),
        ("datv_sing",  "trail",       "firefox"),
        ("accs_sing",  "sym",         "firefox"),
        ("datv_plur",  "wcB_cpMid",   "chrome"),
        ("nomn_sing",  "prep_bez",    "chrome"),
        ("ablt_sing",  "prep_s",      "chrome"),
        ("gent_sing",  "prep_s",      "firefox"),
        ("datv_sing",  "plain",       "firefox"),
        ("loct_plur",  "q_skolko",    "chrome"),
        ("gent_plur",  "plain",       "chrome"),
        ("datv_plur",  "plain_nocp",  "firefox"),
        ("ablt_plur",  "plain",       "firefox"),
        ("accs_sing",  "q_kak",       "chrome"),
        ("loct_plur",  "prep_v",      "chrome"),
        ("nomn_sing",  "prep_na",     "chrome"),
        ("nomn_sing",  "prep_v",      "chrome"),
        ("nomn_sing",  "prep_pod",    "chrome"),
        ("nomn_sing",  "trail",       "chrome"),
        ("datv_plur",  "q_pochemu",   "chrome"),
        ("gent_sing",  "fin_forum",   "chrome"),
        ("gent_plur",  "fin_analogi", "chrome"),
        ("gent_sing",  "prep_iz",     "chrome"),
        ("accs_sing",  "q_kakoy",     "chrome"),
        ("gent_plur",  "q_kakoy",     "chrome"),
        ("ablt_plur",  "prep_ot",     "chrome"),
        ("accs_sing",  "prep_v",      "chrome"),
        ("gent_sing",  "Lwc_cpBL",    "firefox"),
        ("accs_sing",  "q_pochemu",   "chrome"),
        ("nomn_sing",  "prep_ot",     "chrome"),
        ("gent_sing",  "fin_vmesto",  "chrome"),
    ]

    def generate_queries(
        self,
        analysis: MorphSeedAnalysis,
        region: str = "ua",
        include_numbers: bool = False,
        include_letters: bool = True,
    ) -> List[MorphQuery]:

        queries: List[MorphQuery] = []

        EXP_CONFIG = {
            "typo_w1":      ({}, None, None),
            "cyr2lat_w1":   ({}, None, None),
            "cyr2lat_w2":   ({}, None, None),
            "double_space": ({}, None, 3),
            "ds_it":        ({"ds": "it"}, None, 2),
            "client_yt":    ({}, "youtube", 2),
            "cp_one":       ({}, None, 1),
            "brute_и":  ({}, None, None),
            "brute_а":  ({}, None, None),
            "brute_е":  ({}, None, None),
            "brute_у":  ({}, None, None),
            "brute_ы":  ({}, None, None),
            "brute_ом": ({}, None, None),
            "brute_ей": ({}, None, None),
            "brute_о":  ({}, None, None),
            "brute_ю":  ({}, None, None),
        }

        for exp_label, (extra_params, client_override, cp_force) in EXP_CONFIG.items():
            if exp_label not in analysis.case_variants:
                continue
            exp_variant = analysis.case_variants[exp_label]
            _seed_analysis, exp_suffix_queries = self.suffix_gen.generate(
                seed=exp_variant,
                include_numbers=include_numbers,
                include_letters=True,
                region=region,
            )
            display = CASES_RU[exp_label][2]
            for sq in exp_suffix_queries:
                if sq.priority == 0:
                    continue
                query_str = sq.query
                if exp_label == "double_space" and "* " in query_str:
                    query_str = query_str.replace("* ", "*  ", 1)
                cp_val = cp_force if cp_force is not None else sq.cp_override
                ua = "youtube" if client_override == "youtube" else None

                queries.append(MorphQuery(
                    case_label=exp_label,
                    case_display=display,
                    seed_variant=exp_variant,
                    query=query_str,
                    suffix_val=sq.suffix_val,
                    suffix_label=sq.suffix_label,
                    suffix_type=sq.suffix_type,
                    priority=sq.priority,
                    cp_override=cp_val,
                    variant=sq.variant,
                    blocked_by=sq.blocked_by,
                    ua_filter=ua,
                    extra_params=extra_params,
                    client_override=client_override,
                ))

        # ── SEP (Suffix-Ending-Position) — отдельный блок ────────────────────
        for sep_label, sep_variant in analysis.case_variants.items():
            if not sep_label.startswith("sep_"):
                continue
            cp_val = len(sep_variant)
            display = CASES_RU[sep_label][2]
            for sq in self._build_sep_queries(sep_variant):
                queries.append(MorphQuery(
                    case_label=sep_label,
                    case_display=display,
                    seed_variant=sep_variant,
                    query=sq["query"],
                    suffix_val=sq["suffix_val"],
                    suffix_label=sq["suffix_label"],
                    suffix_type="SEP",
                    priority=1,
                    cp_override=cp_val,
                    variant=sq["variant"],
                    blocked_by=None,
                    ua_filter=None,
                    extra_params={},
                    client_override=None,
                ))

        return queries

    def _build_sep_queries(self, sep_variant: str) -> List[Dict]:
        """
        SEP (Suffix-Ending-Position) — хирургические запросы без звёздочек.
        Формат: "стем+триггер  суффикс _буква"  (двойной пробел, underscore)
        Для plain (без суффикса): "стем+триггер   _буква" (тройной пробел)
        cp всегда = len(sep_variant) — после триггера, перед суффиксом.

        Карта суффиксов — из MORPH_SUFFIXES_RU (B/C/D) без звёздочек:
          plain + предлоги + вопросы + финализаторы
        """
        # Универсальная карта суффиксов (без *)
        suffix_map = [
            ("", "plain"),
            # Type B — предлоги
            ("в",    "prep_v"),
            ("на",   "prep_na"),
            ("для",  "prep_dlya"),
            ("с",    "prep_s"),
            ("от",   "prep_ot"),
            ("под",  "prep_pod"),
            ("из",   "prep_iz"),
            ("без",  "prep_bez"),
            # Type C — вопросы
            ("как",     "q_kak"),
            ("какой",   "q_kakoy"),
            ("где",     "q_gde"),
            ("сколько", "q_skolko"),
            ("почему",  "q_pochemu"),
            # Type D — финализаторы
            ("купить",          "fin_kupit"),
            ("цена",            "fin_tsena"),
            ("отзывы",          "fin_otzyvy"),
            ("обзор",           "fin_obzor"),
            ("сравнение",       "fin_sravnenie"),
            ("характеристики",  "fin_harakt"),
            ("аналоги",         "fin_analogi"),
            ("или",             "fin_ili"),
            ("и",               "fin_i"),
            ("vs",              "fin_vs"),
            ("вместо",          "fin_vmesto"),
            ("форум",           "fin_forum"),
        ]

        out = []
        for letter in MORPH_LETTER_SWEEP:
            for sfx_val, sfx_label in suffix_map:
                if sfx_val:
                    query = f"{sep_variant}  {letter} {sfx_val}"
                else:
                    query = f"{sep_variant}  {letter}"
                out.append({
                    "query":        query,
                    "suffix_val":   sfx_val,
                    "suffix_label": f"sep_{sfx_label}_{letter}",
                    "variant":      sfx_label,
                })
        return out

    @staticmethod
    def _sq_to_struct(suffix_label: str) -> str:
        if '_plain_nocp' in suffix_label: return 'plain_nocp'
        if '_plain' in suffix_label:      return 'plain'
        if '_trail' in suffix_label:      return 'trail'
        if '_wcB_cpMid' in suffix_label:  return 'wcB_cpMid'
        if '_Lwc_cpBL' in suffix_label:   return 'Lwc_cpBL'
        if 'sym_ua' in suffix_label or 'sym_ru' in suffix_label: return 'sym'
        if 'prep_na_' in suffix_label:    return 'prep_na'
        if 'prep_dlya_' in suffix_label:  return 'prep_dlya'
        if 'prep_bez_' in suffix_label:   return 'prep_bez'
        if 'prep_s_' in suffix_label:     return 'prep_s'
        if 'prep_ot_' in suffix_label:    return 'prep_ot'
        if 'prep_v_' in suffix_label:     return 'prep_v'
        if 'prep_pod_' in suffix_label:   return 'prep_pod'
        if 'prep_iz_' in suffix_label:    return 'prep_iz'
        if 'q_kak_' in suffix_label:      return 'q_kak'
        if 'q_kakoy_' in suffix_label:    return 'q_kakoy'
        if 'q_skolko_' in suffix_label:   return 'q_skolko'
        if 'q_pochemu_' in suffix_label:  return 'q_pochemu'
        if 'q_gde_' in suffix_label:      return 'q_gde'
        if 'fin_i_' in suffix_label:      return 'fin_i'
        if 'fin_ili_' in suffix_label:    return 'fin_ili'
        if 'fin_otzyvy_' in suffix_label: return 'fin_otzyvy'
        if 'fin_tsena_' in suffix_label:  return 'fin_tsena'
        if 'fin_forum_' in suffix_label:  return 'fin_forum'
        if 'fin_analogi_' in suffix_label:return 'fin_analogi'
        if 'fin_vmesto_' in suffix_label: return 'fin_vmesto'
        return suffix_label

    def summary(self, analysis: MorphSeedAnalysis, queries: List[MorphQuery]) -> Dict:
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
