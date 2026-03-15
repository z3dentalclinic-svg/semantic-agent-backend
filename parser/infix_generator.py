"""
Infix Generator v2.6 — Chrome E: одна cpAL вместо десяти.

Changes vs v2.4:
  - E Chrome: возвращена plain_cpAL (одна вместо десяти)
    Анализ: все cpAL структуры дают одни ключи (~300 каждая),
    но без cpAL теряются 512 инфикс-расширений (гео, бренды).
    3 структуры × 26 букв = 78 E-запросов на gap (было 312)

Changes vs v2.3:
  - E Chrome: убраны 9 лишних cpAL структур (plain_cpAL оставлена)
    -234 запросов на gap

Changes vs v2.2:
  - skip_cp: PREP+NOUN токены больше не блокируют cp-группы
    (аккумулятор на скутер → все 6 групп, было только WC+A)

Changes vs v2.1:
  - WC: wc_nocp_ff убран (0 эксклюзивных)
  - A: A_*_nocp_ff убраны (0 эксклюзивных)

Changes vs v1.0:
  - E: optimized structures (cpAL chrome only)
  - WC: 5 → 1 (nocp_chr only)
  - A: 6 → 2 (nocp_chr only)
  - B: 16 per prep → 1 (B_L_{prep}_cpAL only)
  - C: 10 per word → 3 total
  - D: 10 per word → 3 total

Preprocessing:
  - Strip leading/trailing special chars from seed
  - Strip GEO tokens from seed edges (киев/лондон → убираем перед инфиксом)
  - Merge PREP+NOUN tokens
  - Merge atomic tokens (айфон 16, RTX 3060)
  - Skip gap if w1 is Q-marker
  - Skip full groups if w2 is T-marker → WC only
  - Skip cp-variants if w2 is multi-word token (склеенный предлог)
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Set

# Пробуем импортировать geo_db из основного парсера
# Если недоступно — используем pymorphy3 Geox тег
_geo_db: Optional[Set[str]] = None
_morph = None

def _get_geo_set() -> Set[str]:
    """Загружает гео-базу один раз и кэширует."""
    global _geo_db
    if _geo_db is not None:
        return _geo_db

    # Попытка 1: load_geonames_db из databases (основной проект)
    try:
        from databases import load_geonames_db
        db = load_geonames_db()  # Dict[str, Set[str]] — город → {коды стран}
        _geo_db = set(db.keys())
        return _geo_db
    except Exception:
        pass

    # Попытка 2: через sys.modules (если уже загружено в памяти)
    try:
        import sys
        for mod in sys.modules.values():
            if hasattr(mod, 'GEO_DB'):
                db = mod.GEO_DB
                if isinstance(db, dict):
                    _geo_db = set(db.keys())
                elif isinstance(db, set):
                    _geo_db = set(w.lower() for w in db)
                if _geo_db:
                    return _geo_db
    except Exception:
        pass

    # Fallback: ручной набор для тестирования
    # Включает все вариации городов из тестовых датасетов
    _geo_db = {
        # Киев
        "киев", "київ", "kyiv", "kiev",
        # Днепр и вариации
        "днепр", "дніпро", "дніпр", "днепро", "dnipro", "dnepr",
        "днепропетровск", "дніпропетровськ",
        # Львов и вариации
        "львов", "львів", "lviv", "lvov",
        # Лондон и вариации
        "лондон", "london",
        # Харьков
        "харьков", "харків", "kharkiv", "kharkov",
        # Одесса
        "одесса", "одеса", "odessa", "odesa",
        # Запорожье
        "запорожье", "запоріжжя", "zaporizhzhia",
        # Другие UA города
        "донецк", "донецьк", "луганск", "луганськ",
        "николаев", "миколаїв", "херсон", "полтава",
        "чернигов", "чернігів", "черновцы", "чернівці",
        "ужгород", "ивано-франковск", "івано-франківськ",
        "тернополь", "тернопіль", "хмельницкий", "хмельницький",
        "житомир", "сумы", "суми", "луцк", "луцьк", "ровно", "рівне",
        "кропивницкий", "кропивницький", "винница", "вінниця",
        "кривой рог", "кривий ріг", "мариуполь", "маріуполь",
        "ирпень", "ірпінь", "буча", "бровары", "бровари",
        "борисполь", "бориспіль", "белая церковь", "біла церква",
        # RU города
        "москва", "moscow", "санкт-петербург", "петербург", "спб",
        "новосибирск", "екатеринбург", "казань", "нижний новгород",
        "самара", "омск", "ростов", "ростов-на-дону", "уфа", "красноярск",
        "пермь", "воронеж", "волгоград", "краснодар", "саратов",
        "тюмень", "тольятти", "ижевск", "барнаул", "ульяновск",
        "иркутск", "хабаровск", "ярославль", "владивосток", "махачкала",
        "томск", "оренбург", "кемерово", "новокузнецк", "рязань",
        "астрахань", "набережные челны", "пенза", "липецк", "тула",
        "киров", "чебоксары", "калининград", "брянск", "курск",
        "иваново", "магнитогорск", "улан-удэ", "сочи", "тверь",
        "ставрополь", "белгород", "нижний тагил", "архангельск",
        "владимир", "смоленск", "сургут", "чита", "орел",
        "волжский", "мурманск", "череповец", "вологда", "саранск",
        # BY города
        "минск", "гродно", "брест", "гомель", "витебск", "могилев",
        "минске", "гродне",
        # KZ города
        "алматы", "нур-султан", "астана", "шымкент", "актобе",
        "тараз", "павлодар", "усть-каменогорск", "семей",
        "атырау", "костанай", "петропавловск", "бишкек", "ташкент",
        # Другие СНГ
        "баку", "ереван", "тбилиси", "кишинев", "кишинів",
        "рига", "таллин", "вильнюс", "варшава", "warsaw",
        # Мировые
        "берлин", "berlin", "париж", "paris", "рим", "rome",
        "мадрид", "madrid", "барселона", "barcelona",
        "амстердам", "amsterdam", "вена", "vienna", "прага", "prague",
        "будапешт", "budapest", "варшава", "стамбул", "istanbul",
        "анкара", "ankara", "тель-авив", "дубай", "dubai",
        "нью-йорк", "new york", "лос-анджелес", "чикаго",
        "пекин", "beijing", "шанхай", "shanghai", "токио", "tokyo",
        "сеул", "seoul", "бангкок", "bangkok", "сингапур", "singapore",
    }
    return _geo_db

def _is_geo_word(word: str) -> bool:
    """Определяет является ли слово гео-токеном."""
    global _morph
    w = word.lower().strip()
    if not w or len(w) < 3:
        return False
    # Проверяем geo_db
    geo_set = _get_geo_set()
    if w in geo_set:
        return True
    # Fallback: pymorphy3 Geox тег
    try:
        if _morph is None:
            import pymorphy3
            _morph = pymorphy3.MorphAnalyzer(lang='ru')
        parsed = _morph.parse(w)
        if parsed and parsed[0].score >= 0.3:
            tag = str(parsed[0].tag)
            if 'Geox' in tag:
                return True
    except Exception:
        pass
    return False


# ══════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════

LETTERS_RU = list("абвгдежзийклмнопрстуфхцчшщэюя")  # 26 букв

SYMBOL_UA = ":"
SYMBOL_RU = "&"

PREPS_RU = ["в", "на", "для", "с", "от", "под", "из", "без"]

# Только самые ценные по анализу дублей
QUESTIONS_KEEP = ["как", "сколько"]  # остальные 95%+ дублей

# Только финализаторы с <90% дублей
FINALIZERS_KEEP = ["и", "или", "vs"]

ALL_GROUPS = ["WC", "A", "B", "C", "D", "E"]

# Q-маркеры — gap где w1 = Q-маркер пропускается (Google игнорирует правый якорь)
Q_MARKERS = {"как", "какой", "какая", "какое", "какие", "где", "сколько",
             "почему", "зачем", "когда", "куда", "откуда", "чей"}

# T-маркеры — gap где w2 = T-маркер запускаем только WC.
# цена/стоимость здесь намеренно: все 104 запроса gap'а с w2=цена
# возвращают один тип мусора "[X] цена [город]". WC даёт 15 качественных.
T_MARKERS = {"купить", "купи", "отзывы", "обзор",
             "характеристики", "аналоги", "сравнение", "форум",
             "цена", "стоимость"}

# Предлоги для склейки PREP+NOUN
PREP_MERGE = {"в", "во", "на", "для", "с", "со", "от", "под", "из", "без",
              "по", "за", "при", "до", "над", "через", "про", "об", "о",
              "к", "ко", "у", "между"}


# ══════════════════════════════════════════════
# DATACLASS
# ══════════════════════════════════════════════

@dataclass
class InfixQuery:
    query: str
    gap_index: int
    w1: str
    w2: str
    group: str
    struct: str
    insert_val: str
    insert_type: str
    orientation: str
    cp: int
    cp_note: str
    agents: tuple
    priority: int = 1
    letter: Optional[str] = None


# ══════════════════════════════════════════════
# GENERATOR
# ══════════════════════════════════════════════

class InfixGenerator:
    """
    Generates optimized infix query variants for a seed.

    Usage:
        gen = InfixGenerator()
        queries = gen.generate("ремонт пылесосов")
        # → ~46 запросов (было 1367)
    """

    def generate(self, seed: str, groups: Optional[List[str]] = None) -> List[InfixQuery]:
        # 1. Предобработка сида
        S, geo_tokens = self._preprocess(seed)
        words = S.split()
        grp = set(groups) if groups else set(ALL_GROUPS)
        out: List[InfixQuery] = []

        # 2. Склейка токенов: предлоги + атомарные пары
        # _merge_tokens возвращает List[(token, is_atomic)]
        #   is_atomic=True  → latin/цифровая цепочка (samsung galaxy s21) → skip_cp
        #   is_atomic=False → PREP+NOUN (на скутер) → cp работает нормально
        tokens_with_flags = self._merge_tokens(words)
        tokens = [t for t, _ in tokens_with_flags]
        atomic_set = {t for t, is_atomic in tokens_with_flags if is_atomic}

        # 3. Gap'ы
        gaps = self._get_gaps(tokens)

        for gap_idx, w1, w2, full_coverage in gaps:
            # Q-маркер на w1 → пропускаем gap полностью
            if w1.lower() in Q_MARKERS:
                continue
            # T-маркер на w2 → только WC
            if w2.lower().split()[0] in T_MARKERS:
                active = grp & {"WC"}
            else:
                active = grp if full_coverage else (grp & {"WC"})

            # skip_cp только для атомарных токенов (latin/цифры), не для PREP+NOUN
            skip_cp = (w1 in atomic_set) or (w2 in atomic_set)

            # right_suffix — токены после w2 в исходном сиде.
            # Нужен чтобы сохранить семантический контекст в запросе.
            # Пример "аренда авто без залога", gap[0]:
            #   с suffix:   "аренда А авто без залога" → релевантные инфикс-ключи
            #   без suffix: "аренда А авто"            → generic "аренда авто [город]" мусор
            #
            # НО: если суффикс начинается с T_MARKER (цена/стоимость/купить...),
            # right_suffix убирается для gap[0] — Google фокусируется на T_MARKER
            # и возвращает только гео-вариации вместо инфикс-расширений.
            # Пример "установка кондиционера цена", gap[0]:
            #   с suffix:   "установка А кондиционера цена" → цена киев / цена минск ...
            #   без suffix: "установка А кондиционера"      → инверторного / настенного ...
            w2_idx = tokens.index(w2)
            raw_suffix = " ".join(tokens[w2_idx + 1:]) if w2_idx + 1 < len(tokens) else ""
            suffix_first_word = raw_suffix.split()[0] if raw_suffix else ""
            if gap_idx == 0 and suffix_first_word in T_MARKERS:
                right_suffix = ""
            else:
                right_suffix = raw_suffix

            out.extend(self._generate_gap(gap_idx, w1, w2, active, geo_tokens,
                                          skip_cp=skip_cp, right_suffix=right_suffix))

        return out

    # ──────────────────────────────────────────
    # ПРЕДОБРАБОТКА
    # ──────────────────────────────────────────

    def _preprocess(self, seed: str) -> Tuple[str, str]:
        """
        Стрипаем спецсимволы и гео-токены с краёв сида.

        Возвращает:
            core       — сид без гео-краёв (для построения якорей)
            geo_tokens — гео-токены с краёв (строка, может быть пустой)

        Логика geo_tokens:
            - nocp структуры: geo добавляется в КОНЕЦ  → "w1 [X] w2 geo"
              Google видит оба якоря + гео как правый контекст
            - cp структуры:   geo добавляется в НАЧАЛО → "geo w1 [X] w2"
              geo оказывается в стабильном левом контексте,
              cp сдвигается на len(geo)+1
        """
        s = seed.strip().lower()
        # Убираем ведущие/хвостовые спецсимволы
        s = re.sub(r'^[^\w\s]+', '', s)
        s = re.sub(r'[^\w\s]+$', '', s)
        s = s.strip()

        # Собираем гео-токены с краёв (только если в ядре остаётся ≥2 слов)
        words = s.split()
        geo_collected = []

        while len(words) > 2 and _is_geo_word(words[-1]):
            geo_collected.insert(0, words.pop())   # правый край → в начало списка
        while len(words) > 2 and _is_geo_word(words[0]):
            words = words[1:]                       # левый край → просто убираем

        return ' '.join(words), ' '.join(geo_collected)

    # ──────────────────────────────────────────
    # СКЛЕЙКА ТОКЕНОВ
    # ──────────────────────────────────────────

    def _merge_tokens(self, words: List[str]) -> List[Tuple[str, bool]]:
        """
        Возвращает List[(token, is_atomic)] где:
          is_atomic=True  → latin/цифровая цепочка (samsung galaxy s21) → skip_cp
          is_atomic=False → одиночное слово или PREP+NOUN → cp работает нормально

        Два этапа:
        1. ИТЕРАТИВНАЯ склейка атомарных цепочек: латиница/цифра подряд → один токен
        2. Склейка PREP+NOUN → один токен (is_atomic=False)
        """
        if len(words) < 2:
            return [(w, False) for w in words]

        MODEL_WORDS = {"pro", "max", "plus", "ultra", "fe", "mini",
                       "lite", "note", "air", "se", "s", "e", "x"}

        def _can_merge(w: str, nw: str) -> bool:
            curr_has_lat = bool(re.search(r'[a-zA-Z0-9]', w))
            next_is_atom = bool(re.search(r'^[a-zA-Z0-9]', nw)) or nw.lower() in MODEL_WORDS
            return curr_has_lat and next_is_atom

        # Проход 1: атомарные цепочки (is_atomic=True)
        result: List[Tuple[str, bool]] = [(w, False) for w in words]
        while True:
            merged: List[Tuple[str, bool]] = []
            i = 0
            changed = False
            while i < len(result):
                tok, was_atomic = result[i]
                if i + 1 < len(result) and _can_merge(tok, result[i+1][0]):
                    merged.append((tok + " " + result[i+1][0], True))
                    i += 2
                    changed = True
                else:
                    merged.append((tok, was_atomic))
                    i += 1
            result = merged
            if not changed:
                break

        # Проход 2: PREP+NOUN (is_atomic=False — cp работает через предлог)
        result2: List[Tuple[str, bool]] = []
        i = 0
        while i < len(result):
            tok, is_atomic = result[i]
            if tok in PREP_MERGE and i + 1 < len(result) and result2:
                result2.append((tok + " " + result[i+1][0], False))
                i += 2
            else:
                result2.append((tok, is_atomic))
                i += 1

        return result2

    # ──────────────────────────────────────────
    # GAP СТРАТЕГИЯ
    # ──────────────────────────────────────────

    def _get_gaps(self, tokens: List[str]) -> List[Tuple[int, str, str, bool]]:
        """
        2 токена → gap[0] (full)
        3 токена → gap[0], gap[1] (full)
        4 токена → gap[0] full, gap[1] WC-only, gap[2] full
        5+ токенов → gap[0] full, gap[last] full
        """
        n = len(tokens)
        if n < 2:
            return []
        if n == 2:
            return [(0, tokens[0], tokens[1], True)]
        if n == 3:
            return [(0, tokens[0], tokens[1], True),
                    (1, tokens[1], tokens[2], True)]
        if n == 4:
            return [(0, tokens[0], tokens[1], True),
                    (1, tokens[1], tokens[2], False),
                    (2, tokens[2], tokens[3], True)]
        return [(0, tokens[0], tokens[1], True),
                (n - 2, tokens[n - 2], tokens[n - 1], True)]

    # ──────────────────────────────────────────
    # ГЕНЕРАЦИЯ ОДНОГО GAP'А
    # ──────────────────────────────────────────

    def _generate_gap(self, gap_idx, w1, w2, groups, geo_tokens="", skip_cp=False, right_suffix="") -> List[InfixQuery]:
        out = []
        CHR = ("chrome",)

        # ── Гео-контекст ─────────────────────────────────────────
        # nocp: geo в конце  → "w1 [X] w2 geo"  (оба якоря + гео как правый контекст)
        # cp:   geo в начале → "geo w1 [X] w2"  (гео в стабильном левом контексте)
        geo = geo_tokens.strip()
        geo_shift = len(geo) + 1 if geo else 0  # сдвиг cp-позиции при гео-префиксе

        # skip_cp передаётся из generate() — True только для атомарных токенов
        # (latin/цифровые цепочки: samsung galaxy s21, iphone 16 pro max).
        # PREP+NOUN токены (на скутер) НЕ триггерят skip_cp — cp там работает нормально.

        def _w2_nocp():
            """w2 + right_suffix + гео в конце для nocp-структур.
            right_suffix — токены после w2 в исходном сиде (правый контекст).
            Пример: gap[0] "аренда|авто|без залога" → w2_nocp = "авто без залога"
            """
            parts = [w2]
            if right_suffix:
                parts.append(right_suffix)
            if geo:
                parts.append(geo)
            return " ".join(parts)

        def _w2_cp():
            """w2 + right_suffix для cp-структур (гео уже в начале через _w1_cp)."""
            return f"{w2} {right_suffix}".strip() if right_suffix else w2

        def _w1_cp():
            """w1 с гео-префиксом для cp-структур."""
            return f"{geo} {w1}".strip() if geo else w1

        def q(group, struct, query, cp, cp_note, insert_val, insert_type,
              orientation, agents=CHR, letter=None):
            return InfixQuery(
                query=query, gap_index=gap_idx, w1=w1, w2=w2,
                group=group, struct=struct,
                insert_val=insert_val, insert_type=insert_type,
                orientation=orientation, cp=cp, cp_note=cp_note,
                agents=agents, letter=letter,
            )

        n1 = len(w1)

        # ── WC: только nocp_chr ───────────────────────────────────
        # wc_nocp_ff убран (v2.2): 0 эксклюзивных по датасетам
        if "WC" in groups:
            base = f"{w1} * {_w2_nocp()}"
            out.append(q("WC", "wc_nocp_chr", base, -1, "без cp, chrome", "*", "wildcard", "N", CHR))

        # ── A: только nocp_chr ───────────────────────────────────
        # A_*_nocp_ff убраны (v2.2): 0 эксклюзивных по датасетам
        if "A" in groups:
            for sym, cluster in [(SYMBOL_UA, "ua"), (SYMBOL_RU, "ru")]:
                base = f"{w1} {sym} {_w2_nocp()}"
                out.append(q("A", f"A_{cluster}_nocp_chr", base, -1, "без cp, chrome", sym, "symbol", "N", CHR))

        # ── B: только B_L_{prep}_cpAL ────────────────────────────
        # cpAL = курсор после предлога (тяготеет влево к w1)
        # Единственный вариант с <75% дублей по анализу
        if "B" in groups and not skip_cp:
            for prep in PREPS_RU:
                bl = f"{_w1_cp()} {prep} * {_w2_cp()}"
                cp_al = geo_shift + n1 + 1 + len(prep) + 1  # гео_сдвиг + w1 + space + prep + space
                out.append(q("B", f"B_L_{prep}_cpAL", bl, cp_al, "после предлога", prep, "prep", "L", CHR))

        # ── C: только 3 структуры с <80% дублей ──────────────────
        if "C" in groups and not skip_cp:
            # как_cpAL (79% дублей но 51 уник) — cp структура → гео влево
            cl = f"{_w1_cp()} как * {_w2_cp()}"
            cp_al = geo_shift + n1 + 1 + len("как") + 1
            out.append(q("C", "C_L_как_cpAL", cl, cp_al, "после как", "как", "question", "L", CHR))

            # сколько nocp_chr — оба направления → гео вправо
            for qw in ["сколько"]:
                cl2 = f"{w1} {qw} * {_w2_nocp()}"
                out.append(q("C", f"C_L_{qw}_nocp_chr", cl2, -1, "без cp, chrome", qw, "question", "L", CHR))
                cr2 = f"{w1} * {qw} {_w2_nocp()}"
                out.append(q("C", f"C_R_{qw}_nocp_chr", cr2, -1, "без cp, chrome", qw, "question", "R", CHR))

        # ── D: только 3 финализатора с <92% дублей ───────────────
        if "D" in groups and not skip_cp:
            # D_L_и_cpAL: 28% дублей — лучший результат
            # D_L_или_cpAL: 88%, D_L_vs_cpAL: 90%
            for fin in FINALIZERS_KEEP:
                dl = f"{_w1_cp()} {fin} * {_w2_cp()}"
                cp_al = geo_shift + n1 + 1 + len(fin) + 1
                out.append(q("D", f"D_L_{fin}_cpAL", dl, cp_al, f"после {fin}", fin, "finalizer", "L", CHR))

        # ── E: plain_nocp_chr + cpAL Chrome ──────────────────────
        # Firefox E убран в v2.1 (ROI < 1%). Chrome only.
        if "E" in groups and not skip_cp:
            for L in LETTERS_RU:
                # nocp: гео в конце
                w2n = _w2_nocp()
                t_plain_n   = f"{w1} {L} {w2n}"
                t_Lwc_n     = f"{w1} {L} * {w2n}"
                t_wcL_n     = f"{w1} * {L} {w2n}"
                t_sand_n    = f"{w1} * {L} * {w2n}"
                t_Lstar_n   = f"{w1} {L}* {w2n}"
                t_starL_n   = f"{w1} *{L} {w2n}"
                t_L_hyp_n   = f"{w1} {L} - {w2n}"
                t_hyp_L_n   = f"{w1} - {L} {w2n}"
                t_hyp_Lwc_n = f"{w1} - {L} * {w2n}"

                # cp: гео в начале (w1 → geo+w1)
                w1c = _w1_cp()
                t_plain   = f"{w1c} {L} {_w2_cp()}"
                t_Lwc     = f"{w1c} {L} * {_w2_cp()}"
                t_wcL     = f"{w1c} * {L} {_w2_cp()}"
                t_sand    = f"{w1c} * {L} * {_w2_cp()}"
                t_Lstar   = f"{w1c} {L}* {_w2_cp()}"
                t_starL   = f"{w1c} *{L} {_w2_cp()}"
                t_L_hyp   = f"{w1c} {L} - {_w2_cp()}"
                t_hyp_L   = f"{w1c} - {L} {_w2_cp()}"
                t_hyp_Lwc = f"{w1c} - {L} * {_w2_cp()}"
                t_L_col   = f"{w1c} {L} : {_w2_cp()}"
                t_col_L   = f"{w1c} : {L} {_w2_cp()}"

                n1c = len(w1c)  # длина w1 с гео-префиксом для сдвига cp
                # cp позиции после буквы (cpAL) — сдвинуты на geo_shift
                cp_plain   = n1c + 2
                cp_Lwc     = n1c + 2
                cp_wcL     = n1c + 4
                cp_sand    = n1c + 4
                cp_Lstar   = n1c + 3
                cp_starL   = n1c + 3
                cp_L_hyp   = n1c + 2
                cp_hyp_L   = n1c + 4
                cp_hyp_Lwc = n1c + 4
                cp_L_col   = n1c + 2
                cp_col_L   = n1c + 4

                # Chrome E: 3 структуры на букву = 78 запросов на gap (v2.5)
                # Анализ показал: все 10 cpAL структур возвращают ~одни и те же ключи
                # (300-327 каждая), но в СУММЕ дают 512 инфикс-расширений которых
                # нет ни в nocp ни в Lstar → достаточно одной cpAL.
                # Оставлены:
                #   plain_nocp_chr — nocp базовый
                #   plain_cpAL     — одна cpAL даёт все инфикс-расширения
                #   Lstar_cpAS     — аномально высокий выход (345 эксклюзивных)
                out.append(q("E", f"E_{L}_plain_nocp_chr", t_plain_n, -1,       "без cp",  L, "letter", "N", CHR, letter=L))
                out.append(q("E", f"E_{L}_plain_cpAL",     t_plain,   cp_plain, "после L", L, "letter", "L", CHR, letter=L))
                out.append(q("E", f"E_{L}_Lstar_cpAS",     t_Lstar,   cp_Lstar, "после *", L, "letter", "L", CHR, letter=L))

                # Firefox E — убран (v2.1): 9 структур × 26 букв = 234 запроса на gap
                # даёт 1-10 эксклюзивных по датасетам (<1% на больших сидах), ROI слишком низкий

        return out

    def summary(self, queries: List[InfixQuery]) -> dict:
        by_group = {}
        for g in ALL_GROUPS:
            gq = [q for q in queries if q.group == g]
            by_group[g] = {
                "total": len(gq),
                "chrome": sum(1 for q in gq if "chrome" in q.agents),
                "firefox": sum(1 for q in gq if "firefox" in q.agents),
            }
        gaps = sorted(set(q.gap_index for q in queries))
        by_gap = {}
        for gi in gaps:
            gq = [q for q in queries if q.gap_index == gi]
            by_gap[gi] = {"total": len(gq), "w1": gq[0].w1, "w2": gq[0].w2}
        return {
            "total_queries": len(queries),
            "chrome_queries": sum(1 for q in queries if "chrome" in q.agents),
            "firefox_queries": sum(1 for q in queries if "firefox" in q.agents),
            "gaps": len(gaps),
            "by_group": by_group,
            "by_gap": by_gap,
        }
