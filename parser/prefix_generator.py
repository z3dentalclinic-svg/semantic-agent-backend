"""
Prefix Generator v1.0 — Full prefix matrix for research testing.

Architecture mirrors suffix_generator.py:
    - PrefixQuery dataclass (analog of SuffixQuery)
    - STRUCTURES per group (G1–G9, PA, PC)
    - PrefixGenerator.generate() → List[PrefixQuery]
    - Self-contained: no dependency on suffix_generator

Matrix v2.0 (финализирована по 7 датасетам Chrome + 4 датасетам Firefox):
    G1  — [OP] [S] *         5 cp variants    Chrome + Firefox
    G2  — [OP] [S] <space>   2 variants       Chrome + Firefox
    G3  — [OP] * [S] *       3 variants       Chrome + Firefox
    G4  — * [OP] [S] *       3 variants       Chrome + Firefox
    G5  — * [S] *            3 variants       Chrome + Firefox
    G6  — [OP]  [S] *        2 variants       Chrome + Firefox
    G7  — *[S]*              1 вариант        Chrome + Firefox  (схлопнуто с 4)
    G8  — ** [S] *           1 вариант        Chrome + Firefox  (схлопнуто с 5)
    G9  — без trailing *     2 варианта       Chrome + Firefox  (схлопнуто с 4)
    PA  — [L] [S] ...        9 structures × 30 букв = 270 запросов  Chrome only
    PC  — [вопрос] [S] *     11 запросов      Chrome + Firefox

Agents:
    Chrome:  G1–G9 + PA + PC  (272 запроса)
    Firefox: G1–G9 + PC only  (38 запросов)  PA даёт мусор на Firefox
    Параллельно: Chrome и Firefox стартуют одновременно,
                 внутри каждого агента запросы тоже параллельно.

PA структуры (9 из 14 — по данным 7 датасетов):
    ОСТАВЛЕНЫ:  cp1, cp0, wcB_cpMid, Lwc_cpBL, Lwc_cpAL,
                hyp_Lwc, hyp_wcL, L_hyp, hyp_B_trail
    УДАЛЕНЫ:    nocp (0/7), trail (0/7), sandwich (нестаб),
                L_col (нестаб), col_B_trail (нестаб)

Total: Chrome 272 + Firefox 38 = 310 запросов на сид
"""

from dataclasses import dataclass, field
from typing import List, Optional


# ══════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════

# 33 буквы — полный алфавит минус ъ ы ь (не начинают слова)
# Оставляем все 33 для полного теста — после прогона уберём пустые
LETTERS_RU = list("абвгдеёжзийклмнопрстуфхцчшщэюя")

# Вопросы для Type PC
QUESTIONS_RU = ["как", "какой", "где", "сколько", "почему"]

# Группы для удобства итерации
ALL_GROUPS = ["G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8", "G9", "PA", "PC"]


# ══════════════════════════════════════════════
# DATACLASS
# ══════════════════════════════════════════════

@dataclass
class PrefixQuery:
    """Single generated prefix query with full metadata."""
    query: str            # Полная строка запроса отправляемая в Google
    group: str            # G1..G9, PA, PC
    struct: str           # Название структуры (для трейсера)
    operator: str         # Оператор или буква
    op_type: str          # "letter" / "question" / "intent" / "prep" / "symbol"
    cp: int               # Cursor position (-1 = не передавать)
    cp_note: str          # Человекочитаемое описание позиции курсора

    # Агенты для запуска
    agents: tuple = ("chrome",)  # ("chrome",) | ("firefox",) | ("chrome", "firefox")

    # Флаги для аналитики
    is_alpha: bool = False       # Входит в алфавитный перебор
    is_question: bool = False    # Вопросительный оператор
    letter: Optional[str] = None # Буква для PA группы


# ══════════════════════════════════════════════
# GENERATOR
# ══════════════════════════════════════════════

class PrefixGenerator:
    """
    Generates all prefix query variants for a seed.

    Usage:
        gen = PrefixGenerator()
        queries = gen.generate(seed="имплантация зубов", operator="купить")
        # → List[PrefixQuery], ~471 запросов с полным стеком

    Args:
        seed:      Базовый запрос
        operator:  Оператор для G1-G9 групп (купить, цена, как и т.д.)
        groups:    Набор групп для генерации. None = все группы.
        op_type:   Тип оператора (определяется автоматически если None)
    """

    # Типы операторов для метаданных
    _INTENTS   = {"купить", "цена", "отзывы", "обзор", "характеристики",
                  "аналоги", "vs", "сравнение", "или", "вместо", "форум"}
    _PREPS     = {"в", "на", "для", "с", "от", "под", "из", "без"}
    _QUESTIONS = set(QUESTIONS_RU)

    def _detect_op_type(self, op: str) -> str:
        if op in self._INTENTS:   return "intent"
        if op in self._PREPS:     return "prep"
        if op in self._QUESTIONS: return "question"
        return "other"

    def generate(
        self,
        seed: str,
        operator: str = "купить",
        groups: Optional[List[str]] = None,
        op_type: Optional[str] = None,
    ) -> List[PrefixQuery]:
        """
        Generate full prefix matrix.
        Returns list of PrefixQuery sorted by group then struct.
        """
        S   = seed.strip()
        OP  = operator.strip()
        grp = set(groups) if groups else set(ALL_GROUPS)
        ot  = op_type or self._detect_op_type(OP)
        out: List[PrefixQuery] = []

        BOTH   = ("chrome", "firefox")  # не используется — маршрутизация через FF_SET
        CHROME = ("chrome",)
        FF     = ("firefox",)

        # FF-exclusive структуры prefix — дают GT-ключи которые Chrome не находит.
        # Источник: unified_firefox_structs.json (31 структура по 4 датасетам).
        # Всё что НЕ в этом сете → Chrome only.
        PA_FF_STRUCTS = {
            # cp1 — 21 буква
            "а_cp1", "б_cp1", "в_cp1", "г_cp1", "д_cp1", "е_cp1", "и_cp1",
            "к_cp1", "л_cp1", "м_cp1", "н_cp1", "о_cp1", "п_cp1", "р_cp1",
            "с_cp1", "т_cp1", "ф_cp1", "ц_cp1", "ч_cp1", "ш_cp1", "э_cp1",
            # прочие PA структуры
            "в_cp0", "с_cp0",
            "в_Lwc_cpBL",
            "в_hyp_B_trail", "в_hyp_Lwc",
            "и_hyp_B_trail",
            "к_hyp_Lwc",
        }

        def q(group: str, struct: str, query: str, cp: int,
              cp_note: str, op_val: str = OP, op_t: str = ot,
              is_alpha: bool = False, is_q: bool = False,
              letter: str = None, agents: tuple = CHROME) -> PrefixQuery:
            return PrefixQuery(
                query=query, group=group, struct=struct,
                operator=op_val, op_type=op_t, cp=cp, cp_note=cp_note,
                agents=agents,
                is_alpha=is_alpha, is_question=is_q, letter=letter,
            )

        # ─────────────────────────────────────────────────────────────
        # G1 — [OP] [S] *  — 5 cp variants
        # Базовые позиции курсора для стандартной структуры
        # ─────────────────────────────────────────────────────────────
        if "G1" in grp:
            base = f"{OP} {S} *"
            # vP1: cp после OP пробел, перед S — Google видит OP как первую букву
            out.append(q("G1", "vP1_afterOP_space", base,
                         len(OP) + 1, "после OP, перед S"))
            # vP2: cp на последнем символе OP — OP как отдельный блок
            out.append(q("G1", "vP2_onOP_end", base,
                         len(OP), "на конце OP, перед пробелом"))
            # vP3: cp в начале строки
            out.append(q("G1", "vP3_start", base,
                         0, "начало строки"))
            # vP4: cp после S, перед *
            out.append(q("G1", "vP4_afterS", base,
                         len(OP) + 1 + len(S) + 1, "после S, перед *"))
            # vP5: cp в конце строки
            out.append(q("G1", "vP5_end", base,
                         len(base), "конец строки"))

        # ─────────────────────────────────────────────────────────────
        # G2 — [OP] [S] <space>  — trailing space вместо *
        # В суффиксе trailing space = "предложи следующее слово"
        # ─────────────────────────────────────────────────────────────
        if "G2" in grp:
            trail = f"{OP} {S} "
            out.append(q("G2", "trail_afterOP", trail,
                         len(OP) + 1, "после OP, trailing space"))
            out.append(q("G2", "trail_end", trail,
                         len(trail), "конец строки, trailing space"))

        # ─────────────────────────────────────────────────────────────
        # G3 — [OP] * [S] *  — wildcard как разделитель
        # ─────────────────────────────────────────────────────────────
        if "G3" in grp:
            b = f"{OP} * {S} *"
            out.append(q("G3", "wc_afterOP",   b,
                         len(OP) + 1, "после OP, перед *"))
            out.append(q("G3", "wc_afterStar", b,
                         len(OP) + 3, "после *, перед S"))
            out.append(q("G3", "wc_afterS",    b,
                         len(OP) + 3 + len(S) + 1, "после S, перед конечным *"))

        # ─────────────────────────────────────────────────────────────
        # G4 — * [OP] [S] *  — обратный wildcard перед оператором
        # ─────────────────────────────────────────────────────────────
        if "G4" in grp:
            b = f"* {OP} {S} *"
            out.append(q("G4", "rwc_afterStar",  b,
                         2, "после первого *, перед OP"))
            out.append(q("G4", "rwc_afterOP",    b,
                         2 + len(OP) + 1, "после OP, перед S"))
            out.append(q("G4", "rwc_afterS",     b,
                         2 + len(OP) + 1 + len(S) + 1, "после S, перед *"))

        # ─────────────────────────────────────────────────────────────
        # G5 — * [S] *  /  * [S] <space>  — чистый prefix wildcard
        # Без оператора — что Google дополняет перед сидом
        # ─────────────────────────────────────────────────────────────
        if "G5" in grp:
            b_star  = f"* {S} *"
            b_trail = f"* {S} "
            out.append(q("G5", "pxwc_afterStar", b_star,  2, "после *, перед S",
                         op_val="*", op_t="symbol", agents=FF))
            out.append(q("G5", "pxwc_afterS",    b_star,
                         2 + len(S) + 1, "после S, перед конечным *",
                         op_val="*", op_t="symbol", agents=CHROME))
            out.append(q("G5", "pxwc_trail",     b_trail,
                         len(b_trail), "trailing space (без конечного *)",
                         op_val="*", op_t="symbol", agents=CHROME))

        # ─────────────────────────────────────────────────────────────
        # G6 — [OP]<двойной пробел>[S] *  — нормализует ли Google?
        # ─────────────────────────────────────────────────────────────
        if "G6" in grp:
            b = f"{OP}  {S} *"
            out.append(q("G6", "dbl_afterOP", b,
                         len(OP) + 2, "после двойного пробела, перед S"))
            out.append(q("G6", "dbl_afterS",  b,
                         len(OP) + 2 + len(S) + 1, "после S, перед *"))

        # ─────────────────────────────────────────────────────────────
        # G7 — A_local (:) / A_general (*) перед сидом
        # Зеркало Type A из суффикса — гео-кластеры
        # ─────────────────────────────────────────────────────────────
        if "G7" in grp:
            # Схлопнуто до 1 запроса: все 4 варианта (local/general × cp) давали
            # идентичный результат на всех 7 датасетах. Оставлен sym_general_afterSym.
            out.append(q("G7", "sym_general_afterSym", f"* {S} *",
                         2, "после *, перед S",
                         op_val="*", op_t="symbol"))

        # ─────────────────────────────────────────────────────────────
        # G8 — ** / *** / * *  — вытягивание брендов (Дайсон ремонт пылесосов)
        # ─────────────────────────────────────────────────────────────
        if "G8" in grp:
            # Схлопнуто до 1 запроса: все 5 вариантов давали идентичный результат
            # на всех 7 датасетах (Chrome). Оставлен dstar_afterStar.
            out.append(q("G8", "dstar_afterStar", f"** {S} *",
                         3, "после **, перед S", op_val="**", op_t="symbol"))

        # ─────────────────────────────────────────────────────────────
        # G9 — без trailing *  — контроль режима
        # ─────────────────────────────────────────────────────────────
        if "G9" in grp:
            # Два варианта — дают разные результаты (trailing space меняет режим).
            # nostar_spstar и nostar_single схлопнуты: идентичны nostar_dstar.
            out.append(q("G9", "nostar_dstar",    f"** {S}",
                         3, "после **, перед S, без *",
                         op_val="**", op_t="symbol"))
            out.append(q("G9", "nostar_dstar_tr", f"** {S} ",
                         3 + len(S) + 1, "trailing space — другой режим",
                         op_val="**", op_t="symbol"))

        # ─────────────────────────────────────────────────────────────
        # PA — Алфавитный перебор: 9 структур × 30 букв = 270 запросов
        # Chrome only — Firefox даёт мусор (буква остаётся в начале ключа)
        #
        # Оставлены (по 7 датасетам):
        #   cp1, cp0, wcB_cpMid, Lwc_cpBL, Lwc_cpAL,
        #   hyp_Lwc, hyp_wcL, L_hyp, hyp_B_trail
        # Удалены:
        #   nocp (0 уник на всех датасетах), trail (0 уник),
        #   sandwich (нестаб), L_col (нестаб), col_B_trail (нестаб)
        # ─────────────────────────────────────────────────────────────
        if "PA" in grp:
            for L in LETTERS_RU:
                qstr = f"{L} {S}"

                # 1. cp0
                out.append(q("PA", f"{L}_cp0", qstr,
                             0, "cp=0, начало строки",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_cp0" in PA_FF_STRUCTS else CHROME))

                # 2. cp1
                out.append(q("PA", f"{L}_cp1", qstr,
                             1, "cp=1, после буквы",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_cp1" in PA_FF_STRUCTS else CHROME))

                # 3. wcB_cpMid
                qstr = f"{L} * {S}"
                out.append(q("PA", f"{L}_wcB_cpMid", qstr,
                             len(L) + 3, "cp между * и S",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_wcB_cpMid" in PA_FF_STRUCTS else CHROME))

                # 6. Lwc_cpAL
                qstr = f"{L} {S} *"
                out.append(q("PA", f"{L}_Lwc_cpAL", qstr,
                             len(L) + 1 + len(S) + 1, "cp после S, перед *",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_Lwc_cpAL" in PA_FF_STRUCTS else CHROME))

                # 7. Lwc_cpBL
                out.append(q("PA", f"{L}_Lwc_cpBL", qstr,
                             len(L) + 1, "cp после L, перед S",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_Lwc_cpBL" in PA_FF_STRUCTS else CHROME))

                # 8. hyp_B_trail
                qstr = f"{L} - {S} "
                out.append(q("PA", f"{L}_hyp_B_trail", qstr,
                             len(qstr), "дефис + буква, trailing space",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_hyp_B_trail" in PA_FF_STRUCTS else CHROME))

                # 9. hyp_Lwc
                qstr = f"{L} - {S} *"
                out.append(q("PA", f"{L}_hyp_Lwc", qstr,
                             len(qstr), "дефис + буква + *",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_hyp_Lwc" in PA_FF_STRUCTS else CHROME))

                # 10. hyp_wcL
                qstr = f"{L} - * {S}"
                out.append(q("PA", f"{L}_hyp_wcL", qstr,
                             len(qstr), "дефис + * + S",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_hyp_wcL" in PA_FF_STRUCTS else CHROME))

                # 11. L_hyp
                qstr = f"{L} {S} -"
                out.append(q("PA", f"{L}_L_hyp", qstr,
                             len(qstr), "буква + S + дефис",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L,
                             agents=FF if f"{L}_L_hyp" in PA_FF_STRUCTS else CHROME))

        # ─────────────────────────────────────────────────────────────
        # PC — Вопросы: 5 вопросов × 2-3 cp = 11 запросов
        # Зеркало Type C из суффикса (вопрос перед сидом)
        # ─────────────────────────────────────────────────────────────
        if "PC" in grp:
            for qw in QUESTIONS_RU:
                base = f"{qw} {S} *"
                # vP1: cp после вопроса, перед S
                out.append(q("PC", f"{qw}_vP1", base,
                             len(qw) + 1, "после вопроса, перед S",
                             op_val=qw, op_t="question",
                             is_q=True,
                             agents=FF if f"{qw}_vP1" in {"как_vP1", "как_vP2"} else CHROME))
                # vP2: cp на конце слова вопроса
                out.append(q("PC", f"{qw}_vP2", base,
                             len(qw), "на конце вопроса",
                             op_val=qw, op_t="question",
                             is_q=True,
                             agents=FF if f"{qw}_vP2" in {"как_vP1", "как_vP2"} else CHROME))
                # vP3: только для "почему" — в суффиксе v1/v2 всегда empty, v3 даёт 6
                if qw == "почему":
                    out.append(q("PC", f"{qw}_vP3", base,
                                 len(qw) + 1 + len(S) + 1, "после S, перед * (только почему)",
                                 op_val=qw, op_t="question",
                                 is_q=True))

        # ── Порог 0 — структуры с unique_contrib=0 на всех 4 датасетах ──────
        # Источник: имплантация зубов, аккумулятор на скутер,
        #           установка кондиционера цена, купить айфон 16
        # Метод: Chrome unique_contrib=0 на всех датасетах где структура появляется.
        # 132 структуры — 0 GT потерь гарантировано.
        # Комментируем, не удаляем — для других языков/рынков могут быть полезны.
        PREFIX_SKIP = {
            # G-группы
            "dbl_afterOP",        # [имплант,акб,кондиц,айфон]
            "dbl_afterS",         # [имплант,акб,кондиц,айфон]
            "dstar_afterStar",    # [имплант,акб,кондиц,айфон]
            "nostar_dstar",       # [имплант,акб,кондиц,айфон]
            "nostar_dstar_tr",    # [имплант,акб,кондиц,айфон]
            "rwc_afterOP",        # [имплант,акб,кондиц,айфон]
            "rwc_afterS",         # [имплант,акб,кондиц,айфон]
            "rwc_afterStar",      # [имплант,акб,кондиц,айфон]
            "sym_general_afterSym", # [имплант,акб,кондиц,айфон]
            "vP2_onOP_end",       # [имплант,акб,кондиц,айфон]
            "vP4_afterS",         # [имплант,акб,кондиц,айфон]
            "vP5_end",            # [акб,айфон]
            "wc_afterOP",         # [имплант,акб,кондиц,айфон]
            "wc_afterS",          # [имплант,акб,кондиц,айфон]
            # PA — cp0
            "а_cp0",              # [имплант,акб,кондиц,айфон]
            "й_cp0",              # [имплант,акб,кондиц,айфон]
            "с_cp0",              # [имплант,акб]
            "щ_cp0",              # [имплант,акб,кондиц,айфон]
            "ю_cp0",              # [имплант,акб,кондиц,айфон]
            # PA — wcB_cpMid
            "е_wcB_cpMid",        # [имплант,акб,кондиц,айфон]
            "и_wcB_cpMid",        # [имплант,акб,кондиц,айфон]
            "н_wcB_cpMid",        # [имплант,акб,кондиц,айфон]
            "о_wcB_cpMid",        # [имплант,акб,кондиц,айфон]
            "ю_wcB_cpMid",        # [имплант,акб,кондиц,айфон]
            "ё_wcB_cpMid",        # [имплант,акб,кондиц,айфон]
            # PA — Lwc_cpAL
            "а_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "б_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "е_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "и_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "й_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "м_Lwc_cpAL",         # [имплант,акб,кондиц]
            "т_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "ф_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "ч_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "ш_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "э_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            "ё_Lwc_cpAL",         # [имплант,акб,кондиц,айфон]
            # PA — Lwc_cpBL
            "б_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "д_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "и_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "й_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "к_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "л_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "м_Lwc_cpBL",         # [имплант,акб,кондиц]
            "с_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "у_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "х_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            "ш_Lwc_cpBL",         # [имплант,акб,кондиц,айфон]
            # PA — hyp_B_trail
            "а_hyp_B_trail",      # [имплант,акб]
            "в_hyp_B_trail",      # [акб]   ← Chrome; FF-вариант в PA_FF_STRUCTS жив
            "з_hyp_B_trail",      # [акб]
            "й_hyp_B_trail",      # [акб]
            "к_hyp_B_trail",      # [акб]
            "н_hyp_B_trail",      # [акб]
            "о_hyp_B_trail",      # [акб]
            "п_hyp_B_trail",      # [акб]
            "р_hyp_B_trail",      # [акб]
            "с_hyp_B_trail",      # [акб]
            "у_hyp_B_trail",      # [акб]
            "ф_hyp_B_trail",      # [акб]
            "ч_hyp_B_trail",      # [нигде]
            "ш_hyp_B_trail",      # [акб]
            "э_hyp_B_trail",      # [акб,айфон]
            "ю_hyp_B_trail",      # [акб,айфон]
            "я_hyp_B_trail",      # [акб,айфон]
            "ё_hyp_B_trail",      # [акб]
            # PA — hyp_Lwc
            "а_hyp_Lwc",          # [имплант,акб]
            "в_hyp_Lwc",          # [акб]   ← Chrome; FF-вариант жив
            "г_hyp_Lwc",          # [акб]
            "ж_hyp_Lwc",          # [акб]
            "з_hyp_Lwc",          # [нет данных — нигде]
            "к_hyp_Lwc",          # [акб]   ← Chrome; FF-вариант жив
            "л_hyp_Lwc",          # [акб]
            "н_hyp_Lwc",          # [акб]
            "о_hyp_Lwc",          # [акб]
            "п_hyp_Lwc",          # [акб]
            "р_hyp_Lwc",          # [акб]
            "с_hyp_Lwc",          # [акб]
            "у_hyp_Lwc",          # [акб]
            "ф_hyp_Lwc",          # [акб]
            "х_hyp_Lwc",          # [акб]
            "ц_hyp_Lwc",          # [акб]
            "ч_hyp_Lwc",          # [нет данных]
            "ш_hyp_Lwc",          # [акб]
            "ю_hyp_Lwc",          # [акб,айфон]
            "я_hyp_Lwc",          # [акб,айфон]
            "ё_hyp_Lwc",          # [акб]
            # PA — hyp_wcL
            "а_hyp_wcL",          # [имплант,акб]
            "б_hyp_wcL",          # [акб,кондиц,айфон]
            "в_hyp_wcL",          # [акб]   ← уточнить: нет в PA_FF_STRUCTS → Chrome
            "д_hyp_wcL",          # [акб]
            "е_hyp_wcL",          # [акб,айфон]
            "ж_hyp_wcL",          # [акб]
            "з_hyp_wcL",          # [акб]
            "и_hyp_wcL",          # [имплант,акб,кондиц]
            "й_hyp_wcL",          # [нет данных]
            "к_hyp_wcL",          # [нет данных]
            "л_hyp_wcL",          # [акб]
            "м_hyp_wcL",          # [акб]
            "н_hyp_wcL",          # [акб]
            "о_hyp_wcL",          # [акб]
            "п_hyp_wcL",          # [акб]
            "р_hyp_wcL",          # [акб]
            "с_hyp_wcL",          # [акб]
            "т_hyp_wcL",          # [акб]
            "у_hyp_wcL",          # [акб]
            "ф_hyp_wcL",          # [акб]
            "х_hyp_wcL",          # [акб]
            "ч_hyp_wcL",          # [акб]
            "ш_hyp_wcL",          # [акб]
            "щ_hyp_wcL",          # [нет данных]
            "э_hyp_wcL",          # [акб,айфон]
            "ю_hyp_wcL",          # [акб,айфон]
            "я_hyp_wcL",          # [акб,айфон]
            "ё_hyp_wcL",          # [акб]
            # PA — L_hyp
            "в_L_hyp",            # [акб,айфон]
            "г_L_hyp",            # [акб]
            "з_L_hyp",            # [акб]
            "и_L_hyp",            # [имплант,акб,кондиц]
            "й_L_hyp",            # [акб]
            "к_L_hyp",            # [акб]
            "л_L_hyp",            # [акб]
            "м_L_hyp",            # [акб]
            "н_L_hyp",            # [акб]
            "о_L_hyp",            # [акб]
            "п_L_hyp",            # [акб]
            "р_L_hyp",            # [акб]
            "т_L_hyp",            # [акб]
            "у_L_hyp",            # [акб]
            "ф_L_hyp",            # [акб]
            "х_L_hyp",            # [акб]
            "ч_L_hyp",            # [нет данных]
            "ш_L_hyp",            # [акб]
            "щ_L_hyp",            # [акб]
            "э_L_hyp",            # [акб,айфон]
            "ю_L_hyp",            # [акб,айфон]
            "я_L_hyp",            # [акб,айфон]
            "ё_L_hyp",            # [акб]
        }

        filtered = [pq for pq in out if pq.struct not in PREFIX_SKIP]
        return filtered

    def summary(self, queries: List[PrefixQuery]) -> dict:
        """Stats for tracer — mirrors suffix_generator.summary()"""
        by_group = {}
        for g in ALL_GROUPS:
            gq = [q for q in queries if q.group == g]
            by_group[g] = {"total": len(gq)}

        alpha_total    = sum(1 for q in queries if q.is_alpha)
        question_total = sum(1 for q in queries if q.is_question)
        chrome_total   = sum(1 for q in queries if "chrome"  in q.agents)
        firefox_total  = sum(1 for q in queries if "firefox" in q.agents)

        return {
            "total_queries":   len(queries),
            "chrome_queries":  chrome_total,
            "firefox_queries": firefox_total,
            "by_group":        by_group,
            "alpha_queries":   alpha_total,
            "question_queries": question_total,
            "groups_enabled":  list({q.group for q in queries}),
        }
