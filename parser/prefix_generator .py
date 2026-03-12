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

        BOTH   = ("chrome", "firefox")
        CHROME = ("chrome",)

        def q(group: str, struct: str, query: str, cp: int,
              cp_note: str, op_val: str = OP, op_t: str = ot,
              is_alpha: bool = False, is_q: bool = False,
              letter: str = None, agents: tuple = BOTH) -> PrefixQuery:
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
                         op_val="*", op_t="symbol"))
            out.append(q("G5", "pxwc_afterS",    b_star,
                         2 + len(S) + 1, "после S, перед конечным *",
                         op_val="*", op_t="symbol"))
            out.append(q("G5", "pxwc_trail",     b_trail,
                         len(b_trail), "trailing space (без конечного *)",
                         op_val="*", op_t="symbol"))

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

                # 1. cp0 — L S  cp=0 (курсор в начале строки)
                # По аналогии с kard_cp0: даёт слова ДО буквы (префиксы)
                out.append(q("PA", f"{L}_cp0", qstr,
                             0, "cp=0, начало строки",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 2. cp1 — L S  cp=1 (курсор после буквы)
                # По аналогии с kard_cp1: Google раскрывает букву в полное слово
                out.append(q("PA", f"{L}_cp1", qstr,
                             1, "cp=1, после буквы",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 3. wcB_cpMid — L * S  cp между * и S
                # (nocp, trail, sandwich удалены — 0 уникальных на 7 датасетах)
                qstr = f"{L} * {S}"
                # cp = после "L * " = len(L) + 3
                out.append(q("PA", f"{L}_wcB_cpMid", qstr,
                             len(L) + 3, "cp между * и S",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 6. Lwc_cpAL — L S *  cp после "L S " перед *
                qstr = f"{L} {S} *"
                out.append(q("PA", f"{L}_Lwc_cpAL", qstr,
                             len(L) + 1 + len(S) + 1, "cp после S, перед *",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 7. Lwc_cpBL — L S *  cp после "L " перед S
                out.append(q("PA", f"{L}_Lwc_cpBL", qstr,
                             len(L) + 1, "cp после L, перед S",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 8. hyp_B_trail — L - S <space>  (col_B_trail и L_col удалены — нестабильны)
                qstr = f"{L} - {S} "
                out.append(q("PA", f"{L}_hyp_B_trail", qstr,
                             len(qstr), "дефис + буква, trailing space",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 9.  hyp_Lwc — L - S *
                qstr = f"{L} - {S} *"
                out.append(q("PA", f"{L}_hyp_Lwc", qstr,
                             len(qstr), "дефис + буква + *",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 10. hyp_wcL — L - * S
                qstr = f"{L} - * {S}"
                out.append(q("PA", f"{L}_hyp_wcL", qstr,
                             len(qstr), "дефис + * + S",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

                # 11. L_hyp — L S -
                qstr = f"{L} {S} -"
                out.append(q("PA", f"{L}_L_hyp", qstr,
                             len(qstr), "буква + S + дефис",
                             op_val=L, op_t="letter",
                             is_alpha=True, letter=L, agents=CHROME))

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
                             is_q=True))
                # vP2: cp на конце слова вопроса
                out.append(q("PC", f"{qw}_vP2", base,
                             len(qw), "на конце вопроса",
                             op_val=qw, op_t="question",
                             is_q=True))
                # vP3: только для "почему" — в суффиксе v1/v2 всегда empty, v3 даёт 6
                if qw == "почему":
                    out.append(q("PC", f"{qw}_vP3", base,
                                 len(qw) + 1 + len(S) + 1, "после S, перед * (только почему)",
                                 op_val=qw, op_t="question",
                                 is_q=True))

        return out

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
