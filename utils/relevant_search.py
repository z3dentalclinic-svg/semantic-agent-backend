# relevant_search.py
# Релевантный поиск — прод-версия (финал калибровки, схема FAMILIES).
# Вход: сид. Выход: до TOP_N переформулировок для параллельного парсинга.
#
# Прод-конфигурация (утверждена):
#   генератор gemini-3.6-flash thinkingLevel=medium, кластер gemini-3.7-flash thinkingBudget=768
#   (связка 1.0 — 3.7 оба, b768/b512 — закомментирована в конфиге как точка отката)
#   лин-промпт кластера, откат на полный JSON при парс-фейле, аудит семей выключен
#
# Публичный вход: await relevant_variants(seed, client) -> dict
#   {"seed", "final": [...], "families": [...], "candidates": [...], "stats": {...}}
# Калибровочный стенд (relevant_search_test.py) остаётся отдельно, к проду не подключён.

import os
import re
import json
import time
import asyncio
import httpx
import pymorphy3

# ---------------- конфиг ----------------
# Связка 1.0 (точка отката): 3.7-flash оба этапа, генератор b768, кластер b512.
# Прогон «доставка цветов» показал шум генератора («купить цветов», класс A, 3 голоса)
# и пропуск кластера на b512 → генератор возвращён на 3.6/medium (Andrew: работал
# нормально), кластеру бюджет поднят до 768 для перестраховки.
# GEN_MODEL = "gemini-3.7-flash"
# GEN_THINKING = 768
GEN_MODEL = "gemini-3.6-flash"
GEN_THINKING = "medium"        # 3.6: thinking уровнем (thinkingLevel), не бюджетом
VER_MODEL = "gemini-3.7-flash"
# VER_THINKING = 512
VER_THINKING = 768
# $/1M input, output по моделям — правь под актуальный прайс
PRICES = {"gemini-3.7-flash": (0.75, 3.75), "gemini-3.6-flash": (1.50, 7.50)}

GEN_RUNS = 3          # прогонов генерации на каждый класс (A токенные, B свободные)
TOP_N = 3             # потолок вариантов в работу; добор запрещён
# BUILD = "relevant_search_prod_1.0"
# BUILD = "relevant_search_prod_1.1 (gen 3.6/medium, cluster 3.7/b768)"
# BUILD = "relevant_search_prod_1.2 (gen prompts: same-intent requirement)"
# BUILD = "relevant_search_prod_1.2.1 (+ input sanitizer: list markers stripped)"
# BUILD = "relevant_search_prod_1.3 (cluster lean V2: immediate-operation criterion)"
# BUILD = "relevant_search_prod_1.4 (+ derivative-of-seed code cut; reps ordered by real usage)"
# BUILD = "relevant_search_prod_1.5 (+ seed+adverb code cut; translit/translation of seed word -> group 0)"
# BUILD = "relevant_search_prod_1.6 (+ merge-audit: attach-verb families collapse to one slot)"
# BUILD = "relevant_search_prod_1.6.1 (merge-audit also collapses word-extension and translit slots)"
# BUILD = "relevant_search_prod_1.7 (cluster: 3-vote aggregation + code root-split of families)"
# BUILD = "relevant_search_prod_1.7.1 (root-split: strict full-matching, translit-bridge fixed)"
# BUILD = "relevant_search_prod_1.7.2 (merge guard: slot must root-contain all seed words)"
# BUILD = "relevant_search_prod_1.8 (axis-diverse final pick: cover different replaced seed words)"
# BUILD = "relevant_search_prod_1.8.1 (axis novelty = uncovered seed lemma, not new lemma-set)"
# BUILD = "relevant_search_prod_1.8.2 (additions get own axes; merge ranks colloquial last)"
# BUILD = "relevant_search_prod_1.9 (hard cut: any adverb/pronoun beyond seed kills candidate)"
# BUILD = "relevant_search_prod_1.10 (question-word ban in gen prompts + stop-word list cut)"
BUILD = "relevant_search_prod_1.11 (+ recycle: <=1 variant -> one fresh regeneration)"
RECYCLE_MIN_VARIANTS = 2   # меньше — свежий повтор генерации (один раз)

GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
# Пул ключей при параллельных сидах — хвост интеграции, пока один ключ.

# ---------------- промпты (утверждены) ----------------
GEN_PROMPT = """Запрос пользователя: "{seed}"

Разбери запрос на слова. Для каждого слова укажи роль:
- obj (предмет), act (действие), app (к чему относится),
  comm (цена/купить/отзывы и т.п.), geo (город/страна),
  brand (бренд/модель), num (число/характеристика), func (предлог/союз)

Для ролей geo, brand, num, func замен НЕ давать — пустой список.

Для остальных слов дай 0-3 замены. Замена — это слово, которым
другой реальный человек заменил бы это слово, ища ТО ЖЕ САМОЕ
в Google. Проверяй подстановкой: замена должна встать на место
исходного слова в "{seed}" так, чтобы запрос искали те же люди
с той же целью.

Замена обязана сохранять тот же поисковый интент, что у
"{seed}": ту же услугу или цель и тот же этап пути к ней.
Слово другого этапа или смежной цели — не замена.
Не давай вопросительных слов (что, где, когда, куда, сколько,
как, почему) — если только они не входят в сам "{seed}".

НЕ давать:
- оценки и модификаторы (дешево, лучший, срочно, рядом)
- обобщения (устройство вместо конкретного предмета)
- сужения (конкретная модель вместо общего слова)
- слова, меняющие цель поиска

Давай только замены, которые сам видел в реальных
поисковых запросах или названиях товаров.

Ответ строго JSON без пояснений:
{{"tokens":[{{"word":"...","role":"...","subs":["..."]}}]}}"""

GEN_B_PROMPT = """Запрос пользователя в Google: "{seed}"

Напиши 4-7 запросов, которыми реальный человек ищет
ТО ЖЕ САМОЕ другими словами. Полные фразы целиком,
как их вводят в поиск.

Каждый запрос обязан сохранять тот же поисковый интент,
что "{seed}": ту же услугу или цель и тот же этап пути
к ней. Запрос про смежную цель или другой этап не годится.
Не пиши вопросительных запросов и запросов со словами
вопроса (что, где, когда, куда, сколько, как, почему) —
если только сам "{seed}" не является вопросом.

Используй разные формы:
- через глагол (что человек хочет сделать)
- через название самой услуги или предмета
- через того, кто эту услугу оказывает или где
  этот предмет берут

Не менять: города, бренды, модели, числа.
Не добавлять: оценки и модификаторы (дешево, лучший,
срочно, рядом).
Не конкретизируй предмет: если в запросе не сказано,
что именно доставляют/ремонтируют/ищут — в твоих
запросах этого тоже нет.

Давай только запросы, которые сам видел в реальном
поиске или похожие на них по форме.

Ответ строго JSON без пояснений:
{{"queries":["...","..."]}}"""

# Лин-промпт кластера (прод)
# V1 (точка отката): критерий «другая услуга или другой этап пути» — пропускал «заказ цветов»,
# см. brief_intent_leak.md; дословный текст V1 — в git/стенде.
# Лин-промпт кластера V2 (консилиум 4 моделей, правка узла 1/3):
# сравнение НЕПОСРЕДСТВЕННОЙ ОПЕРАЦИИ, а не общей цели; шкала отношений внутри,
# наружу прежний лин-формат. Якорь: операция сида должна остаться названной в кандидате;
# действие вокруг сохранённой операции («заказать ремонт») — валид, подмена операции
# («заказ» вместо «доставки») — группа 0.
FAM_PROMPT_PROD = """Отбери переформулировки запроса для Google Autocomplete и раздели их на группы.

Сид: "{seed}"

Кандидаты:
{candidates}

Шаг 1. Определи непосредственную операцию сида: что пользователь просит
найти этим запросом прямо сейчас, а не его конечную жизненную цель.

Шаг 2. Для каждого кандидата определи его непосредственную операцию
и отношение к операции сида: ТА ЖЕ / ПОДГОТОВКА / СЛЕДУЮЩИЙ ШАГ /
СОСЕДНЯЯ / ДРУГАЯ.

Проходит только ТА ЖЕ: операция сида в кандидате осталась — названа
тем же словом, его формой, синонимом этого действия, его исполнителем
или каналом её исполнения. Слова, добавленные вокруг сохранённой
операции (в том числе её оформление), не мешают.

Не проходит (группа 0): операция сида исчезла и заменена действием
вокруг неё — оформлением, приобретением, выбором продавца,
самостоятельным выполнением — или другой операцией. Даже если конечная
цель совпадает. Даже если кандидат — необходимый шаг к операции сида.
Общая цепочка действий не делает операции одинаковыми.
Также группа 0: потерян объект сида (замена синонимом допустима),
изменилось место или направление, добавлена оценка.
Также группа 0: кандидат отличается от сида только записью того же
слова на другом языке или алфавите (перевод названия, транслитерация,
латиница вместо кириллицы) — это тот же вход поиска, не новый вариант.

Шаг 3. Прошедших раздели на группы по отличающему слову: одна группа =
одно отличающее слово в его формах или с расширением; разные слова —
разные группы, даже синонимы. В каждой группе укажи представителя —
самый естественный запрос.

Номера представителей перечисли в порядке употребимости в реальном
поиске: сначала обычные формулировки, которыми люди действительно
ищут; разговорные, книжные и редкие словоформы — в конец.

Ответь одной строкой без пояснений: номер группы для каждого кандидата
по порядку через запятую (0 = не прошёл), затем | и номера представителей.
Формат: 1,1,2,0,3|1,4,6"""

# Полный JSON-промпт кластера — откат при парс-фейле лин-ответа (режектор 3/3, кластер 3/3 заморожены)
FAM_PROMPT = """Мы собираем разные входы для Google Autocomplete.

Исходный запрос: "{seed}"

Кандидаты — переформулировки этого запроса:
{candidates}

Шаг 1. Найди в исходном запросе главное слово услуги или
действия — это якорь интента.

Кандидат сохраняет интент, только если ищет ТУ ЖЕ услугу:
- тем же словом, его формой или синонимом того же действия
- через организацию или место, где эту услугу оказывают
- через способ или канал выполнения этой услуги
- с другим объектом той же услуги

Отбрось в "rejected" кандидатов про ДРУГУЮ услугу или про
другой этап пути к результату — даже смежный, даже ведущий
к тому же итогу: смежный интент — не наш интент.
Отбрось кандидатов, потерявших ОБЪЕКТ исходного запроса:
объект можно заменить синонимом или родственным словом,
но без объекта запрос расширяется на другие области и
охватывает чужие интенты.
Также отбрось: меняется место или направление действия,
добавлена оценка.

Шаг 2. Остальных сгруппируй в семьи.

Важно: у всех оставшихся кандидатов цель и интент УЖЕ
одинаковы — это проверено раньше. Общая цель — НЕ причина
объединять в одну семью.

Критерий семьи — лексический. Найди у каждого кандидата
слово, которым он отличается от исходного запроса.

Одна семья = одно и то же отличающее слово в любых видах:
- его формы: падеж, число, глагол и существительное от
  того же слова (одно действие в двух формах)
- его расширения: то же слово плюс уточняющее слово

РАЗНЫЕ отличающие слова = РАЗНЫЕ семьи. Даже если слова
синонимы. Даже если оба — глаголы одного действия разными
словами. Автокомплит продолжает разные строки разными
подсказками, поэтому разные слова — всегда отдельные семьи.

Не создавай семьи ради количества, но и не сливай разные
слова в одну семью. В каждой семье укажи одного
представителя — самый естественный и частотный запрос
в реальном поиске.

Ответ строго JSON без пояснений:
{{"rejected":[номера],
"families":[{{"scenario":"до 10 слов","members":[номера],"representative":номер,"reason":"кратко почему остальные не дают отдельный слот"}}]}}"""

# Мерж-аудит представителей (схема вместо 4-й правки лин-промпта, Andrew 2026-08-23):
# один вызов после кластера, вход — только представители семей. Склеивает семьи,
# различающиеся лишь глаголом-обвязкой вокруг общего ядра сида («поставить/вставить/
# установить зубной имплант» → один вход). Разные услуги/исполнители не склеивает
# («починить»/«отремонтировать», «ремонт»/«сервис» — разные входы, калибровка Andrew).
# Риск асимметричен: перелив = потеря слабого варианта, не мусор.
MERGE_PROMPT = """Проверь список строк — кандидаты на разные входы Google Autocomplete для одного запроса.

Сид: "{seed}"

Кандидаты:
{reps}

Два кандидата дают ОДИН вход, когда у них одно смысловое ядро, содержащее
все значимые слова сида (в любых формах или однокоренных вариантах),
а различаются они только глаголом-обвязкой — действием над этим ядром
(например, поставить / вставить / установить): продолжения автокомплита
у таких строк совпадают.

Также ОДИН вход, когда отличающее слово у кандидатов одно и то же:
у одного оно с уточняющим словом (расширением), или записано другим
языком или алфавитом, или в другой форме. Расширение и запись слова —
не новый вход.

НЕ склеивай кандидатов, если:
- различающее слово само называет услугу или действие-услугу — разные
  услуги остаются разными входами, даже синонимы;
- различающее слово называет исполнителя или место — это отдельный вход;
- различие в предмете или его уточнении.

Сомневаешься — не склеивай.

Ответь одной строкой без пояснений: номер входа для каждого кандидата
по порядку через запятую, затем | и по одному представителю каждого
входа — самому употребимому в реальном поиске — через запятую,
в порядке употребимости. Разговорные, просторечные и редкие
словоформы ставь после стандартных названий, даже если они
звучат естественно.
Формат: 1,1,2,3|1,4,5"""


# Аудит семей — в проде ВЫКЛЮЧЕН (точка отката, не удалять)
FAM_AUDIT_PROMPT = """Проверь одну группу поисковых формулировок.

Исходный запрос: "{seed}"

Группа:
{members}

Все варианты относятся к общей цели исходного запроса.
Вопрос только один: являются ли они взаимозаменяемым
первым вводом одного и того же поиска с практически
одинаковыми продолжениями?

Общая цель — не причина держать их вместе: цель у всех
одна по условию. Критерий лексический: одна семья = одно
и то же ключевое слово в разных формах (падеж, число,
глагол/существительное того же слова) или с расширением
(то же слово плюс уточняющее).
"split" — если в группе есть кандидаты, построенные на
РАЗНЫХ словах (даже синонимах): разные слова дают разные
подсказки.
"keep" — если все кандидаты построены на одном слове.
Пары «одно слово в двух формах» и «слово и его расширение»
не разделяй никогда.

Ответ строго JSON без пояснений:
{{"decision":"keep","groups":[[номера],[номера]],"reason":"до 14 слов"}}
где decision = "keep" или "split"; groups заполняй только при split."""


# ---------------- морфология / кодовые отсевы ----------------
FROZEN_ROLES = {"geo", "brand", "num", "func"}
MORPH = pymorphy3.MorphAnalyzer()
FUNC_POS = {"PREP", "CONJ", "PRCL"}


def norm(s):
    return re.sub(r"\s+", " ", s.strip().lower())


def sanitize_text(q):
    """Срез ведущих маркеров списка/нумерации у сида, замен и фраз («• x», «- x», «1. x»)."""
    q = str(q).strip()
    q = re.sub(r"^[^0-9a-zA-Z\u0400-\u04FF]+", "", q)
    q = re.sub(r"^\d+[.)]\s+", "", q)
    return q.strip()


def split_tokens(text):
    """Два мультимножества: значимые токены и служебные (предлог/союз/частица)."""
    content, func = {}, {}
    for w in norm(text).split():
        pos = MORPH.parse(w)[0].tag.POS
        d = func if pos in FUNC_POS else content
        d[w] = d.get(w, 0) + 1
    return content, func


def lemma_tokens(text):
    """Мультимножество лемм значимых токенов — для отсева морфовариантов сида."""
    out = {}
    for w in norm(text).split():
        p = MORPH.parse(w)[0]
        if p.tag.POS in FUNC_POS:
            continue
        lm = p.normal_form
        out[lm] = out.get(lm, 0) + 1
    return out


def _same_root(a, b):
    """Словообразовательное родство двух лемм: общий префикс достаточной длины
    («имплантация»/«имплантирование», «доставка»/«доставить», «заказ»/«заказать»).
    Без словарей: только длина общей основы."""
    p = 0
    for x, y in zip(a, b):
        if x != y:
            break
        p += 1
    m = min(len(a), len(b))
    # одно слово — префикс другого («цвет»/«цветок») или длинная общая основа
    return (p >= 3 and p == m) or (p >= 5 and p >= 0.6 * m)


AUX_POS = {"ADVB", "NPRO", "PRED"}   # где/как/куда, местоимения, предикативы — довесок, не новое слово

# Служебные и вопросительные слова — единый источник: parser.stop_words в main.py
# (копия; при изменении там — синхронизировать здесь). Список продиктован Andrew:
# правило работает по СЛОВАМ, не по POS-классам (решение 2026-08-23 после отката POS-правила).
STOP_WORDS_UNION = frozenset().union(
    {'и', 'в', 'во', 'не', 'на', 'с', 'от', 'для', 'по', 'о', 'об', 'к', 'у', 'за',
     'из', 'со', 'до', 'при', 'без', 'над', 'под', 'а', 'но', 'да', 'или', 'чтобы',
     'что', 'как', 'где', 'когда', 'куда', 'откуда', 'почему', 'сколько', 'зачем'},
    {'і', 'від', 'але', 'та', 'або', 'що', 'як', 'де', 'коли', 'куди', 'звідки', 'чому', 'скільки'},
    {'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'with', 'by', 'from', 'up', 'about',
     'into', 'through', 'during', 'and', 'or', 'but', 'when', 'where', 'how', 'why', 'what'},
)


def stopword_beyond_seed(variant, seed_tokens):
    """True, если у кандидата есть служебное/вопросительное слово из списка,
    которого нет в самом сиде (сид-вопрос сохраняет право на свои слова)."""
    return any(w in STOP_WORDS_UNION and w not in seed_tokens
               for w in norm(variant).split())


def aux_lemmas(text):
    """Мультимножество наречных/местоименных/предикативных лемм фразы."""
    out = {}
    for w in norm(text).split():
        p = MORPH.parse(w)[0]
        if p.tag.POS in AUX_POS:
            lm = p.normal_form
            out[lm] = out.get(lm, 0) + 1
    return out


def _aux_beyond_seed(variant, seed_aux):
    """True, если у кандидата есть наречие/местоимение/предикатив сверх сидовых."""
    v = aux_lemmas(variant)
    return any(c > seed_aux.get(l, 0) for l, c in v.items())


# is_seed_plus_aux (вхождение сида + довесок) вытеснен жёстким _aux_beyond_seed —
# оставлен как точка отката.
def is_seed_plus_aux(variant, seed_lemmas):
    """Кандидат = сид целиком + только вопросительные/наречные/местоименные довески:
    «где купить айфон 16», «купить айфон 16 недорого» при сиде «купить айфон 16».
    Вход тот же (довесок — вопрос или оценка, «оценка» и так группа 0 у кластера) —
    не вариант. Определение по POS pymorphy, без словарей слов."""
    v = lemma_tokens(variant)
    if any(v.get(l, 0) < c for l, c in seed_lemmas.items()):
        return False   # сид не содержится целиком
    extra = []
    for l, c in v.items():
        d = c - seed_lemmas.get(l, 0)
        extra.extend([l] * d)
    if not extra:
        return False   # это морфовариант, его ловит другое правило
    return all((MORPH.parse(l)[0].tag.POS in AUX_POS) for l in extra)


def is_word_derivative(variant_lemmas, seed_lemmas):
    """Кандидат = сид, в котором одно слово заменено словообразовательной формой
    того же слова («имплантирование зубов» при сиде «имплантация зубов»).
    По лексическому критерию семья такого кандидата — сам сид, отдельного входа
    автокомплита он не даёт. Внедрён по решению Andrew (2026-08-23)."""
    if sum(variant_lemmas.values()) != sum(seed_lemmas.values()):
        return False
    v_only = [l for l, c in variant_lemmas.items() for _ in range(c - seed_lemmas.get(l, 0)) if c > seed_lemmas.get(l, 0)]
    s_only = [l for l, c in seed_lemmas.items() for _ in range(c - variant_lemmas.get(l, 0)) if c > variant_lemmas.get(l, 0)]
    if not v_only or len(v_only) != len(s_only) or len(v_only) > 3:
        return False
    # Полное паросочетание отличающихся лемм по корню: каждая пара должна быть
    # словообразовательным родством. Лемма-шум pymorphy («цветов»→цвет,
    # «цветы»→цветок) — тоже однокоренная пара, поэтому требование «ровно одна
    # отличающаяся пара» давало ложные пропуски.
    from itertools import permutations
    for perm in permutations(s_only):
        if all(_same_root(v, sl) for v, sl in zip(v_only, perm)):
            return True
    return False


# ---------------- вызовы модели ----------------
RETRYABLE = {429, 500, 502, 503, 504}


def _clean_err(e):
    s = str(e)
    if GEMINI_KEY:
        s = s.replace(GEMINI_KEY, "***")
    return s


async def call_llm(client, model, thinking, prompt, _retries=2):
    """thinking: int → thinkingBudget (3.7), str → thinkingLevel (3.6: minimal|low|medium|high)."""
    for attempt in range(_retries + 1):
        try:
            return await _call_llm_once(client, model, thinking, prompt)
        except httpx.HTTPStatusError as e:
            if attempt < _retries and e.response.status_code in RETRYABLE:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(_clean_err(e)) from None
        except httpx.TimeoutException as e:
            if attempt < _retries:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(_clean_err(e)) from None


async def _call_llm_once(client, model, thinking, prompt):
    t0 = time.time()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_KEY}"
    tc = ({"thinkingLevel": thinking} if isinstance(thinking, str)
          else {"thinkingBudget": int(thinking)})
    body = {"contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"thinkingConfig": tc}}
    r = await client.post(url, json=body, timeout=90)
    r.raise_for_status()
    d = r.json()
    text = "".join(p.get("text", "") for p in d["candidates"][0]["content"]["parts"])
    um = d.get("usageMetadata", {})
    tin = um.get("promptTokenCount", 0)
    tout = um.get("candidatesTokenCount", 0) + um.get("thoughtsTokenCount", 0)
    price = PRICES.get(model, (0.75, 3.75))
    cost = tin / 1e6 * price[0] + tout / 1e6 * price[1]
    return {"text": text, "tin": tin, "tout": tout, "cost": cost, "wall": time.time() - t0}


def parse_json_block(text):
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _parse_lean_cluster(text, n):
    """'1,1,2,0,3|1,4,6' -> (группы по кандидатам, номера представителей) или None."""
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or not any(ch.isdigit() for ch in line):
            continue
        parts = line.split("|")
        try:
            groups = [int(x) for x in parts[0].replace(" ", "").split(",") if x != ""]
        except ValueError:
            continue
        if len(groups) != n:
            continue
        reps = []
        if len(parts) > 1:
            try:
                reps = [int(x) for x in parts[1].replace(" ", "").split(",") if x != ""]
            except ValueError:
                reps = []
        return groups, reps
    return None


# ---------------- генерация кандидатов ----------------
def build_variants(seed, tokens):
    """Одиночная замена токена. Возвращает [(variant, sub_word, orig_word)] в порядке появления."""
    words = seed.split()
    out = []
    for tk in tokens:
        if tk.get("role") in FROZEN_ROLES:
            continue
        w = tk.get("word", "")
        if w not in words:
            continue
        idx = words.index(w)
        for sub in tk.get("subs", [])[:3]:
            sub = sanitize_text(sub)
            if not sub or norm(sub) == norm(w):
                continue
            v = words.copy()
            v[idx] = sub
            out.append((" ".join(v), sub, w))
    return out


async def _generate_candidates(client, seed, stats):
    """3 прогона класса A (токенные замены) + 3 прогона класса B (свободные фразы), все параллельно.
    Кодовые отсевы до кластера: паритет служебных слов, перестановка сида, морфовариант сида."""
    gen_prompt = GEN_PROMPT.format(seed=seed)
    gen_b_prompt = GEN_B_PROMPT.format(seed=seed)
    all_runs = await asyncio.gather(
        *[call_llm(client, GEN_MODEL, GEN_THINKING, gen_prompt) for _ in range(GEN_RUNS)],
        *[call_llm(client, GEN_MODEL, GEN_THINKING, gen_b_prompt) for _ in range(GEN_RUNS)],
        return_exceptions=True)
    runs, runs_b = all_runs[:GEN_RUNS], all_runs[GEN_RUNS:]

    candidates = {}   # norm_variant -> {"variant","votes","positions","sub","orig","cls"}
    errors = []
    seed_content, seed_func = split_tokens(seed)
    seed_lemmas = lemma_tokens(seed)
    seed_token_set = set(norm(seed).split())

    def add_candidate(variant, pos, cls, sub="", orig=""):
        k = norm(variant)
        if k == seed:
            return
        v_content, v_func = split_tokens(variant)
        if v_func != seed_func:
            return  # служебные слова должны совпадать с сидом: добавленный предлог = не вариант
        if v_content == seed_content:
            return  # сид перестановкой — не вариант
        v_lemmas = lemma_tokens(variant)
        if v_lemmas == seed_lemmas:
            return  # морфовариант сида (падеж/число) — не вариант
        if is_word_derivative(v_lemmas, seed_lemmas):
            return  # словообразовательная форма слова сида — семья сида, не отдельный вход
        # Срез по СПИСКУ служебных/вопросительных слов (Andrew): слово из списка,
        # отсутствующее в сиде, убивает кандидата сразу — без POS-классов и без
        # проверки вхождения сида (транслит-обход «где купить iphone 16» закрыт).
        # POS-правило 1.9 (_aux_beyond_seed) откачено: резало наречия без команды.
        if stopword_beyond_seed(variant, seed_token_set):
            return
        c = candidates.setdefault(k, {"variant": variant, "votes": 0, "positions": [],
                                      "sub": sub, "orig": orig, "cls": set()})
        c["votes"] += 1
        c["positions"].append(pos)
        c["cls"].add(cls)
        if cls == "A" and sub:
            c["sub"], c["orig"] = sub, orig

    def account(res):
        stats["gen"]["tin"] += res["tin"]; stats["gen"]["tout"] += res["tout"]
        stats["gen"]["cost"] += res["cost"]; stats["gen"]["wall"] = max(stats["gen"]["wall"], res["wall"])

    for res in runs:
        if isinstance(res, Exception):
            errors.append(f"gen_a: {res}")
            continue
        account(res)
        data = parse_json_block(res["text"])
        if not data or "tokens" not in data:
            errors.append("gen_a: parse_fail")
            continue
        for pos, (variant, sub, orig) in enumerate(build_variants(seed, data["tokens"]), start=1):
            add_candidate(variant, pos, "A", sub, orig)

    for res in runs_b:
        if isinstance(res, Exception):
            errors.append(f"gen_b: {res}")
            continue
        account(res)
        data = parse_json_block(res["text"])
        if not data or "queries" not in data:
            errors.append("gen_b: parse_fail")
            continue
        for pos, q in enumerate([sanitize_text(q) for q in data["queries"]][:7], start=1):
            if q:
                add_candidate(q, pos, "B")

    return candidates, errors


# ---------------- кластеризация в семьи ----------------
CLUSTER_VOTES = 3   # параллельных вызовов лин-кластера; вердикты агрегируются большинством


async def _cluster_families(client, seed, candidates, stats):
    """3 параллельных вызова лин-кластера, агрегация большинством (группа 0 и пары семей);
    один валидный ответ — работаем по нему; ноль — откат на полный JSON-промпт.
    Систематическую склейку синонимов модель чинит не голосование, а кодовый
    пост-сплит семей по корням (ниже). Возвращает (rejected, families)."""
    keys = list(candidates.keys())
    rejected, families = set(), []
    model_rep_order = {}   # key -> позиция в списке представителей от модели (порядок употребимости)
    if not keys:
        return rejected, families

    def cand_rank(k):
        c = candidates[k]
        return (-c["votes"], sum(c["positions"]) / len(c["positions"]))

    def account(res):
        stats["ver"]["tin"] += res["tin"]; stats["ver"]["tout"] += res["tout"]
        stats["ver"]["cost"] += res["cost"]

    listing = "\n".join(f"{i+1}. {candidates[k]['variant']}" for i, k in enumerate(keys))
    calls = await asyncio.gather(
        *[call_llm(client, VER_MODEL, VER_THINKING,
                   FAM_PROMPT_PROD.format(seed=seed, candidates=listing))
          for _ in range(CLUSTER_VOTES)],
        return_exceptions=True)
    parses = []
    for res in calls:
        if isinstance(res, Exception):
            continue
        account(res)
        lean = _parse_lean_cluster(res["text"], len(keys))
        if lean is not None:
            parses.append(lean)
    stats["ver"]["cluster_votes_ok"] = len(parses)

    n = len(keys)
    if not parses:
        stats["ver"]["fallback_json"] = True
        res = await call_llm(client, VER_MODEL, VER_THINKING,
                             FAM_PROMPT.format(seed=seed, candidates=listing))
        account(res)
        data = parse_json_block(res["text"]) or {}
    else:
        need = len(parses) // 2 + 1   # большинство валидных ответов
        zero_cnt = [0] * n
        pair_cnt = {}
        rep_pos = {}                  # idx -> [позиции в списке представителей]
        for groups, reps in parses:
            for i, g in enumerate(groups):
                if g == 0:
                    zero_cnt[i] += 1
            by_g = {}
            for i, g in enumerate(groups):
                if g > 0:
                    by_g.setdefault(g, []).append(i)
            for mem in by_g.values():
                for a in range(len(mem)):
                    for b in range(a + 1, len(mem)):
                        pair_cnt[(mem[a], mem[b])] = pair_cnt.get((mem[a], mem[b]), 0) + 1
            for pos, r in enumerate(reps):
                if 1 <= r <= n:
                    rep_pos.setdefault(r - 1, []).append(pos)
        rejected_idx = {i for i in range(n) if zero_cnt[i] >= need}
        # компоненты по парам-большинству среди неотсеянных
        parent = list(range(n))
        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]; x = parent[x]
            return x
        for (a, b), c in pair_cnt.items():
            if c >= need and a not in rejected_idx and b not in rejected_idx:
                parent[find(a)] = find(b)
        comps = {}
        for i in range(n):
            if i not in rejected_idx:
                comps.setdefault(find(i), []).append(i)
        def rep_score(i):
            ps = rep_pos.get(i)
            return (0, sum(ps) / len(ps)) if ps else (1, 0)
        fams = []
        for mem in comps.values():
            r = min(mem, key=lambda i: (rep_score(i), cand_rank(keys[i])))
            fams.append({"members": [m + 1 for m in mem], "representative": r + 1})
        for f in fams:
            i = f["representative"] - 1
            if i in rep_pos:
                model_rep_order[keys[i]] = sum(rep_pos[i]) / len(rep_pos[i])
        data = {"rejected": [i + 1 for i in sorted(rejected_idx)], "families": fams}

    def to_keys(nums):
        out = []
        for n in nums or []:
            try:
                idx = int(n) - 1
            except (ValueError, TypeError):
                continue
            if 0 <= idx < len(keys):
                out.append(keys[idx])
        return out

    rejected = set(to_keys(data.get("rejected")))
    seen = set(rejected)
    for f in data.get("families", []):
        members = [k for k in to_keys(f.get("members")) if k not in seen]
        if not members:
            continue
        seen.update(members)
        rep = to_keys([f.get("representative")])
        rep = rep[0] if rep and rep[0] in members else min(members, key=cand_rank)
        families.append({"scenario": str(f.get("scenario", "")), "members": members,
                         "rep": rep, "reason": str(f.get("reason", ""))})
    for k in keys:
        if k not in seen:
            families.append({"scenario": "(вне кластеризации)", "members": [k],
                             "rep": k, "reason": "модель не отнесла к семье"})

    # === АУДИТ СЕМЕЙ ВЫКЛЮЧЕН в проде (Andrew). Не удалять — точка отката. ===
    # async def audit(fi):
    #     fam = families[fi]
    #     mem_list = "\n".join(f"{i+1}. {candidates[k]['variant']}" for i, k in enumerate(fam["members"]))
    #     res = await call_llm(client, VER_MODEL, VER_THINKING,
    #                          FAM_AUDIT_PROMPT.format(seed=seed, members=mem_list))
    #     account(res)
    #     d = parse_json_block(res["text"]) or {}
    #     return fi, d
    # audit_ids = [i for i, f in enumerate(families) if len(f["members"]) >= 2]
    # results = await asyncio.gather(*[audit(i) for i in audit_ids], return_exceptions=True)
    # split_plan = {}
    # for r in results:
    #     if isinstance(r, Exception):
    #         continue
    #     fi, d = r
    #     if str(d.get("decision", "")).strip().lower() == "split" and d.get("groups"):
    #         split_plan[fi] = d
    # if split_plan:
    #     ... (полная логика дробления — в relevant_search_test.py / истории git)

    # ── Кодовый пост-сплит: закон «разные слова — разные семьи» доисполняет код ──
    # Модель систематически склеивает синонимы одного действия («заказать»+«приобрести»).
    # Члены остаются вместе, только если их отличающие от сида леммы связаны корнем
    # или общим словом (расширение); иначе семья механически режется по корневым группам.
    seed_lm = lemma_tokens(seed)

    def diff_lemmas(k):
        v = lemma_tokens(candidates[k]["variant"])
        return [l for l, c in v.items() for _ in range(c - seed_lm.get(l, 0)) if c > seed_lm.get(l, 0)]

    def related(d1, d2):
        """Связь = буква закона «одно отличающее слово (в формах или с расширением)»:
        ВСЕ отличия меньшего множества паросочетаются с отличиями большего по
        равенству/корню; лишние леммы большего — расширение, допустимо.
        Частичное пересечение («заказать iphone»/«приобрести iphone» через мост
        iphone) связью НЕ является — мост склеивал разные слова в одну семью."""
        a, b = (d1, d2) if len(d1) <= len(d2) else (d2, d1)
        if not a:
            return True
        from itertools import permutations
        for perm in permutations(b, len(a)):
            if all(x == y or _same_root(x, y) for x, y in zip(a, perm)):
                return True
        return False

    split_out = []
    for f in families:
        if len(f["members"]) < 2:
            split_out.append(f)
            continue
        diffs = {k: diff_lemmas(k) for k in f["members"]}
        groups_ = []
        for k in f["members"]:
            for g in groups_:
                if any(related(diffs[k], diffs[m]) for m in g):
                    g.append(k)
                    break
            else:
                groups_.append([k])
        if len(groups_) == 1:
            split_out.append(f)
            continue
        for g in groups_:
            r = f["rep"] if f["rep"] in g else min(g, key=cand_rank)
            split_out.append({"scenario": f["scenario"], "members": g, "rep": r,
                              "reason": (f["reason"] + " " if f["reason"] else "") + "(сплит по корню)"
                              if r != f["rep"] else f["reason"]})
    if len(split_out) != len(families):
        stats["ver"]["root_split"] = {"in": len(families), "out": len(split_out)}
    families = split_out

    # Порядок семей: употребимость от модели (лин-ответ) первична; кодовый ранг
    # (голоса/позиция) — запасной, и единственный при откате на JSON.
    families.sort(key=lambda f: (model_rep_order.get(f["rep"], 10 ** 6), cand_rank(f["rep"])))

    # ── Мерж-аудит: семьи, различающиеся лишь глаголом-обвязкой, схлопываются ──
    if len(families) >= 2:
        rep_keys = [f["rep"] for f in families]
        listing_r = "\n".join(f"{i+1}. {candidates[k]['variant']}" for i, k in enumerate(rep_keys))
        try:
            res = await call_llm(client, VER_MODEL, VER_THINKING,
                                 MERGE_PROMPT.format(seed=seed, reps=listing_r))
            account(res)
            lean = _parse_lean_cluster(res["text"], len(rep_keys))
        except Exception as e:
            stats["ver"]["merge_error"] = str(e)
            lean = None
        if lean is not None:
            # Кодовый страж склейки: слот из НЕСКОЛЬКИХ семей принимается, только если
            # каждый его представитель содержит все леммы сида (равенство/корень).
            # «Поставить/вставить зубной имплант» — содержат (имплант~имплантация,
            # зубной~зубов) → склейка законна. «Заказать/приобрести айфон 16» — «купить»
            # отсутствует, слова сида ЗАМЕНЕНЫ разными словами → это разные семьи,
            # модельная склейка отклоняется. Правило закона семей — в код.
            seed_lm2 = lemma_tokens(seed)

            def contains_seed_by_root(k):
                v = lemma_tokens(candidates[k]["variant"])
                return all(any(l == vl or _same_root(l, vl) for vl in v) for l in seed_lm2)

            slots_arr, chosen = lean
            guard_broken = 0
            seen_slot = {}
            for fi in range(len(slots_arr)):
                sl = slots_arr[fi]
                if sl <= 0:
                    continue
                seen_slot.setdefault(sl, []).append(fi)
            next_free = max(slots_arr, default=0) + 1
            for sl, fis in seen_slot.items():
                if len(fis) < 2:
                    continue
                if not all(contains_seed_by_root(rep_keys[fi]) for fi in fis):
                    guard_broken += 1
                    for fi in fis[1:]:   # склейка отклонена: каждый остаётся своим слотом
                        slots_arr[fi] = next_free
                        next_free += 1
            if guard_broken:
                stats["ver"]["merge_guard_rejected"] = guard_broken
            slot_map = {}
            for fi, slot in enumerate(slots_arr):
                slot_map.setdefault(slot if slot > 0 else f"solo{fi}", []).append(fi)
            chosen_fi = [c - 1 for c in chosen if 1 <= c <= len(rep_keys)]
            merged, used = [], set()
            def build(slot_members, rep_fi):
                mem = []
                for fi in slot_members:
                    mem.extend(families[fi]["members"])
                base = families[rep_fi]
                merged.append({"scenario": base["scenario"], "members": mem,
                               "rep": base["rep"], "reason": base["reason"]})
            for c_fi in chosen_fi:   # порядок употребимости от модели
                slot = slots_arr[c_fi] if slots_arr[c_fi] > 0 else f"solo{c_fi}"
                if slot in used:
                    continue
                used.add(slot)
                build(slot_map[slot], c_fi)
            for slot, mems in slot_map.items():   # хвост, не названный представителями
                if slot not in used:
                    build(mems, mems[0])
            stats["ver"]["merge"] = {"in": len(families), "out": len(merged)}
            families = merged
    return rejected, families


# ---------------- публичный вход ----------------
async def relevant_variants(seed, client=None):
    """Сид -> до TOP_N переформулировок (представители семей). Добор запрещён:
    сколько семей выжило, столько и вариантов."""
    seed = norm(sanitize_text(seed))
    stats = {"build": BUILD,
             "gen": {"tin": 0, "tout": 0, "cost": 0.0, "wall": 0.0},
             "ver": {"tin": 0, "tout": 0, "cost": 0.0, "wall": 0.0}}
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient()
    t_total = time.time()
    try:
        # Рецикл (Andrew): недетерминированность генерации изредка даёт один вариант
        # («доставка цветов» → только «доставка букетов»; повтор дал три). Если семей
        # меньше RECYCLE_MIN_VARIANTS — одна свежая попытка генерация+кластер с нуля;
        # из двух попыток берётся та, где семей больше. Стоимость обеих — в stats.
        best = None
        for attempt in range(2):
            candidates, errors = await _generate_candidates(client, seed, stats)
            t0 = time.time()
            rejected, families = await _cluster_families(client, seed, candidates, stats)
            stats["ver"]["wall"] = time.time() - t0
            if best is None or len(families) > len(best[2]):
                best = (candidates, errors, families, rejected)
            if len(best[2]) >= RECYCLE_MIN_VARIANTS:
                break
            stats["recycle_attempts"] = attempt + 2
        candidates, errors, families, rejected = best
    finally:
        if own_client:
            await client.aclose()

    # ── Осевой отбор тройки: покрыть РАЗНЫЕ заменённые слова сида ──────────
    # Ось семьи = леммы сида, замещённые в представителе (нет равной/однокоренной).
    # У многословного сида замены самого частотного слова забивали всю тройку
    # («прокат/автопрокат/напрокат» при живой оси «залог→депозит»). Жадный отбор:
    # по употребимости, но непокрытая ось имеет приоритет; добор — по употребимости.
    seed_lm_axis = lemma_tokens(seed)

    def family_axis(f):
        """Ось = замещённые леммы сида; у добавок без замены — сами добавленные
        слова (клиника/стоматология/установка — разные оси, а не одна «добавка»)."""
        v = lemma_tokens(candidates[f["rep"]]["variant"])
        replaced = frozenset(l for l in seed_lm_axis
                             if not any(l == vl or _same_root(l, vl) for vl in v))
        if replaced:
            return replaced
        added = frozenset(vl for vl in v
                          if not any(l == vl or _same_root(l, vl) for l in seed_lm_axis))
        return frozenset("+" + a for a in added)

    # Новизна оси = хотя бы одна ещё НЕ покрытая лемма сида. Комбо-ось
    # ({аренда,авто} после {авто} и {аренда}) нового не покрывает — пропуск,
    # чтобы дошла очередь до непокрытой оси («залог→депозит»).
    final_keys, covered = [], set()
    for f in families:                      # проход 1: только оси с новой леммой
        ax = family_axis(f)
        if ax and not (ax - covered):
            continue
        covered |= ax
        final_keys.append(f["rep"])
        if len(final_keys) == TOP_N:
            break
    if len(final_keys) < TOP_N:            # проход 2: добор по употребимости
        for f in families:
            if f["rep"] not in final_keys:
                final_keys.append(f["rep"])
                if len(final_keys) == TOP_N:
                    break
    fam_of = {}
    for i, f in enumerate(families):
        for k in f["members"]:
            fam_of[k] = (i, f["rep"] == k)

    ranked = []
    for k, c in candidates.items():
        avg_pos = sum(c["positions"]) / len(c["positions"])
        if k in rejected:
            score = "цель✗"
        elif k in fam_of:
            i, is_rep = fam_of[k]
            score = f"F{i+1}" + ("★" if is_rep else "")
        else:
            score = "—"
        ranked.append({"variant": c["variant"], "sub": c["sub"], "orig": c["orig"],
                       "cls": "".join(sorted(c["cls"])),
                       "votes": c["votes"], "avg_pos": round(avg_pos, 2),
                       "verdict": 1 if k in final_keys else 0,
                       "score": score})
    ranked.sort(key=lambda x: (-x["verdict"], -x["votes"], x["avg_pos"]))

    stats["total_cost"] = stats["gen"]["cost"] + stats["ver"]["cost"]
    stats["total_wall"] = time.time() - t_total
    if errors:
        stats["errors"] = errors

    return {"seed": seed,
            "final": [candidates[k]["variant"] for k in final_keys],
            "families": [{"scenario": f["scenario"],
                          "members": [candidates[k]["variant"] for k in f["members"]],
                          "rep": candidates[f["rep"]]["variant"],
                          "reason": f["reason"]} for f in families],
            "candidates": ranked,
            "stats": stats}
