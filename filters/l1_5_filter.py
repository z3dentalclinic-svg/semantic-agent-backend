"""
L1.5 Domain Anchor Filter

Размещается между L0 и L2 в пайплайне:
    L0 → [L1.5] → L2 → L3

Назначение: отсечь явный off-topic мусор из GREY-зоны L0 ДО передачи в L2/L3.

Логика: для каждого ключа в keywords_grey проверяет наличие domain anchor.
Anchor = главный объект seed (object_anchor) ± числовой qualifier (для tech-сидов).
Если anchor отсутствует → перемещение в anchors (TRASH).

Принципы:
- Алгоритмически, без хардкодов и whitelists
- Cross-niche (любой seed)
- БЕЗ LLM
- Минимизация FP важнее максимизации TRASH recall

Уровни сигналов (от мягких к жёстким, можно расширять):
- L1: substring match (object_anchor in kw)
- L2: + qualifier check (число + текстовая форма)
- L3: + лемматизация (pymorphy3)
- L4+: cross-script, L0_VALID expansion, RuWordNet (опционально, см. handoff)
"""

from __future__ import annotations

import logging
import os
import re
import time
import urllib.request
from typing import Optional

import numpy as np

from .shared_morph import morph
from .shared_model import get_embedding_model

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Уровень 5: MiniLM word-level cosine с adaptive threshold
# ──────────────────────────────────────────────────────────────────────────────

# Stopwords (для фильтра nouns в kw)
_STOPWORDS = {
    'купить', 'заказать', 'цена', 'стоимость', 'отзывы', 'продажа', 'доставка',
    'в', 'на', 'с', 'и', 'или', 'без', 'для', 'под', 'через', 'у', 'к', 'от',
    'из', 'о', 'об', 'как', 'где', 'почему', 'что', 'это', 'до', 'по', 'за',
    'про', 'со', 'но', 'же', 'ли', 'бы', 'не', 'ни', 'весь', 'все', 'всё',
}

# Кеш эмбеддингов слов (per-process)
_embedding_cache: dict[str, np.ndarray] = {}


def get_word_embedding(word: str) -> Optional[np.ndarray]:
    """
    Возвращает word-level эмбеддинг через MiniLM (с кешем).
    Возвращает None если модель не загружена.
    """
    if not word:
        return None
    if word in _embedding_cache:
        return _embedding_cache[word]
    
    model = get_embedding_model()
    if model is None:
        return None
    
    try:
        # fastembed.embed() — генератор, передаём список из одной строки
        embeddings = list(model.embed([word]))
        if not embeddings:
            return None
        emb = np.asarray(embeddings[0], dtype=np.float32)
        # Нормализация для cosine
        norm = np.linalg.norm(emb)
        if norm > 0:
            emb = emb / norm
        _embedding_cache[word] = emb
        return emb
    except Exception as e:
        logger.debug(f"[L1.5] Embedding failed for '{word}': {e}")
        return None


def cosine_sim(v1: Optional[np.ndarray], v2: Optional[np.ndarray]) -> float:
    """Cosine similarity (для нормализованных векторов = dot product)."""
    if v1 is None or v2 is None:
        return 0.0
    return float(np.dot(v1, v2))


# ──────────────────────────────────────────────────────────────────────────────
# Domain profile из L0_VALID (per-seed, кешируется)
# ──────────────────────────────────────────────────────────────────────────────

# Кеш профиля по object_anchor
_domain_profile_cache: dict[str, dict] = {}


def build_domain_profile(object_anchor: str, l0_valid_kws: list[str], seed: str,
                          l0_trash_kws: Optional[list[str]] = None) -> dict:
    """
    Строит профиль домена для object_anchor.
    
    Возвращает:
        {
            'anchor_emb': эмбеддинг object_anchor,
            'valid_lemmas': set лемм из L0_VALID хвостов,
            'threshold': адаптивный порог cosine (>= 0.50),
            'centroid_valid': средний эмбеддинг top-релевантных слов из L0_VALID,
            'centroid_trash': средний эмбеддинг NOUN-лемм из L0_TRASH (для contrastive),
            'enabled': bool — работает ли MiniLM
        }
    """
    cache_key = f"{seed}::{object_anchor}"
    if cache_key in _domain_profile_cache:
        return _domain_profile_cache[cache_key]
    
    anchor_emb = get_word_embedding(object_anchor)
    if anchor_emb is None:
        profile = {
            'anchor_emb': None,
            'valid_lemmas': set(),
            'threshold': 0.55,
            'centroid_valid': None,
            'centroid_trash': None,
            'enabled': False,
        }
        _domain_profile_cache[cache_key] = profile
        return profile
    
    seed_words = set(seed.lower().split())
    seed_words.update(_STOPWORDS)
    
    # === VALID side ===
    valid_lemmas: set = set()
    
    # v4.1: object_neighbors — леммы которые встречаются в окне ±2 от
    # object_anchor в L0_VALID ХОТЯ БЫ В 2 РАЗНЫХ ключах. Это даёт чистый 
    # доменный словарь.
    # 
    # Улучшения относительно v4.0:
    # - min_freq=2 (раньше: 1) — отрезает single-shot шум типа "4room", "флаур"
    # - только NOUN (раньше: NOUN+ADJF+PRTF) — прилагательные/причастия добавляли шум
    object_neighbors: set = set()
    WINDOW = 2
    MIN_FREQ_NEIGHBORS = 2
    
    from collections import Counter as _Counter
    _neighbor_counter: _Counter = _Counter()
    
    for kw in l0_valid_kws:
        kw_low = kw.lower() if isinstance(kw, str) else ''
        words_in_kw = re.findall(r'[а-яёa-z]+', kw_low)
        
        # Лемматизируем всё слово для скана
        kw_lemmas_pos: list = []  # [(lemma, pos, idx, w)] 
        for idx, w in enumerate(words_in_kw):
            if len(w) <= 2:
                continue
            p = morph.parse(w)[0]
            lemma = p.normal_form
            pos = p.tag.POS
            
            # Базовый valid_lemmas — все NOUN-леммы (как раньше) — пока сохраним
            # на случай если где-то ещё используется
            if pos == 'NOUN' and w not in seed_words:
                valid_lemmas.add(lemma)
            
            kw_lemmas_pos.append((lemma, pos, idx, w))
        
        # Найти позиции anchor-слов (object_anchor или начинается с него)
        anchor_positions = []
        for lemma, pos, idx, w in kw_lemmas_pos:
            if object_anchor and (lemma == object_anchor or lemma.startswith(object_anchor)):
                anchor_positions.append(idx)
        
        if not anchor_positions:
            continue
        
        # Собираем уникальные leмы окна для ЭТОГО kw (per-kw set),
        # чтобы повторы внутри одного kw не накачивали счётчик
        kw_window_lemmas: set = set()
        for anchor_idx in anchor_positions:
            for lemma, pos, idx, w in kw_lemmas_pos:
                if abs(idx - anchor_idx) > WINDOW or idx == anchor_idx:
                    continue
                if w in seed_words or len(w) <= 2:
                    continue
                # ТОЛЬКО NOUN (без ADJF/PRTF — они добавляют шум)
                if pos == 'NOUN':
                    kw_window_lemmas.add(lemma)
        
        for n in kw_window_lemmas:
            _neighbor_counter[n] += 1
    
    # Финальный set — леммы с MIN_FREQ_NEIGHBORS+ встреч
    object_neighbors = {n for n, c in _neighbor_counter.items() if c >= MIN_FREQ_NEIGHBORS}
    
    # Cosine valid lemmas vs anchor
    lemma_sims: list[tuple[str, float]] = []
    for lemma in list(valid_lemmas)[:100]:
        emb = get_word_embedding(lemma)
        if emb is not None:
            sim = cosine_sim(emb, anchor_emb)
            lemma_sims.append((lemma, sim))
    
    # Адаптивный порог
    # v4.1: снижен до 0.45 (было 0.50) — для спасения гипонимов где
    # cos(гипоним, anchor) на грани (роза=0.44, тюльпан=0.42).
    # Контраст с trash-кластером отключён ниже через guard если он шумный.
    if len(lemma_sims) >= 5:
        sims_sorted = sorted([s for _, s in lemma_sims])
        p10_idx = max(0, int(len(sims_sorted) * 0.10))
        threshold = max(0.45, sims_sorted[p10_idx])
    else:
        threshold = 0.50
    
    # Centroid VALID — top-10 близких к anchor
    top10 = sorted(lemma_sims, key=lambda x: -x[1])[:10]
    centroid_valid = None
    if top10:
        embs = [get_word_embedding(l) for l, _ in top10]
        embs = [e for e in embs if e is not None]
        if embs:
            centroid_valid = np.mean(embs, axis=0)
            n = np.linalg.norm(centroid_valid)
            if n > 0:
                centroid_valid = centroid_valid / n
    
    # === TRASH side (новое — dual-cluster) ===
    centroid_trash = None
    trash_lemmas_for_log: list = []
    if l0_trash_kws:
        trash_lemmas: set = set()
        for kw in l0_trash_kws:
            kw_low = kw.lower() if isinstance(kw, str) else ''
            for w in re.findall(r'[а-яёa-z]+', kw_low):
                if w in seed_words or len(w) <= 2:
                    continue
                p = morph.parse(w)[0]
                if p.tag.POS == 'NOUN':
                    trash_lemmas.add(p.normal_form)
        
        # Убираем леммы которые также в VALID (они нейтральные, не помогают разделять)
        trash_only = trash_lemmas - valid_lemmas
        
        if trash_only:
            trash_embs = []
            for lemma in list(trash_only)[:100]:
                emb = get_word_embedding(lemma)
                if emb is not None:
                    trash_embs.append(emb)
                    trash_lemmas_for_log.append(lemma)
            
            if len(trash_embs) >= 3:
                centroid_trash = np.mean(trash_embs, axis=0)
                n = np.linalg.norm(centroid_trash)
                if n > 0:
                    centroid_trash = centroid_trash / n
                
                # Safety guard: если centroid_trash семантически близок к anchor
                # (cos > 0.55) — это значит L0_TRASH содержит много слов из того же 
                # тематического кластера что anchor (например для 'скутер' в L0_TRASH 
                # сидят украинские варианты с теми же мото-словами). В таком случае
                # contrastive проверка будет работать ПРОТИВ нас: гипонимы 'мопед' 
                # окажутся ближе к этому шумному trash-кластеру чем к anchor.
                # Отключаем centroid_trash.
                trash_anchor_sim = cosine_sim(centroid_trash, anchor_emb)
                if trash_anchor_sim > 0.55:
                    logger.warning(
                        f"[L1.5/L5] centroid_trash disabled — too close to anchor "
                        f"(cos={trash_anchor_sim:.2f}). Trash sample looks like "
                        f"noisy variants of the seed itself."
                    )
                    centroid_trash = None
    
    profile = {
        'anchor_emb': anchor_emb,
        'valid_lemmas': valid_lemmas,
        'object_neighbors': object_neighbors,  # NEW: leхемы в окне ±2 от anchor в L0_VALID
        'threshold': threshold,
        'centroid_valid': centroid_valid,
        'centroid_trash': centroid_trash,
        'enabled': True,
        'top_sims': top10[:5],
        'trash_lemmas_sample': trash_lemmas_for_log[:5],
    }
    
    _domain_profile_cache[cache_key] = profile
    logger.info(
        f"[L1.5/L5] domain_profile for '{object_anchor}': "
        f"valid_lemmas={len(valid_lemmas)}, "
        f"object_neighbors={len(object_neighbors)} (window±{WINDOW}, min_freq={MIN_FREQ_NEIGHBORS}, NOUN only): "
        f"{sorted(object_neighbors)[:20]}, "
        f"threshold={threshold:.2f}, "
        f"top_sims={[(l, round(s, 2)) for l, s in top10[:5]]}, "
        f"centroid_trash={'YES' if centroid_trash is not None else 'NO'} "
    )
    return profile


def check_semantic_anchor(
    kw: str,
    seed_words: set,
    object_anchor: str,
    profile: dict,
    l0_pos_signals: Optional[list] = None,
) -> tuple[bool, str]:
    """
    Уровень 5 (v4.3 — scoring-based):
    Объединяет несколько слабых сигналов в один score:
    
      score(cand) = cos(cand, anchor)
                  + L0_BONUS если у kw есть positive signal от L0
                  + NEIGHBORS_BONUS если cand в object_neighbors
    
    Pass если max(score) >= SCORE_THRESHOLD.
    
    Это позволяет:
    - Спасти гипонимы (роза/тюльпан) у которых низкий cos 0.42-0.44, 
      но есть L0 geo signal — bonus вытягивает score >= 0.50
    - Зарезать параметры (вольт/ампер) — cos 0.43, нет L0 signal — 
      score 0.43-0.48 < 0.50 → TRASH
    
    Returns: (matched, signal_name)
    """
    if not profile.get('enabled') or profile.get('anchor_emb') is None:
        return False, ''
    
    object_neighbors = profile.get('object_neighbors', set())
    anchor_emb = profile['anchor_emb']
    
    # Параметры scoring
    SCORE_THRESHOLD = 0.50
    L0_BONUS = 0.10
    NEIGHBORS_BONUS = 0.05
    
    has_l0_signal = bool(l0_pos_signals)
    
    kw_low = kw.lower()
    
    # Извлекаем кандидатов
    candidates: list[str] = []
    for w in re.findall(r'[а-яёa-z]+', kw_low):
        if w in seed_words or w in _STOPWORDS or len(w) <= 2:
            continue
        p = morph.parse(w)[0]
        if p.tag.POS == 'NOUN':
            candidates.append(p.normal_form)
    
    if not candidates:
        return False, ''
    
    # Для каждого считаем score
    best_score = 0.0
    best_word = None
    best_breakdown = ''
    
    for cand in candidates[:5]:
        emb = get_word_embedding(cand)
        if emb is None:
            continue
        sim_anchor = cosine_sim(emb, anchor_emb)
        
        bonus = 0.0
        sources = []
        if has_l0_signal:
            bonus += L0_BONUS
            sources.append(f'L0={"+".join(l0_pos_signals)}')
        if cand in object_neighbors:
            bonus += NEIGHBORS_BONUS
            sources.append('neigh')
        
        score = sim_anchor + bonus
        if score > best_score:
            best_score = score
            best_word = cand
            sources_str = '|'.join(sources) if sources else 'none'
            best_breakdown = f'{cand}=cos{sim_anchor:.2f}+{bonus:.2f}({sources_str})={score:.2f}'
    
    if not best_word:
        return False, ''
    
    if best_score >= SCORE_THRESHOLD:
        return True, f'score:{best_breakdown}>={SCORE_THRESHOLD}'
    
    # Diagnostic signal для логов — почему отказали
    return False, f'score_low:{best_breakdown}<{SCORE_THRESHOLD}'




# ──────────────────────────────────────────────────────────────────────────────
# RuWordNet — taxonomy для synonyms/hyponyms (Уровень 4 расширения anchor)
# ──────────────────────────────────────────────────────────────────────────────

_RUWORDNET_DB_URL = "https://github.com/avidale/python-ruwordnet/releases/download/0.0.4/ruwordnet-2021.db"
_RUWORDNET_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "_data",
    "ruwordnet.db",
)

_wn = None  # singleton, инициализируется при первом обращении


def _ensure_ruwordnet_db() -> Optional[str]:
    """Скачивает БД RuWordNet один раз. Возвращает путь или None при ошибке."""
    if os.path.exists(_RUWORDNET_DB_PATH):
        return _RUWORDNET_DB_PATH
    
    try:
        os.makedirs(os.path.dirname(_RUWORDNET_DB_PATH), exist_ok=True)
        logger.info(f"[L1.5] Downloading RuWordNet DB (~96MB) from {_RUWORDNET_DB_URL}")
        t0 = time.time()
        urllib.request.urlretrieve(_RUWORDNET_DB_URL, _RUWORDNET_DB_PATH)
        size_mb = os.path.getsize(_RUWORDNET_DB_PATH) / 1024 / 1024
        logger.info(f"[L1.5] RuWordNet DB downloaded ({size_mb:.1f}MB) in {time.time()-t0:.1f}s")
        return _RUWORDNET_DB_PATH
    except Exception as e:
        logger.warning(f"[L1.5] RuWordNet DB download failed: {e}")
        return None


def _get_wn():
    """Lazy singleton — RuWordNet object."""
    global _wn
    if _wn is not None:
        return _wn if _wn is not False else None
    
    try:
        from ruwordnet import RuWordNet
        db_path = _ensure_ruwordnet_db()
        if db_path is None:
            _wn = False
            return None
        _wn = RuWordNet(db_path)
        logger.info("[L1.5] RuWordNet initialized")
        return _wn
    except ImportError:
        logger.warning("[L1.5] ruwordnet package not installed — synonyms disabled")
        _wn = False
        return None
    except Exception as e:
        logger.warning(f"[L1.5] RuWordNet init failed: {e} — synonyms disabled")
        _wn = False
        return None


# Кеш synonyms по object_anchor (один раз на сид)
_synonyms_cache: dict[str, set[str]] = {}


def get_synonyms_for(object_anchor: str) -> set[str]:
    """
    Возвращает синонимы / гипонимы / гиперонимы object_anchor через RuWordNet.
    
    Для 'скутер' вернёт {'мопед', 'мотороллер', 'электроскутер', ...}
    Для 'цветок' вернёт {'роза', 'тюльпан', 'эустома', 'букет', ...}
    
    Если RuWordNet недоступен — пустое множество.
    Кешируется per-anchor.
    """
    if not object_anchor:
        return set()
    
    if object_anchor in _synonyms_cache:
        return _synonyms_cache[object_anchor]
    
    wn = _get_wn()
    if not wn:
        _synonyms_cache[object_anchor] = set()
        return set()
    
    related: set[str] = set()
    try:
        senses = wn.get_senses(object_anchor)
        for sense in senses:
            synset = sense.synset
            # Гипонимы (виды) — наследники
            for h in synset.hyponyms:
                for s in h.senses:
                    related.add(s.name.lower())
            # Синонимы (тот же synset)
            for s in synset.senses:
                related.add(s.name.lower())
            # Гиперонимы (родители) — обобщения (для подстраховки)
            for h in synset.hypernyms:
                for s in h.senses:
                    related.add(s.name.lower())
    except Exception as e:
        logger.debug(f"[L1.5] RuWordNet lookup error for '{object_anchor}': {e}")
    
    related.discard(object_anchor.lower())
    _synonyms_cache[object_anchor] = related
    
    if related:
        logger.info(f"[L1.5] RuWordNet synonyms for '{object_anchor}' ({len(related)}): {sorted(related)[:10]}")
    else:
        logger.info(f"[L1.5] RuWordNet: no synonyms for '{object_anchor}'")
    
    return related


# ──────────────────────────────────────────────────────────────────────────────
# Конфигурация
# ──────────────────────────────────────────────────────────────────────────────

# Текстовые формы чисел для qualifier-проверки (например seed "купить айфон 16"
# должен матчиться на "купить шестнадцатый iphone")
_NUM_TO_TEXT: dict[str, list[str]] = {
    '1': ['первый'], '2': ['второй'], '3': ['третий'], '4': ['четвертый'],
    '5': ['пятый'], '6': ['шестой'], '7': ['седьмой'], '8': ['восьмой'],
    '9': ['девятый'], '10': ['десятый'], '11': ['одиннадцатый'],
    '12': ['двенадцатый'], '13': ['тринадцатый'], '14': ['четырнадцатый'],
    '15': ['пятнадцатый'], '16': ['шестнадцатый'], '17': ['семнадцатый'],
    '18': ['восемнадцатый'], '19': ['девятнадцатый'], '20': ['двадцатый'],
}


# ──────────────────────────────────────────────────────────────────────────────
# Извлечение object_anchor из seed
# ──────────────────────────────────────────────────────────────────────────────

def extract_object_anchor(seed: str) -> tuple[Optional[str], Optional[str], list[str]]:
    """
    Извлекает domain anchor из seed.
    
    Returns:
        (object_anchor, qualifier, qualifier_text)
        
        object_anchor: лемма главного существительного. Приоритет:
            1. NOUN в gent (родительный падеж) — объект действия
               "доставка цветов" → "цветов" (gent) → лемма "цвет"
            2. NOUN после предлога 'на'/'для'/'в'
               "аккумулятор на скутер" → "скутер"
            3. NOUN в accs (винительный)
            4. Последний NOUN в seed
        
        qualifier: число если есть ('16' для 'купить айфон 16')
        qualifier_text: словесные формы числа ['шестнадцатый']
    
    Примеры:
        "доставка цветов" → ('цвет', None, [])
        "купить айфон 16" → ('айфон', '16', ['шестнадцатый'])
        "аккумулятор на скутер" → ('скутер', None, [])
        "имплантация зубов" → ('зуб', None, [])
        "установка кондиционера цена" → ('кондиционер', None, [])
    """
    words = seed.lower().split()
    qualifier: Optional[str] = None
    qualifier_text: list[str] = []
    nouns: list[tuple[str, Optional[str], bool]] = []  # (lemma, case, after_prep)
    after_prep = False
    
    for w in words:
        # Числовой qualifier (16, 8 — отдельный токен)
        if re.match(r'^\d+$', w):
            qualifier = w
            qualifier_text = _NUM_TO_TEXT.get(w, [])
            after_prep = False
            continue
        
        p = morph.parse(w)[0]
        if p.tag.POS == 'NOUN':
            case = str(p.tag.case) if p.tag.case else None
            nouns.append((p.normal_form, case, after_prep))
        after_prep = (p.tag.POS == 'PREP')
    
    if not nouns:
        return None, qualifier, qualifier_text
    
    # Единственный NOUN — он object
    if len(nouns) == 1:
        return nouns[0][0], qualifier, qualifier_text
    
    # Priority 1: NOUN в gent
    for lemma, case, _ap in nouns:
        if case == 'gent':
            return lemma, qualifier, qualifier_text
    
    # Priority 2: NOUN после предлога
    for lemma, _case, ap in nouns:
        if ap:
            return lemma, qualifier, qualifier_text
    
    # Priority 3: NOUN в accs
    for lemma, case, _ap in nouns:
        if case == 'accs':
            return lemma, qualifier, qualifier_text
    
    # Default: последний NOUN
    return nouns[-1][0], qualifier, qualifier_text


# ──────────────────────────────────────────────────────────────────────────────
# Извлечение action_anchor из seed (intent)

# ──────────────────────────────────────────────────────────────────────────────
# Проверка anchor в kw
# ──────────────────────────────────────────────────────────────────────────────

def has_anchor(
    kw: str,
    object_anchor: Optional[str],
    qualifier: Optional[str],
    qualifier_text: list[str],
    synonyms: Optional[set] = None,
    domain_profile: Optional[dict] = None,
    seed_words: Optional[set] = None,
    l0_pos_signals: Optional[list] = None,
    seed_content_lemmas: Optional[set] = None,
) -> tuple[bool, str]:
    """
    Проверяет наличие domain anchor в kw.
    
    Уровни проверки (от жёсткого к мягкому):
    L0_HARD: kw содержит хотя бы одну лемму из seed-фразы (object/action леммы).
             Если нет НИ ОДНОГО seed-слова — мгновенный TRASH. Это режет ключи 
             типа 'букет до 300 грн' для seed 'доставка цветов' — там нет ни 
             'доставка', ни 'цветов'/'цвет', значит ключ не про этот домен.
    L1: substring object_anchor в kw_low
    L2: qualifier (число)
    L3: лемма == object_anchor
    L4: RuWordNet synonyms
    L5: MiniLM scoring (cosine + bonuses)
    """
    if not object_anchor:
        return True, 'no_object_extracted'
    
    kw_low = kw.lower()
    
    # Леммы kw (сразу собираем для всех проверок ниже)
    kw_words = re.findall(r'[а-яёa-z]+', kw_low)
    kw_lemmas: set = set()
    for w in kw_words:
        p = morph.parse(w)[0]
        kw_lemmas.add(p.normal_form)
    
    # === QUALIFIER_HARD guard ===
    # Если в seed есть число-qualifier ('16' для 'айфон 16', '10' для 
    # 'грузоперевозки 10 тонн') — это число ОБЯЗАТЕЛЬНО должно быть в kw, 
    # иначе ключ про другую модификацию/спецификацию продукта.
    # 
    # 'грузоперевозки 5т' для seed 'грузоперевозки 10 тонн' → нет '10' → TRASH
    # 'купить айфон 14' для seed 'купить айфон 16' → нет '16' → TRASH
    # 'купить айфон 16 pro' → есть '16' → продолжаем
    if qualifier:
        has_qualifier = bool(re.search(rf'(?<!\d){qualifier}(?!\d)', kw_low))
        # Также проверяем словесные формы (если есть)
        if not has_qualifier and qualifier_text:
            for txt in qualifier_text:
                if txt in kw_low:
                    has_qualifier = True
                    break
        if not has_qualifier:
            return False, f'no_qualifier:{qualifier}'
    
    # === L0_HARD: содержит ли kw хотя бы одну лемму из seed-фразы ===
    # Это сильный guard для случаев когда в kw НЕТ слов из домена.
    # Например для seed='доставка цветов' (seed_content_lemmas={'доставка','цвет'}):
    #   'букет до 300 грн' (леммы={букет,до,300,грн}) → нет пересечения → TRASH
    #   'розы доставка киев' (леммы={роза,доставка,киев}) → есть 'доставка' → продолжаем
    # 
    # Дополнительно проверяем substring object_anchor (для морфо-вариантов 
    # которые лемматизатор может не свернуть к object_anchor).
    if seed_content_lemmas:
        has_seed_lemma = bool(kw_lemmas & seed_content_lemmas)
        # Substring fallback на object_anchor (на случай если лемматизатор не справился)
        has_object_substring = object_anchor in kw_low
        if not has_seed_lemma and not has_object_substring:
            return False, 'no_seed_lemma'
    
    # L1: Substring object_anchor
    if object_anchor in kw_low:
        return True, 'substring'
    
    # L2: Qualifier (число)
    if qualifier:
        if re.search(rf'(?<!\d){qualifier}(?!\d)', kw_low):
            return True, 'qualifier_digit'
        for txt in qualifier_text:
            if txt in kw_low:
                return True, 'qualifier_text'
    
    # L3: лемма == object_anchor
    if object_anchor in kw_lemmas:
        return True, 'lemma'
    
    # L4: RuWordNet synonyms (substring + лемма)
    if synonyms:
        for syn in synonyms:
            if syn in kw_low:
                return True, f'synonym_substring:{syn}'
        for lemma in kw_lemmas:
            if lemma in synonyms:
                return True, f'synonym_lemma:{lemma}'
    
    # L5: MiniLM scoring (cosine + bonuses)
    if domain_profile and seed_words:
        matched, l5_signal = check_semantic_anchor(
            kw, seed_words, object_anchor, domain_profile,
            l0_pos_signals=l0_pos_signals,
        )
        if matched:
            return True, l5_signal
        if l5_signal:
            return False, l5_signal
    
    return False, 'no_anchor'


# ──────────────────────────────────────────────────────────────────────────────
# Проход 2: Action anchor (intent-фильтр)
# ──────────────────────────────────────────────────────────────────────────────

def extract_action_anchor(seed: str) -> tuple[Optional[str], Optional[str]]:
    """
    Извлекает action_anchor (действие/intent) из seed.
    
    action_anchor — это лемма первого NOUN или INFN/VERB в seed.
    Это слово определяет intent (что пользователь хочет сделать).
    
    Также возвращается action_root — общая часть для substring match
    однокоренных слов (берётся первые 5 символов леммы).
    
    Примеры:
        "доставка цветов"             → ('доставка', 'достав')
        "купить айфон 16"             → ('купить', 'купит') 
        "имплантация зубов"           → ('имплантация', 'имплантац')  
        "установка кондиционера цена" → ('установка', 'устано')
        "аккумулятор на скутер"       → ('аккумулятор', 'аккумулят')
    
    Если первое слово — препозиция или союз, переходим ко следующему.
    """
    words = seed.lower().split()
    
    for w in words:
        if re.match(r'^\d+$', w):
            continue
        p = morph.parse(w)[0]
        pos = p.tag.POS
        if pos in ('NOUN', 'INFN', 'VERB'):
            lemma = p.normal_form
            # root: первые 5 символов леммы, чтобы ловить морфо-вариации
            # но не слишком короткие случайные совпадения
            root = lemma[:min(5, max(4, len(lemma) - 2))]
            return lemma, root
    
    return None, None


def build_action_set(
    action_anchor: str,
    action_root: str,
    l0_valid_kws: list[str],
    object_anchor: Optional[str],
    l0_grey_kws: Optional[list[str]] = None,
) -> set[str]:
    """
    Action_set = action_anchor + синонимы.
    
    Источники синонимов:
    
    1. VERB/INFN из L0_VALID хвостов с частотой >=3 (бесспорно actions —
       'купить', 'заказать', 'привезти' если они есть в VALID).
    
    2. NOUN-леммы из L0_VALID+L0_GREY которые семантически близки к 
       action_anchor через MiniLM (cos >= 0.50). 
       
       Это спасает NOUN-синонимы action: для 'аккумулятор' это 'акб', 
       'батарея' (cos ~0.55-0.65 — прямые синонимы).
       Не пускает 'букет/днепр' (cos < 0.30 — далеко от action).
       
       Использует L0_GREY как источник потому что некоторые синонимы 
       (акб/батарея) есть только в GREY, не в VALID (L0 их зарезал
       за отсутствие seed-слова).
    
    Объект и однокоренные action — исключаем.
    """
    from collections import Counter
    
    if not action_anchor:
        return set()
    
    action_set = {action_anchor}
    
    # === Источник 1: VERB/INFN из L0_VALID ===
    counter: Counter = Counter()
    for kw in l0_valid_kws:
        kw_low = kw.lower() if isinstance(kw, str) else ''
        words = re.findall(r'[а-яёa-z]+', kw_low)
        for w in words:
            if w in _STOPWORDS or len(w) <= 3:
                continue
            p = morph.parse(w)[0]
            pos = p.tag.POS
            lemma = p.normal_form
            
            if object_anchor and (lemma == object_anchor or object_anchor in lemma):
                continue
            if action_root and lemma.startswith(action_root):
                continue
            
            if pos in ('INFN', 'VERB'):
                counter[lemma] += 1
    
    for lemma, count in counter.most_common(30):
        if count >= 3:
            action_set.add(lemma)
    
    # === Источник 2: NOUN-синонимы action через MiniLM cosine ===
    # Берём NOUN из L0_VALID + L0_GREY, оставляем только семантически 
    # близкие к action_anchor (cos >= 0.50).
    # 
    # КРИТИЧНО: исключаем леммы которые БОЛЕЕ близки к object_anchor чем 
    # к action_anchor — это object-стороны (букет/роза для цветов).
    ACTION_SYN_THRESHOLD = 0.50
    anchor_emb = get_word_embedding(action_anchor)
    object_emb = get_word_embedding(object_anchor) if object_anchor else None
    
    if anchor_emb is not None:
        # Собираем кандидатов: NOUN-леммы из VALID + GREY
        all_kws = (l0_valid_kws or []) + (l0_grey_kws or [])
        candidate_lemmas: set = set()
        for kw in all_kws:
            kw_low = kw.lower() if isinstance(kw, str) else ''
            for w in re.findall(r'[а-яёa-z]+', kw_low):
                if w in _STOPWORDS or len(w) <= 2:
                    continue
                p = morph.parse(w)[0]
                pos = p.tag.POS
                lemma = p.normal_form
                
                if pos != 'NOUN':
                    continue
                if object_anchor and (lemma == object_anchor or object_anchor in lemma):
                    continue
                if action_root and lemma.startswith(action_root):
                    continue
                if lemma in action_set:
                    continue
                candidate_lemmas.add(lemma)
        
        # Фильтруем через MiniLM cos
        synonyms_added = []
        for lemma in candidate_lemmas:
            emb = get_word_embedding(lemma)
            if emb is None:
                continue
            sim_action = cosine_sim(emb, anchor_emb)
            if sim_action < ACTION_SYN_THRESHOLD:
                continue
            
            # Проверка стороны: если object_anchor существует и кандидат 
            # ближе к нему чем к action_anchor — это object-side, не action.
            # Это блокирует попадание гипонимов (букет/роза) в action_set.
            if object_emb is not None:
                sim_object = cosine_sim(emb, object_emb)
                if sim_object > sim_action:
                    continue  # это object-сторона
            
            action_set.add(lemma)
            synonyms_added.append((lemma, sim_action))
        
        if synonyms_added:
            synonyms_added.sort(key=lambda x: -x[1])
            logger.info(
                f"[L1.5/Pass2] action NOUN-synonyms (cos>={ACTION_SYN_THRESHOLD}): "
                f"{[(l, round(s, 2)) for l, s in synonyms_added[:15]]}"
            )
    
    return action_set


def has_action(
    kw: str,
    action_anchor: Optional[str],
    action_root: Optional[str],
    action_set: set[str],
    object_anchor: Optional[str] = None,
) -> tuple[bool, str]:
    """
    Проверка action-anchor в kw.
    
    Три сигнала:
    A. Лемма начинается с action_root — однокоренные формы action_anchor
       (например 'доста' → доставка/доставку/доставленный).
       Через лемму чтобы избежать коллизии 'подставка' ≠ 'доста'.
    B. Лемма в action_set (синонимы через MiniLM из L0_VALID/GREY:
       курьер/привезти/акб/батарея).
    C. Positional fallback — первое слово kw это NOUN, не равно object_anchor 
       и не его substring, а в kw присутствует object_anchor (substring).
       Это структурный сигнал: слово стоит в той же позиции что action в seed,
       значит ведёт себя как action. Спасает аббревиатуры/редкие синонимы 
       которые MiniLM не различает (типа 'акб' — cos с 'аккумулятор' < 0.5,
       но позиционно равен action).
    
    Returns: (has_action, signal_name)
    """
    if not action_anchor or not action_root:
        return True, 'no_action_extracted'  # safe-fail
    
    kw_low = kw.lower()
    
    for w in re.findall(r'[а-яёa-z]+', kw_low):
        p = morph.parse(w)[0]
        lemma = p.normal_form
        # A. Лемма начинается с action_root (однокоренные)
        if lemma.startswith(action_root):
            return True, f'action_root:{lemma}'
        # B. Лемма в action_set (синонимы)
        if lemma in action_set:
            return True, f'action_lemma:{lemma}'
    
    # C. Positional fallback — первое слово KOROTKOE NOUN (≤4 символа,
    # как аббревиатура), не равно object_anchor, при наличии object_anchor
    # substring в kw.
    # 
    # Это структурный сигнал: короткое слово на pos 0 в kw с объектом — 
    # вероятно аббревиатура action (типа 'акб' для 'аккумулятор').
    # 
    # Ограничение длиной отрезает FP типа 'подставка для цветов'
    # (подставка — 9 букв, не аббревиатура, не action).
    MAX_ABBREV_LEN = 4
    if object_anchor and object_anchor in kw_low:
        words = re.findall(r'[а-яёa-z]+', kw_low)
        if words:
            first = words[0]
            if 2 <= len(first) <= MAX_ABBREV_LEN:
                p_first = morph.parse(first)[0]
                first_lemma = p_first.normal_form
                if (p_first.tag.POS == 'NOUN'
                    and first_lemma != object_anchor
                    and not first_lemma.startswith(object_anchor)
                    and object_anchor not in first_lemma):
                    return True, f'action_abbrev_pos0:{first_lemma}'
    
    return False, 'no_action'


# ──────────────────────────────────────────────────────────────────────────────
# Главная функция фильтра
# ──────────────────────────────────────────────────────────────────────────────

def apply_l1_5_filter(data: dict, seed: str) -> dict:
    """
    Применяет L1.5 Domain Anchor Filter к keywords_grey.
    
    Два прохода:
    1. Object anchor — есть ли в kw объект seed (или его гипоним/синоним)
    2. Action anchor — есть ли в kw действие seed (или его синоним из L0_VALID)
    
    Ключ считается валидным только если ОБА прохода успешны.
    
    Безопасность:
    - Не трогает keywords (VALID от L0) и existing anchors (TRASH от L0)
    - Если object_anchor не извлёкся из seed — фильтр пропускает GREY без изменений
    - Минимум 10 ключей в GREY чтобы запускаться (на маленьких наборах не имеет смысла)
    
    Args:
        data: dict с keywords/keywords_grey/anchors/_l0_trace
        seed: строка seed
    
    Returns:
        Обновлённый data с:
        - keywords_grey: уменьшен
        - anchors: увеличен (бывшие GREY → TRASH)
        - _l1_5_trace: список trace для отладки (опционально)
        - _filter_timings['l1_5']: время работы
    """
    t0 = time.perf_counter()
    
    grey = data.get('keywords_grey', [])
    if len(grey) < 10:
        # Слишком мало — пропускаем
        logger.info(f"[L1.5] skipped (only {len(grey)} grey keywords)")
        data.setdefault('_filter_timings', {})['l1_5'] = round(time.perf_counter() - t0, 4)
        return data
    
    # Извлекаем anchor из seed
    object_anchor, qualifier, qualifier_text = extract_object_anchor(seed)
    
    if not object_anchor:
        logger.info(f"[L1.5] skipped (no object_anchor from seed='{seed}')")
        data.setdefault('_filter_timings', {})['l1_5'] = round(time.perf_counter() - t0, 4)
        return data
    
    # Уровень 4: synonyms/hyponyms через RuWordNet (per-seed cache)
    synonyms = get_synonyms_for(object_anchor)
    
    # Уровень 5: domain_profile (MiniLM word-cosine + dual-cluster) из L0_VALID и L0_TRASH хвостов
    l0_trace = data.get('_l0_trace', [])
    l0_valid_kws = [
        t['keyword'] for t in l0_trace 
        if isinstance(t, dict) and t.get('label') == 'VALID'
    ]
    l0_trash_kws = [
        t['keyword'] for t in l0_trace
        if isinstance(t, dict) and t.get('label') == 'TRASH'
    ]
    
    # Map kw → positive L0 signals для bonus в L5 scoring
    # Positive signals (geo, commerce, info_intent, action, conjunctive, reputation, 
    # product_spec) — это сигналы что L0 уже распознал в kw знакомую коммерческую 
    # структуру. Мы добавляем bonus к score таким ключам.
    l0_signals_map: dict = {}
    for t in l0_trace:
        if not isinstance(t, dict):
            continue
        sigs = t.get('signals', [])
        if not sigs:
            continue
        pos_sigs = [s for s in sigs if not s.startswith('-')]
        if pos_sigs:
            l0_signals_map[t.get('keyword', '')] = pos_sigs
    
    domain_profile = build_domain_profile(object_anchor, l0_valid_kws, seed, l0_trash_kws)
    seed_words = set(seed.lower().split())
    
    # === Seed content lemmas (для L0_HARD guard в has_anchor) ===
    # Леммы всех content-слов из seed-фразы (исключая stopwords/предлоги).
    # Если в kw нет НИ ОДНОЙ из этих лемм И нет substring object_anchor — TRASH.
    # Например для seed='доставка цветов' → {'доставка', 'цвет'}.
    # 'букет до 300 грн' → леммы {букет, грн} — нет пересечения → TRASH.
    seed_content_lemmas: set = set()
    for sw in seed.lower().split():
        if sw in _STOPWORDS or len(sw) <= 2:
            continue
        p = morph.parse(sw)[0]
        if p.tag.POS in ('NOUN', 'INFN', 'VERB', 'ADJF', 'PRTF', 'ADJS'):
            seed_content_lemmas.add(p.normal_form)
    
    logger.info(f"[L1.5] seed_content_lemmas: {sorted(seed_content_lemmas)}")
    
    # === Action anchor (Pass 2) — intent-фильтр ===
    # Включён по умолчанию. Можно отключить через data['_l1_5_disable_action'] = True
    enable_action_check = not bool(data.get('_l1_5_disable_action', False))
    
    action_anchor: Optional[str] = None
    action_root: Optional[str] = None
    action_set: set = set()
    if enable_action_check:
        action_anchor, action_root = extract_action_anchor(seed)
        if action_anchor:
            # Передаём GREY-ключи как источник синонимов action — там могут 
            # быть 'акб', 'батарея' и т.п. синонимы action_anchor 
            # которые L0 не пометил VALID но через MiniLM cos они близкие
            grey_kws_for_action = [
                kw if isinstance(kw, str) else kw.get('keyword', '')
                for kw in grey
            ]
            action_set = build_action_set(
                action_anchor, action_root, l0_valid_kws, object_anchor,
                l0_grey_kws=grey_kws_for_action,
            )
    
    logger.info(
        f"[L1.5] seed='{seed}' → object='{object_anchor}', qualifier='{qualifier}', "
        f"synonyms({len(synonyms)}), "
        f"action='{action_anchor}' root='{action_root}' set_size={len(action_set)} enabled={enable_action_check}, "
        f"l5_enabled={domain_profile.get('enabled', False)}"
    )
    if action_set:
        action_set_sample = sorted(action_set)[:20]
        logger.info(f"[L1.5/Pass2] action_set: {action_set_sample}")
    
    new_grey: list = []
    new_trash: list = []
    trace_records: list = []
    l5_saves: list = []  # ключи которые спас L5 (для логирования)
    l5_misses: list = []  # ключи которые L5 не спас, но провёл анализ
    action_misses: list = []  # ключи отрезанные на Pass 2 (нет action)
    
    for kw_item in grey:
        # kw может быть строкой или dict
        kw = kw_item if isinstance(kw_item, str) else kw_item.get('keyword', '')
        
        # Получаем L0 positive signals для этого kw (если есть)
        kw_l0_signals = l0_signals_map.get(kw, [])
        
        # === Pass 1: object_anchor проверка ===
        ok, signal = has_anchor(
            kw, object_anchor, qualifier, qualifier_text,
            synonyms=synonyms,
            domain_profile=domain_profile,
            seed_words=seed_words,
            l0_pos_signals=kw_l0_signals,
            seed_content_lemmas=seed_content_lemmas,
        )
        
        # === Pass 2: action_anchor проверка (только если Pass 1 прошёл) ===
        if ok and action_anchor:
            action_ok, action_signal = has_action(kw, action_anchor, action_root, action_set, object_anchor=object_anchor)
            if not action_ok:
                # Pass 1 прошёл (есть object), но Pass 2 нет (нет action) → TRASH
                ok = False
                signal = f'{signal}|no_action'
                action_misses.append((kw, signal))
        
        if ok:
            new_grey.append(kw_item)
            if signal.startswith('score:') or signal.startswith('word_cosine:') or signal.startswith('in_valid_vocab:') or signal.startswith('in_object_neighbors:'):
                l5_saves.append((kw, signal))
        else:
            new_trash.append(kw_item)
            if signal and (signal.startswith('score_low') or signal.startswith('word_cosine')):
                l5_misses.append((kw, signal))
            
            # Определяем причину
            if 'no_action' in signal:
                reason = f'no_action (action={action_anchor}, object_passed={object_anchor})'
            else:
                reason = f'no_domain_anchor (object={object_anchor})'
            
            trace_records.append({
                'keyword': kw,
                'label': 'TRASH',
                'decided_by': 'l1_5',
                'reason': reason,
                'signals': [signal] if signal else [],
            })
    
    # Логируем L5-спасения (для диагностики)
    if l5_saves:
        logger.info(f"[L1.5/L5] saved {len(l5_saves)} keywords via semantic word-cosine:")
        for kw, sig in l5_saves[:15]:
            logger.info(f"[L1.5/L5]   ✓ '{kw}' ({sig})")
        if len(l5_saves) > 15:
            logger.info(f"[L1.5/L5]   ... +{len(l5_saves)-15} more")
    
    # Логируем L5-промахи (для диагностики порогов)
    if l5_misses:
        # Особо выделяем кейсы где cosine высокий но не прошёл (порог/contrastive)
        high_cos_misses = [(kw, sig) for kw, sig in l5_misses
                           if 'word_cosine_trash:' in sig or 'word_cosine_low:' in sig]
        # Сортируем по cosine (читаем число из строки)
        def _extract_sim(sig):
            import re as _re
            m = _re.search(r'=(\d+\.\d+)', sig)
            return float(m.group(1)) if m else 0.0
        high_cos_misses.sort(key=lambda x: -_extract_sim(x[1]))
        
        logger.info(f"[L1.5/L5] missed {len(l5_misses)} keywords (top by sim):")
        for kw, sig in high_cos_misses[:20]:
            logger.info(f"[L1.5/L5]   ✗ '{kw}' ({sig})")
    
    # Логируем action-промахи (Pass 2)
    if action_misses:
        logger.info(f"[L1.5/Pass2] cut {len(action_misses)} keywords without action '{action_anchor}':")
        for kw, sig in action_misses[:15]:
            logger.info(f"[L1.5/Pass2]   ✗ '{kw}' ({sig})")
        if len(action_misses) > 15:
            logger.info(f"[L1.5/Pass2]   ... +{len(action_misses)-15} more")
    
    # Обновляем data
    data['keywords_grey'] = new_grey
    data.setdefault('anchors', []).extend(new_trash)
    
    # anchors_count для UI (если поле используется)
    if 'anchors_count' in data:
        data['anchors_count'] = len(data['anchors'])
    
    # Trace для отладки — список всех ключей которые мы затрешили
    # (parser.tracer в main.py сам подхватит этот блок через after_filter с reasons)
    existing_trace = data.get('_l1_5_trace', [])
    data['_l1_5_trace'] = existing_trace + trace_records
    
    elapsed = round(time.perf_counter() - t0, 4)
    data.setdefault('_filter_timings', {})['l1_5'] = elapsed
    
    logger.info(
        f"[L1.5] grey: {len(grey)} → {len(new_grey)}, "
        f"to_trash: {len(new_trash)}, elapsed: {elapsed}s"
    )
    
    return data
