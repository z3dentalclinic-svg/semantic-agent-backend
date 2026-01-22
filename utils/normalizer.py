import re
import pymorphy3
import logging
from typing import List

logger = logging.getLogger("GoldenNormalizer")

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()
    
    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword

        logger.debug(f"[NORMALIZER] START: seed='{golden_seed}' | keyword='{keyword}'")

        # 1. ВСЕ ФОРМЫ слов сида → эталон сида
        seed_forms = {}
        seed_lemmas = set()

        for w in re.findall(r'\w+', golden_seed.lower()):
            try:
                parsed = self.morph.parse(w)
                if not parsed:
                    continue
                seed_form = w  # форма из сида (эталон)
                base_lemma = parsed[0].normal_form
                seed_lemmas.add(base_lemma)

                for form_obj in parsed[0].lexeme:
                    seed_forms[form_obj.word.lower()] = seed_form
            except Exception as e:
                logger.debug(f"[NORMALIZER] seed word parse fail: '{w}' → {e}")
                continue

        tokens = keyword.split()
        result_tokens = []

        # Для детального лога по одному ключу
        changes = []
        unmapped_seed_like = []

        for token in tokens:
            if not token:
                continue

            original = token
            clean_token = token.lower().strip('.,!?() ')
            if not clean_token:
                result_tokens.append(token)
                continue

            replaced = False

            try:
                # 1) точная форма среди seed_forms
                if clean_token in seed_forms:
                    new = seed_forms[clean_token]
                    result_tokens.append(new)
                    changes.append((original, new, "direct_form"))
                    replaced = True
                else:
                    # 2) пробуем морфологию
                    parsed = self.morph.parse(clean_token)
                    if parsed:
                        lemma = parsed[0].normal_form
                        form_word = parsed[0].word.lower()

                        if form_word in seed_forms:
                            new = seed_forms[form_word]
                            result_tokens.append(new)
                            changes.append((original, new, f"via_form:{form_word}"))
                            replaced = True
                        elif lemma in seed_lemmas:
                            # Лемма совпадает с сидом, но форма не в seed_forms → кандидат на проблему
                            result_tokens.append(token)
                            unmapped_seed_like.append((original, lemma))
                        else:
                            result_tokens.append(token)
                    else:
                        result_tokens.append(token)
            except Exception as e:
                logger.debug(f"[NORMALIZER] token parse fail: '{token}' → {e}")
                result_tokens.append(token)

        normalized = " ".join(result_tokens)

        # DEBUG: всегда логируем unmapped (даже если пусто)
        logger.debug(f"[NORMALIZER] END: unmapped_count={len(unmapped_seed_like)}, changes_count={len(changes)}")

        # ЛОГИРУЕМ только проблемные случаи:
        if unmapped_seed_like:
            logger.warning(
                f"[NORMALIZER] SEED-LEMMA UNMAPPED: seed='{golden_seed}' | "
                f"keyword='{keyword}' → '{normalized}' | "
                f"unmapped={unmapped_seed_like} | changes={changes}"
            )

        return normalized
    
    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        if not keywords or not golden_seed:
            return keywords
        return [self.normalize_by_golden_seed(kw, golden_seed) for kw in keywords]

_normalizer = None

def get_normalizer():
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer

def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    n = get_normalizer()
    return n.process_batch(keywords, seed)
