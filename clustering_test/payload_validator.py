"""
Валидация payload-ов перед отправкой в LLM.
Возвращает warnings и stats для дебаг-эндпоинта.
"""
from collections import Counter

from .payload_extractor import get_seed_lemmas, _lemma, _TOKEN_RE, estimate_tokens


def validate_payloads(
    seed: str,
    payload_to_keywords: dict[str, list[str]],
) -> dict:
    seed_lemmas = get_seed_lemmas(seed)
    payloads = list(payload_to_keywords.keys())
    
    warnings = {
        'empty_payload': [],
        'still_contains_seed_token': [],
        'single_char_payload': [],
        'very_short_payload': [],
    }
    
    for p in payloads:
        kws = payload_to_keywords[p]
        
        if p == '':
            warnings['empty_payload'].append({
                'payload': p,
                'keywords': kws,
            })
            continue
        
        # Остался ли в хвосте токен с леммой сида
        leaked = []
        for t in _TOKEN_RE.findall(p.lower()):
            if _lemma(t) in seed_lemmas:
                leaked.append(t)
        if leaked:
            warnings['still_contains_seed_token'].append({
                'payload': p,
                'leaked_tokens': leaked,
                'keywords_sample': kws[:3],
            })
        
        if len(p) == 1:
            warnings['single_char_payload'].append({
                'payload': p,
                'keywords': kws,
            })
        elif len(p) <= 2 and ' ' not in p:
            warnings['very_short_payload'].append({
                'payload': p,
                'keywords': kws,
            })
    
    # Статы
    lengths = [len(p.split()) if p else 0 for p in payloads]
    length_dist = dict(Counter(lengths))
    
    payloads_text = '\n'.join(f'{i+1}. {p}' for i, p in enumerate(payloads))
    
    return {
        'warnings': {
            k: {'count': len(v), 'items': v[:30]}
            for k, v in warnings.items()
        },
        'stats': {
            'length_distribution': dict(sorted(length_dist.items())),
            'estimated_tokens_payloads': estimate_tokens(payloads_text),
        },
    }
