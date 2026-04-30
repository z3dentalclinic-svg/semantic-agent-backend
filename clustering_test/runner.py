"""
Runner: оркестрирует пайплайн extract → validate → LLM → expand.
"""
import json
import time
from collections import Counter

from .payload_extractor import build_payload_mapping, get_seed_lemmas, estimate_tokens
from .payload_validator import validate_payloads
from .prompts import build_prompts
from .llm_client import call_llm, calc_cost


def parse_llm_json(raw: str) -> tuple[dict, str | None]:
    """
    Поддерживает два формата ответа модели:

    НОВЫЙ (компактный, ~3x меньше output):
        {
          "clusters": {"1": "имя_кластера", "2": "имя_другого", ...},
          "assignments": {"1": 1, "2": 5, "3": 2, ...}
        }

    СТАРЫЙ (fallback, для обратной совместимости):
        {"1": "имя_кластера", "2": "имя_кластера", ...}

    Возвращает (assignments_dict, error_msg) где assignments_dict
    имеет вид {payload_id_str: cluster_name_str} — это формат который
    ожидает expand_clusters() ниже.
    """
    s = raw.strip()
    # На всякий случай удаляем markdown-обёртку если модель её добавила
    if s.startswith('```'):
        s = s.strip('`')
        if s.startswith('json'):
            s = s[4:]
        s = s.strip()
    try:
        parsed = json.loads(s)
        if not isinstance(parsed, dict):
            return {}, f'Expected dict, got {type(parsed).__name__}'

        # Новый формат: {"clusters": {...}, "assignments": {...}}
        if 'clusters' in parsed and 'assignments' in parsed:
            clusters = parsed.get('clusters') or {}
            assignments = parsed.get('assignments') or {}
            if not isinstance(clusters, dict) or not isinstance(assignments, dict):
                return {}, 'clusters/assignments must be dicts'

            result = {}
            unknown_cluster_ids = set()
            for key, value in assignments.items():
                # Формат A: {cluster_id: [payload_id, payload_id, ...]}
                if isinstance(value, list):
                    cluster_name = clusters.get(str(key))
                    if cluster_name is None:
                        unknown_cluster_ids.add(str(key))
                        continue
                    for pid in value:
                        result[str(pid)] = str(cluster_name)
                # Формат B (fallback): {payload_id: cluster_id}
                else:
                    cid_str = str(value)
                    cluster_name = clusters.get(cid_str)
                    if cluster_name is None:
                        unknown_cluster_ids.add(cid_str)
                        continue
                    result[str(key)] = str(cluster_name)

            if unknown_cluster_ids and not result:
                return {}, f'No valid assignments (unknown cluster ids: {sorted(unknown_cluster_ids)[:5]})'
            return result, None

        # Старый формат: {"1": "имя_кластера", ...}
        if all(isinstance(v, str) for v in parsed.values()):
            return parsed, None

        return {}, 'Unknown response format (no clusters/assignments and values are not strings)'
    except json.JSONDecodeError as e:
        return {}, f'JSON parse error: {e}'


def expand_clusters(
    llm_assignments: dict[str, str],
    unique_payloads: list[str],
    payload_to_keywords: dict[str, list[str]],
) -> tuple[dict[str, list[str]], int]:
    """
    Возвращает (cluster_to_keywords, unassigned_count).
    """
    cluster_to_keywords: dict[str, list[str]] = {}
    unassigned = 0
    
    for i, payload in enumerate(unique_payloads, 1):
        cluster = llm_assignments.get(str(i))
        kws = payload_to_keywords[payload]
        if cluster is None:
            unassigned += len(kws)
            cluster_to_keywords.setdefault('UNASSIGNED', []).extend(kws)
        else:
            cluster_to_keywords.setdefault(cluster, []).extend(kws)
    
    return cluster_to_keywords, unassigned


def extract_inputs_from_light_search(light_search_result: dict) -> dict:
    """Достаёт seed/keywords/region/language из результата /api/light-search."""
    seed = light_search_result.get('seed')
    keywords = light_search_result.get('keywords', [])
    
    l3_stats = light_search_result.get('l3_stats') or {}
    region = l3_stats.get('region', 'Украина')
    language = l3_stats.get('language', 'русский')
    
    if not seed:
        raise ValueError('seed missing in light_search_result')
    if not keywords:
        raise ValueError('keywords missing or empty in light_search_result')
    
    return {
        'seed': seed,
        'keywords': keywords,
        'region': region,
        'language': language,
    }


async def run_debug_payloads(light_search_result: dict) -> dict:
    """
    Извлекает хвосты, валидирует, возвращает диагностику.
    БЕЗ вызова LLM.
    """
    inputs = extract_inputs_from_light_search(light_search_result)
    seed = inputs['seed']
    keywords = inputs['keywords']
    
    t0 = time.time()
    unique_payloads, payload_to_keywords = build_payload_mapping(keywords, seed)
    t_extract = time.time() - t0
    
    validation = validate_payloads(seed, payload_to_keywords)
    
    seed_lemmas = sorted(get_seed_lemmas(seed))
    
    # Sample первых 50 хвостов
    sample = []
    for i, p in enumerate(unique_payloads[:50], 1):
        kws = payload_to_keywords[p]
        sample.append({
            'id': i,
            'payload': p if p else '(пусто)',
            'keywords_count': len(kws),
            'keywords': kws[:5],
        })
    
    # Топ дубли
    top_dups = sorted(
        payload_to_keywords.items(),
        key=lambda x: -len(x[1]),
    )[:15]
    top_dups_out = [
        {'payload': p if p else '(пусто)', 'keywords_count': len(kws), 'keywords': kws}
        for p, kws in top_dups
    ]
    
    return {
        'seed': seed,
        'region': inputs['region'],
        'language': inputs['language'],
        'seed_lemmas': seed_lemmas,
        'input_keywords_count': len(keywords),
        'unique_payloads_count': len(unique_payloads),
        'dedup_ratio': round(len(unique_payloads) / len(keywords), 3),
        'extract_time_sec': round(t_extract, 3),
        'warnings': validation['warnings'],
        'stats': validation['stats'],
        'payloads_sample': sample,
        'top_duplicates': top_dups_out,
    }


async def run_clustering(
    light_search_result: dict,
    model: str,
) -> dict:
    """
    Полный пайплайн: extract → LLM → expand.
    """
    t_total_start = time.time()
    inputs = extract_inputs_from_light_search(light_search_result)
    seed = inputs['seed']
    keywords = inputs['keywords']
    
    # 1. Извлечение хвостов
    t0 = time.time()
    unique_payloads, payload_to_keywords = build_payload_mapping(keywords, seed)
    t_extract = time.time() - t0
    
    # 2. Промпт
    system_prompt, user_prompt = build_prompts(
        seed=seed,
        region=inputs['region'],
        language=inputs['language'],
        payloads=unique_payloads,
    )
    
    # 3. LLM
    errors = []
    try:
        llm_result = await call_llm(model, system_prompt, user_prompt)
    except Exception as e:
        return {
            'model': model,
            'seed': seed,
            'errors': [f'LLM call failed: {type(e).__name__}: {e}'],
            'wall_time_sec': round(time.time() - t_total_start, 3),
        }
    
    # 4. Парсинг JSON
    assignments, parse_error = parse_llm_json(llm_result['raw_response'])
    json_parse_ok = parse_error is None
    if parse_error:
        errors.append(parse_error)
    
    # 5. Разворот на ключи
    cluster_to_keywords, unassigned = expand_clusters(
        assignments, unique_payloads, payload_to_keywords,
    )
    
    cluster_sizes = {c: len(kws) for c, kws in cluster_to_keywords.items()}
    sorted_sizes = sorted(cluster_sizes.items(), key=lambda x: -x[1])
    
    cost = calc_cost(model, llm_result['tokens_in'], llm_result['tokens_out'])
    wall_time = time.time() - t_total_start
    
    max_size = sorted_sizes[0][1] if sorted_sizes else 0
    max_pct = round(100 * max_size / len(keywords), 1) if keywords else 0
    
    return {
        'model': model,
        'seed': seed,
        'region': inputs['region'],
        'language': inputs['language'],
        
        'input_keywords_count': len(keywords),
        'unique_payloads_count': len(unique_payloads),
        
        'clusters': cluster_to_keywords,
        'cluster_sizes': dict(sorted_sizes),
        
        'metrics': {
            'wall_time_sec': round(wall_time, 3),
            'extract_time_sec': round(t_extract, 3),
            'api_time_sec': llm_result['api_time_sec'],
            'tokens_input': llm_result['tokens_in'],
            'tokens_output': llm_result['tokens_out'],
            'cost_usd': cost,
            'json_parse_ok': json_parse_ok,
            'clusters_count': len(cluster_to_keywords),
            'unassigned_keywords': unassigned,
            'max_cluster_size': max_size,
            'max_cluster_pct': max_pct,
            'estimated_input_tokens': estimate_tokens(system_prompt + user_prompt),
        },
        
        'raw_llm_response': llm_result['raw_response'],
        'errors': errors,
    }
