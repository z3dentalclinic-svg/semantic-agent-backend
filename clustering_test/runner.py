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
    Парсит блочный позиционный формат:

        A=гео_город;B=цена;C=бренд_сервис;D=коммерция
        ---
        1:ABCDABCDABCDABCDABCDABCDA
        26:BCDABCDABCDABCDABCDABCDAB
        51:CDABCDABCD

    Первая непустая строка — легенда (пары `КОД=имя` через `;`).
    Затем строка-разделитель `---`.
    Далее блоки `СТАРТ:КОДЫ`, где СТАРТ — позиция первого хвоста в блоке (1-индексация),
    КОДЫ — подряд без разделителей односимвольные коды для 25 хвостов
    (последний блок может быть короче).

    Стратегия восстановления при ошибках:
      - Блоки-якоря (СТАРТ:) — главный ориентир. Если блок короче ожидаемого,
        недостающие хвосты остаются неприсвоенными (locally bounded error).
      - Если блок длиннее 25 символов, лишние символы игнорируются — следующий
        блок не сдвигается, потому что у него свой якорь.
      - Неизвестные коды → unknown_labels, хвост остаётся неприсвоенным.

    Возвращает (assignments_dict, error_msg) где assignments_dict
    имеет вид {payload_id_str: cluster_name_str}.

    Имя `parse_llm_json` сохранено ради совместимости с вызывающим кодом.
    """
    BLOCK_SIZE = 25

    s = raw.strip()
    # Срезаем markdown-обёртку если модель её добавила
    if s.startswith('```'):
        s = s.strip('`')
        first_nl = s.find('\n')
        if first_nl != -1 and ' ' not in s[:first_nl]:
            s = s[first_nl + 1:]
        s = s.strip()

    if not s:
        return {}, 'Empty response'

    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    if len(lines) < 2:
        return {}, f'Expected legend + separator + blocks, got {len(lines)} non-empty line(s)'

    legend_line = lines[0]

    # Ищем разделитель `---`. Он должен быть на второй строке, но допустим
    # отсутствие если модель забыла — тогда блоки начинаются сразу со строки 2.
    if len(lines) >= 2 and lines[1] == '---':
        block_lines = lines[2:]
    else:
        block_lines = lines[1:]

    if not block_lines:
        return {}, 'No assignment blocks found after legend'

    # Парсим легенду: "A=имя;B=имя;C=имя"
    label_to_name: dict[str, str] = {}
    bad_legend: list[str] = []
    for tok in legend_line.split(';'):
        tok = tok.strip()
        if not tok:
            continue
        eq_idx = tok.find('=')
        if eq_idx == -1:
            bad_legend.append(tok[:60])
            continue
        label = tok[:eq_idx].strip()
        name = tok[eq_idx + 1:].strip()
        if not label or not name:
            bad_legend.append(tok[:60])
            continue
        label_to_name[label] = name

    if not label_to_name:
        first = bad_legend[0] if bad_legend else legend_line[:80]
        return {}, f'No valid legend entries. First problem: {first!r}'

    # Парсим блоки присвоений: "СТАРТ:КОДЫ"
    result: dict[str, str] = {}
    unknown_labels: set[str] = set()
    bad_blocks: list[str] = []
    short_blocks = 0
    long_blocks = 0
    duplicate_ids: set[str] = set()

    # Сначала валидируем заголовки блоков, чтобы знать какой блок последний
    parsed_blocks: list[tuple[int, str]] = []
    for block in block_lines:
        colon_idx = block.find(':')
        if colon_idx == -1:
            bad_blocks.append(block[:40])
            continue
        start_str = block[:colon_idx].strip()
        codes = block[colon_idx + 1:].strip()
        if not start_str.isdigit() or not codes:
            bad_blocks.append(block[:40])
            continue
        parsed_blocks.append((int(start_str), codes))

    last_idx = len(parsed_blocks) - 1
    for idx, (start, codes) in enumerate(parsed_blocks):
        is_last = idx == last_idx
        if len(codes) < BLOCK_SIZE and not is_last:
            short_blocks += 1
        elif len(codes) > BLOCK_SIZE:
            long_blocks += 1
            codes = codes[:BLOCK_SIZE]  # берём ровно BLOCK_SIZE, лишнее игнорируем

        for offset, label in enumerate(codes):
            pid = str(start + offset)
            if pid in result:
                duplicate_ids.add(pid)
                continue
            cluster_name = label_to_name.get(label)
            if cluster_name is None:
                unknown_labels.add(label)
                continue
            result[pid] = cluster_name

    warnings: list[str] = []
    if unknown_labels:
        warnings.append(f'Unknown labels: {sorted(unknown_labels)[:5]}')
    if bad_blocks:
        warnings.append(f'Skipped {len(bad_blocks)} bad block(s). First: {bad_blocks[0]!r}')
    if short_blocks:
        warnings.append(f'{short_blocks} block(s) shorter than {BLOCK_SIZE}')
    if long_blocks:
        warnings.append(f'{long_blocks} block(s) longer than {BLOCK_SIZE}, truncated')
    if duplicate_ids:
        warnings.append(f'Duplicate payload ids: {sorted(duplicate_ids, key=lambda x: int(x))[:5]}')
    if bad_legend:
        warnings.append(f'Skipped {len(bad_legend)} bad legend entry(ies)')

    if not result:
        return {}, '; '.join(warnings) if warnings else 'No valid assignments parsed'

    return result, ('; '.join(warnings) if warnings else None)


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
