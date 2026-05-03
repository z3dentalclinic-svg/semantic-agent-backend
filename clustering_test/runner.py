"""
Runner: оркестрирует пайплайн extract → validate → LLM → expand.
"""
import json
import re
import time
from collections import Counter

from .payload_extractor import build_payload_mapping, get_seed_lemmas, estimate_tokens
from .payload_validator import validate_payloads
from .prompts import build_prompts
from .llm_client import call_llm, calc_cost


# Регулярка для пары вида `НОМЕР:КОД` (где КОД — буква A-Z, обычно одна).
_PAIR_RE = re.compile(r'^(\d+):([A-Z]+)$')

# Регулярка для записи легенды `КОД=имя кластера`.
# Имя — любые непустые символы до конца записи.
_LEGEND_ENTRY_RE = re.compile(r'^([A-Z]+)\s*=\s*(.+)$')


def parse_llm_json(raw: str) -> tuple[dict, str | None]:
    """
    Парсит двухстрочный формат A2: сначала разметка, потом легенда.

        1:A,2:B,3:A,4:C,5:B,6:A
        A=гео_город;B=коммерция;C=инфо_отзывы

    Стратегия:
      - Срезаем markdown-обёртку ``` если есть.
      - Разбиваем ответ на непустые строки.
      - Строка-разметка: содержит пары `N:БУКВА` через запятую (НЕ содержит '=').
      - Строка-легенда: содержит `БУКВА=имя` через ';' (или ',' как фолбэк).
        Распознаётся по наличию '=' в строке.
      - Если модель прислала всё одной строкой (склеенный формат) — пытаемся
        разделить по первому символу '=' назад до ближайшего токена `БУКВА`.

    Восстановление при ошибках:
      - Битая пара → пропуск с warning.
      - Код в разметке без записи в легенде → unassigned для этих pid + warning.
      - Дубль pid → первое присвоение, warning.
      - Дубль кода в легенде → первое определение, warning.

    Возвращает (assignments_dict, error_msg) где assignments_dict
    имеет вид {payload_id_str: cluster_name_str}.

    Имя `parse_llm_json` сохранено ради совместимости с вызывающим кодом.
    """
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

    # Разбиваем на непустые строки
    lines = [ln.strip() for ln in s.replace('\r', '').split('\n') if ln.strip()]
    if not lines:
        return {}, 'No content lines in response'

    # Классифицируем строки.
    # Признак разметки: строка содержит хотя бы одну пару `N:БУКВА`.
    # Признак легенды: строка содержит запись `БУКВА=`, и НЕ содержит пар `N:`.
    # Склеенный случай: строка содержит и пары и записи легенды → разметка
    # с последующим фолбэк-разделением.
    assignment_line: str | None = None
    legend_line: str | None = None

    has_pair_re = re.compile(r'\b\d+:[A-Z]')
    has_legend_re = re.compile(r'\b[A-Z]+\s*=')

    for ln in lines:
        has_pair = bool(has_pair_re.search(ln))
        has_legend = bool(has_legend_re.search(ln))

        if has_pair and assignment_line is None:
            # Строка с парами — это разметка (возможно со склеенной легендой)
            assignment_line = ln
            continue
        if has_legend and legend_line is None:
            legend_line = ln
            continue

    # Фолбэк: если ничего не классифицировалось — пытаемся работать с первой строкой
    if assignment_line is None and legend_line is None:
        assignment_line = lines[0]

    # Фолбэк склейки: если в строке-разметке есть '=' — это значит модель
    # склеила всё в одну строку. Разделим по первой букве, после которой идёт '=',
    # отделённой от предыдущей пары.
    if assignment_line and '=' in assignment_line and legend_line is None:
        # Ищем границу: точка где заканчивается разметка и начинается легенда.
        # Разметка состоит из токенов `N:БУКВА` через запятую.
        # Легенда начинается с `БУКВА=...`.
        # Граница — первый матч `,БУКВА=` или начало строки `БУКВА=`.
        m = re.search(r',([A-Z]+=)', assignment_line)
        if m:
            split_pos = m.start()
            legend_line = assignment_line[split_pos + 1:]
            assignment_line = assignment_line[:split_pos]
        else:
            # Совсем странный случай: '=' есть но не в виде `,БУКВА=`.
            # Оставляем как есть, парсер пар отбракует.
            pass

    warnings: list[str] = []

    # === Парсинг легенды ===
    code_to_name: dict[str, str] = {}
    duplicate_legend_codes: set[str] = set()

    if legend_line:
        # Разделитель — ';' (предпочтительно) или ',' как фолбэк
        if ';' in legend_line:
            entries = [e.strip() for e in legend_line.split(';') if e.strip()]
        else:
            entries = [e.strip() for e in legend_line.split(',') if e.strip()]

        bad_legend_entries: list[str] = []
        for entry in entries:
            m = _LEGEND_ENTRY_RE.match(entry)
            if not m:
                bad_legend_entries.append(entry[:40])
                continue
            code = m.group(1)
            name = m.group(2).strip()
            if not name:
                bad_legend_entries.append(entry[:40])
                continue
            if code in code_to_name:
                duplicate_legend_codes.add(code)
                continue
            code_to_name[code] = name

        if bad_legend_entries:
            warnings.append(
                f'Skipped {len(bad_legend_entries)} bad legend entry(s). '
                f'First: {bad_legend_entries[0]!r}'
            )
        if duplicate_legend_codes:
            warnings.append(
                f'Duplicate legend codes: {sorted(duplicate_legend_codes)[:5]}'
            )

    if not code_to_name:
        warnings.append('No legend entries parsed')

    # === Парсинг разметки ===
    pid_to_code: dict[str, str] = {}
    bad_pairs: list[str] = []
    duplicate_ids: set[str] = set()

    if assignment_line:
        # Разбиваем по запятым (никаких скобок здесь нет, простой split)
        pairs = [p.strip() for p in assignment_line.split(',') if p.strip()]

        for pair in pairs:
            m = _PAIR_RE.match(pair)
            if not m:
                bad_pairs.append(pair[:40])
                continue
            pid = m.group(1)
            code = m.group(2)

            if pid in pid_to_code:
                duplicate_ids.add(pid)
                continue
            pid_to_code[pid] = code

        if bad_pairs:
            warnings.append(
                f'Skipped {len(bad_pairs)} bad pair(s). First: {bad_pairs[0]!r}'
            )
        if duplicate_ids:
            warnings.append(
                f'Duplicate payload ids: '
                f'{sorted(duplicate_ids, key=lambda x: int(x))[:5]}'
            )
    else:
        warnings.append('No assignment line found')

    # === Соединение pid → code → cluster_name ===
    result: dict[str, str] = {}
    unknown_codes: set[str] = set()
    for pid, code in pid_to_code.items():
        cluster_name = code_to_name.get(code)
        if cluster_name is None:
            unknown_codes.add(code)
            continue
        result[pid] = cluster_name

    if unknown_codes:
        warnings.append(
            f'Codes used in markup but missing in legend: {sorted(unknown_codes)[:5]}'
        )

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
