"""
Runner: оркестрирует пайплайн extract → validate → LLM → expand.

C1-архитектура: входные хвосты режутся на чанки по CHUNK_SIZE, каждый чанк
обрабатывается параллельно через call_llm. Имена кластеров между чанками
объединяются дополнительным дешёвым merge-вызовом через MERGE_MODEL.
"""
import asyncio
import json
import re
import time
from collections import Counter

from .payload_extractor import build_payload_mapping, get_seed_lemmas, estimate_tokens
from .payload_validator import validate_payloads
from .prompts import build_prompts
from .llm_client import call_llm, calc_cost


# === C1 параметры ===
# Размер чанка (хвостов на один LLM-вызов).
# 185 — даёт 3 чанка на 553 хвоста (185+185+183),
# для проверки оптимума параллелизма.
CHUNK_SIZE = 185

# Модель для merge-вызова. Pro даёт 29→8 (эталон) но 10с и $0.0072 — слишком дорого.
# 3-flash должен дать промежуточный результат: ~10-12 кластеров за ~3-4с.
MERGE_MODEL = 'gemini-3-flash-preview'

# Ретраи параметры
CHUNK_RETRY_DELAY_SEC = 1.0
CHUNK_MAX_RETRIES = 1  # сначала первая попытка, потом 1 retry


# Регулярка для пары вида `A(имя_кластера)`: захватывает код и имя.
# Имя может содержать кириллицу, латиницу, цифры, подчёркивания, пробелы, дефисы.
_CODE_WITH_NAME_RE = re.compile(r'^([A-Z]+)\(([^)]+)\)$')


def parse_llm_json(raw: str) -> tuple[dict, str | None]:
    """
    Парсит динамический однострочный формат:

        1:A(гео_город),2:B(коммерция),3:A,4:C(инфо_отзывы),5:B,6:A

    Каждая пара — это `НОМЕР:КОД` или `НОМЕР:КОД(ИМЯ)`.
    Имя кластера указывается в скобках только при первом использовании кода;
    при повторных использованиях — только буква.

    Стратегия восстановления при ошибках:
      - Код встречен без имени до того как был определён → unknown_code, хвост unassigned
      - Битая пара (без `:`, без числа, без буквы) → пропуск с warning
      - Дубль pid → берётся первое присвоение, warning
      - Markdown-обёртка ``` → срезается

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

    # Удаляем переносы строк (формат должен быть одной строкой, но модель может
    # пропустить эту инструкцию)
    s = s.replace('\n', '').replace('\r', '').strip()

    # Разбиваем по запятым на пары. ВАЖНО: запятая может быть внутри имени
    # кластера в скобках, поэтому считаем глубину скобок.
    pairs: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in s:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth = max(0, depth - 1)
            current.append(ch)
        elif ch == ',' and depth == 0:
            if current:
                pairs.append(''.join(current).strip())
                current = []
        else:
            current.append(ch)
    if current:
        pairs.append(''.join(current).strip())

    if not pairs:
        return {}, 'No pairs found in response'

    code_to_name: dict[str, str] = {}
    pid_to_code: dict[str, str] = {}
    bad_pairs: list[str] = []
    duplicate_ids: set[str] = set()

    for pair in pairs:
        if not pair:
            continue
        colon_idx = pair.find(':')
        if colon_idx == -1:
            bad_pairs.append(pair[:40])
            continue
        pid = pair[:colon_idx].strip()
        rest = pair[colon_idx + 1:].strip()
        if not pid.isdigit() or not rest:
            bad_pairs.append(pair[:40])
            continue

        # rest может быть либо `A` (только код), либо `A(имя_кластера)`
        m = _CODE_WITH_NAME_RE.match(rest)
        if m:
            code = m.group(1)
            name = m.group(2).strip()
            # Если код уже определён с другим именем — оставляем первое определение
            if code not in code_to_name and name:
                code_to_name[code] = name
        else:
            # Только код без скобок: должна быть валидной буквенной группой A-Z
            if not rest.isalpha() or not rest.isupper():
                bad_pairs.append(pair[:40])
                continue
            code = rest

        if pid in pid_to_code:
            duplicate_ids.add(pid)
            continue
        pid_to_code[pid] = code

    # Преобразуем pid → code в pid → cluster_name через legend
    result: dict[str, str] = {}
    unknown_codes: set[str] = set()
    for pid, code in pid_to_code.items():
        cluster_name = code_to_name.get(code)
        if cluster_name is None:
            unknown_codes.add(code)
            continue
        result[pid] = cluster_name

    warnings: list[str] = []
    if unknown_codes:
        warnings.append(f'Codes used without name definition: {sorted(unknown_codes)[:5]}')
    if bad_pairs:
        warnings.append(f'Skipped {len(bad_pairs)} bad pair(s). First: {bad_pairs[0]!r}')
    if duplicate_ids:
        warnings.append(f'Duplicate payload ids: {sorted(duplicate_ids, key=lambda x: int(x))[:5]}')
    if not code_to_name:
        warnings.append('No legend entries (no code with name found)')

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


# ============================================================
# C1 helpers: чанкинг, параллельный вызов с ретраем, merge имён
# ============================================================

def _chunk_payloads(payloads: list[str], size: int) -> list[list[str]]:
    """Нарезает список хвостов на чанки фиксированного размера.
    Последний чанк может быть короче."""
    return [payloads[i:i + size] for i in range(0, len(payloads), size)]


async def _call_chunk_with_retry(
    chunk_idx: int,
    chunk_payloads: list[str],
    seed: str,
    region: str,
    language: str,
    model: str,
) -> dict:
    """
    Вызывает LLM для одного чанка с одним ретраем при ошибке.
    Возвращает структуру с assignments (local pid → cluster_name) и диагностикой.

    Никогда не бросает исключение — все ошибки упаковываются в поле 'error'.
    """
    diag = {
        'chunk_idx': chunk_idx,
        'tokens_in': 0,
        'tokens_out': 0,
        'api_time_sec': 0.0,
        'json_parse_ok': False,
        'clusters_count': 0,
        'retries': 0,
        'raw_response': '',
        'parse_error': None,
    }

    system_prompt, user_prompt = build_prompts(
        seed=seed,
        region=region,
        language=language,
        payloads=chunk_payloads,
    )

    last_error: str | None = None
    llm_result: dict | None = None
    for attempt in range(CHUNK_MAX_RETRIES + 1):
        diag['retries'] = attempt  # Записываем номер попытки независимо от исхода
        try:
            llm_result = await call_llm(model, system_prompt, user_prompt)
            last_error = None
            break
        except Exception as e:
            last_error = f'{type(e).__name__}: {e}'
            if attempt < CHUNK_MAX_RETRIES:
                await asyncio.sleep(CHUNK_RETRY_DELAY_SEC)

    if llm_result is None:
        return {
            'chunk_idx': chunk_idx,
            'assignments': {},
            'diag': diag,
            'error': f'LLM call failed after {CHUNK_MAX_RETRIES + 1} attempts: {last_error}',
        }

    diag['tokens_in'] = llm_result['tokens_in']
    diag['tokens_out'] = llm_result['tokens_out']
    diag['api_time_sec'] = llm_result['api_time_sec']
    diag['raw_response'] = llm_result['raw_response']

    # Парсим — формат тот же что в проде (1:A(имя),2:B,...)
    assignments, parse_error = parse_llm_json(llm_result['raw_response'])
    diag['json_parse_ok'] = parse_error is None
    diag['parse_error'] = parse_error
    diag['clusters_count'] = len(set(assignments.values())) if assignments else 0

    return {
        'chunk_idx': chunk_idx,
        'assignments': assignments,
        'diag': diag,
        'error': None,
    }


async def _merge_cluster_names(
    prefixed_names: list[str],
    seed: str,
    language: str,
    merge_model: str,
) -> tuple[dict[str, str], dict]:
    """
    Делает один LLM-вызов в дешёвую модель: группирует синонимы среди имён
    кластеров, пришедших из разных чанков.

    Args:
        prefixed_names: список имён вида ['c0:цена_общая', 'c1:стоимость_имплантов', ...].
        seed: исходный сид (для контекста модели).
        language: язык (для имён).
        merge_model: модель для merge-вызова (обычно flash-lite).

    Returns:
        (remap, meta) где
          remap: dict prefixed_name → canonical_name
          meta: {'tokens_in', 'tokens_out', 'cost_usd'}

    Если LLM вернёт некорректный JSON — фолбэк: каждое имя само себе каноническое
    (без префикса). Это означает merge не сработал, но пайплайн не падает.
    """
    # Уникализируем входной список (на всякий случай) с сохранением порядка
    seen = set()
    unique_names = []
    for n in prefixed_names:
        if n not in seen:
            seen.add(n)
            unique_names.append(n)

    # Промпт: просим вернуть JSON-объект {prefixed_name: canonical_name}
    # Каноническое имя — короткое (1-3 слова на нужном языке), без префикса.
    sys_prompt = (
        'Ты объединяешь близкие по смыслу названия кластеров поисковых запросов '
        'в общие категории. На вход — список названий с префиксом cN: (где N — номер чанка).\n\n'
        'ПРАВИЛА СКЛЕЙКИ. Объединяй имена под одним каноническим именем, если они:\n'
        '• описывают близкий или пересекающийся интент пользователя;\n'
        '• одно из них является подмножеством другого (например "Отзывы" и "Отзывы и рейтинг");\n'
        '• описывают один аспект продукта (цена, гео, отзывы, виды/типы, процесс, противопоказания);\n'
        '• содержат общий ключевой смысл, даже если формулировки разные.\n\n'
        'ПРИМЕРЫ СКЛЕЙКИ:\n'
        '• "Отзывы" + "Отзывы и рейтинг" + "Форум" → "Отзывы"\n'
        '• "Виды и технологии" + "Виды и методы" + "Виды имплантов" → "Виды имплантации"\n'
        '• "Риски" + "Риски и противопоказания" + "Противопоказания" → "Противопоказания"\n'
        '• "Видео" + "Фото" + "Видео и фото" → "Фото и видео"\n'
        '• "Акции" + "Акции и скидки" + "Льготы" → "Акции и скидки"\n'
        '• "Цена" + "Стоимость" + "Цены и условия" → "Цена"\n\n'
        'НЕ СКЛЕИВАЙ имена, описывающие разные интенты:\n'
        '• "Цена" и "Гео" — разные интенты\n'
        '• "Виды имплантации" и "Процесс установки" — разные аспекты\n\n'
        f'ЦЕЛЬ: финальное число уникальных канонических имён должно быть 5–12. '
        f'Если входных имён много — агрегируй смело, но не теряй принципиально разные интенты. '
        f'Каноническое имя — короткое, 1–3 слова на {language}, без префикса cN:.\n\n'
        'ФОРМАТ ОТВЕТА — СТРОГО валидный JSON-объект, без markdown, без пояснений:\n'
        '{"cN:исходное_имя": "каноническое_имя", ...}\n\n'
        'Каждое исходное имя из входного списка должно присутствовать как ключ.'
    )
    user_prompt = (
        f'Сид: {seed}\n'
        f'Язык: {language}\n\n'
        f'Имена кластеров ({len(unique_names)}):\n'
        + '\n'.join(unique_names)
    )

    llm_result = await call_llm(merge_model, sys_prompt, user_prompt)
    cost = calc_cost(merge_model, llm_result['tokens_in'], llm_result['tokens_out'])
    meta = {
        'tokens_in': llm_result['tokens_in'],
        'tokens_out': llm_result['tokens_out'],
        'cost_usd': cost,
    }

    raw = llm_result['raw_response'].strip()
    # Срезаем markdown
    if raw.startswith('```'):
        raw = raw.strip('`')
        first_nl = raw.find('\n')
        if first_nl != -1 and ' ' not in raw[:first_nl]:
            raw = raw[first_nl + 1:]
        raw = raw.strip()
    # Иногда модель оборачивает в ```json
    if raw.lower().startswith('json'):
        raw = raw[4:].strip()

    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError('merge response is not a JSON object')
    except Exception:
        # Фолбэк: каждое имя само себе каноническое (без префикса)
        remap = {n: (n.split(':', 1)[1] if ':' in n else n) for n in unique_names}
        return remap, meta

    # Применяем remap. Если каких-то имён нет в ответе — фолбэк на их имя без префикса.
    remap: dict[str, str] = {}
    for n in unique_names:
        canonical = parsed.get(n)
        if isinstance(canonical, str) and canonical.strip():
            remap[n] = canonical.strip()
        else:
            remap[n] = n.split(':', 1)[1] if ':' in n else n

    return remap, meta


async def run_clustering(
    light_search_result: dict,
    model: str,
) -> dict:
    """
    Полный пайплайн (C1: chunked parallel + merge):
      extract → 6 параллельных LLM вызовов → merge имён → expand.
    """
    t_total_start = time.time()
    inputs = extract_inputs_from_light_search(light_search_result)
    seed = inputs['seed']
    keywords = inputs['keywords']
    region = inputs['region']
    language = inputs['language']

    # 1. Извлечение хвостов
    t0 = time.time()
    unique_payloads, payload_to_keywords = build_payload_mapping(keywords, seed)
    t_extract = time.time() - t0

    # 2. Нарезка на чанки
    chunks = _chunk_payloads(unique_payloads, CHUNK_SIZE)

    # 3. Параллельные LLM вызовы по всем чанкам
    t_chunks_start = time.time()
    chunk_tasks = [
        _call_chunk_with_retry(
            chunk_idx=i,
            chunk_payloads=chunk,
            seed=seed,
            region=region,
            language=language,
            model=model,
        )
        for i, chunk in enumerate(chunks)
    ]
    chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=False)
    t_chunks = time.time() - t_chunks_start

    # 4. Сборка глобального assignments (offset по индексам внутри чанка)
    # и сбор всех уникальных имён кластеров с префиксом чанка.
    global_assignments: dict[str, str] = {}  # global_pid → prefixed_name
    all_prefixed_names: list[str] = []  # для merge-вызова
    chunk_diag: list[dict] = []
    chunk_errors: list[str] = []
    failed_chunks = 0

    for cr in chunk_results:
        chunk_diag.append(cr['diag'])
        if cr['error']:
            chunk_errors.append(f"chunk {cr['chunk_idx']}: {cr['error']}")
            failed_chunks += 1
            continue

        offset = cr['chunk_idx'] * CHUNK_SIZE
        for local_pid, name in cr['assignments'].items():
            prefixed = f"c{cr['chunk_idx']}:{name}"
            if prefixed not in all_prefixed_names:
                all_prefixed_names.append(prefixed)
            global_pid = str(offset + int(local_pid))
            global_assignments[global_pid] = prefixed

    # 5. Merge имён кластеров через дешёвый LLM-вызов
    t_merge_start = time.time()
    merge_diag: dict = {
        'attempted': False,
        'names_before': len(all_prefixed_names),
        'names_after': len(all_prefixed_names),
        'merge_time_sec': 0.0,
        'merge_tokens_in': 0,
        'merge_tokens_out': 0,
        'merge_cost_usd': 0.0,
        'merge_error': None,
    }
    name_remap: dict[str, str] = {}  # prefixed_name → canonical_name
    if len(all_prefixed_names) > 1:
        merge_diag['attempted'] = True
        try:
            name_remap, merge_meta = await _merge_cluster_names(
                prefixed_names=all_prefixed_names,
                seed=seed,
                language=language,
                merge_model=MERGE_MODEL,
            )
            merge_diag['merge_tokens_in'] = merge_meta['tokens_in']
            merge_diag['merge_tokens_out'] = merge_meta['tokens_out']
            merge_diag['merge_cost_usd'] = merge_meta['cost_usd']
            merge_diag['names_after'] = len(set(name_remap.values()))
        except Exception as e:
            merge_diag['merge_error'] = f'{type(e).__name__}: {e}'
            # Фолбэк: каждое имя само себе каноническое (без чанк-префикса)
            for n in all_prefixed_names:
                name_remap[n] = n.split(':', 1)[1] if ':' in n else n
    else:
        # Один кластер на всё — нечего мерджить
        for n in all_prefixed_names:
            name_remap[n] = n.split(':', 1)[1] if ':' in n else n
    merge_diag['merge_time_sec'] = round(time.time() - t_merge_start, 3)

    # Применяем remap к global_assignments
    final_assignments: dict[str, str] = {}
    for gpid, prefixed in global_assignments.items():
        final_assignments[gpid] = name_remap.get(
            prefixed, prefixed.split(':', 1)[1] if ':' in prefixed else prefixed
        )

    # 6. Разворот на ключи
    cluster_to_keywords, unassigned = expand_clusters(
        final_assignments, unique_payloads, payload_to_keywords,
    )

    cluster_sizes = {c: len(kws) for c, kws in cluster_to_keywords.items()}
    sorted_sizes = sorted(cluster_sizes.items(), key=lambda x: -x[1])

    # Агрегаты по чанкам
    total_tokens_in = sum(d['tokens_in'] for d in chunk_diag) + merge_diag['merge_tokens_in']
    total_tokens_out = sum(d['tokens_out'] for d in chunk_diag) + merge_diag['merge_tokens_out']
    chunk_cost = sum(
        calc_cost(model, d['tokens_in'], d['tokens_out']) for d in chunk_diag
    )
    total_cost = round(chunk_cost + merge_diag['merge_cost_usd'], 6)

    chunk_api_times = [d['api_time_sec'] for d in chunk_diag]
    max_chunk_api = max(chunk_api_times) if chunk_api_times else 0.0
    json_parse_ok = all(d['json_parse_ok'] for d in chunk_diag) and not failed_chunks

    wall_time = time.time() - t_total_start
    max_size = sorted_sizes[0][1] if sorted_sizes else 0
    max_pct = round(100 * max_size / len(keywords), 1) if keywords else 0

    # Собираем ошибки из всех источников
    all_errors: list[str] = []
    all_errors.extend(chunk_errors)
    if merge_diag['merge_error']:
        all_errors.append(f'merge: {merge_diag["merge_error"]}')
    for d in chunk_diag:
        if d.get('parse_error'):
            all_errors.append(f"chunk {d['chunk_idx']} parse: {d['parse_error']}")

    # Сырые ответы чанков для отладки
    raw_responses = {
        f'chunk_{d["chunk_idx"]}': d.get('raw_response', '')
        for d in chunk_diag
    }

    return {
        'model': model,
        'seed': seed,
        'region': region,
        'language': language,

        'input_keywords_count': len(keywords),
        'unique_payloads_count': len(unique_payloads),

        'clusters': cluster_to_keywords,
        'cluster_sizes': dict(sorted_sizes),

        'metrics': {
            'wall_time_sec': round(wall_time, 3),
            'extract_time_sec': round(t_extract, 3),
            'chunks_phase_time_sec': round(t_chunks, 3),
            'max_chunk_api_time_sec': round(max_chunk_api, 3),
            'merge_time_sec': merge_diag['merge_time_sec'],
            'api_time_sec': round(t_chunks + merge_diag['merge_time_sec'], 3),

            'tokens_input': total_tokens_in,
            'tokens_output': total_tokens_out,
            'cost_usd': total_cost,

            'json_parse_ok': json_parse_ok,
            'clusters_count': len(cluster_to_keywords),
            'unassigned_keywords': unassigned,
            'max_cluster_size': max_size,
            'max_cluster_pct': max_pct,

            # === C1-специфичная диагностика ===
            'chunks_count': len(chunks),
            'chunk_size': CHUNK_SIZE,
            'failed_chunks': failed_chunks,
            'chunk_api_times_sec': [round(t, 3) for t in chunk_api_times],
            'chunk_tokens_in': [d['tokens_in'] for d in chunk_diag],
            'chunk_tokens_out': [d['tokens_out'] for d in chunk_diag],
            'chunk_clusters_count': [d.get('clusters_count', 0) for d in chunk_diag],
            'chunk_retries': [d.get('retries', 0) for d in chunk_diag],
            'merge_diag': merge_diag,
        },

        'raw_llm_responses_per_chunk': raw_responses,
        'errors': all_errors,
    }
