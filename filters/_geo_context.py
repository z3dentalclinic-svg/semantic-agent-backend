"""
Shared geo-context guards (G4/G6) — применяются во всех geo-фильтрах.

G4 — adj-город + street marker справа → улица/район внутри seed_city,
     не чужой город. Примеры: 'харьковский район', 'днепровское шоссе'.

G6 — жк/мкр/микрорайон слева от топонима → название ЖК/комплекса,
     не город. Примеры: 'жк софия', 'мкр крылатское', 'жк паркленд'.

Guards принимают words[] (список слов в их исходном порядке) и индекс/слово.
Возвращают True если слово НЕ является независимым топонимом (skip lookup).

ИСПОЛЬЗОВАНИЕ:
  from filters._geo_context import is_street_name_context, is_complex_name_context

  # внутри цикла по словам:
  if is_street_name_context(raw_word, word_norm, word_positions, words):
      continue
  if is_complex_name_context(raw_word, word_positions, words):
      continue
"""

# G4 — маркеры улично-районной структуры.
# Нарицательные существительные которые после адъективной формы города
# превращают его в название улицы/района внутри текущего города.
STREET_MARKERS = frozenset({
    # RU
    'район', 'шоссе', 'массив', 'проспект', 'улица', 'переулок', 'площадь',
    'бульвар', 'переезд', 'вокзал', 'набережная', 'аллея', 'тупик',
    'квартал', 'проезд', 'станция', 'метро',
    # UA
    'вулиця', 'провулок', 'майдан', 'набережна', 'площа', 'станція',
    # Сокращения
    'ул', 'пр', 'просп', 'пер', 'пл', 'ст', 'наб', 'бул',
})

# G6 — маркеры жилого/торгового комплекса.
# После маркера идёт произвольное название, которое не должно трактоваться
# как независимый топоним.
COMPLEX_MARKERS = frozenset({
    # RU
    'жк', 'мкр', 'микрорайон', 'жилой', 'комплекс', 'тц', 'трц', 'бц',
    'жм', 'жилмассив',
    # UA
    'житловий',
})

# Адъективные суффиксы для распознавания adj-формы города.
_ADJ_SUFFIXES = (
    # RU
    'ский', 'ская', 'ское', 'ской', 'ским', 'ского', 'ским', 'ских',
    # UA
    'ськ', 'ська', 'ське', 'ською', 'ському', 'ських',
)


def is_street_name_context(raw_word, word_norm, word_positions, words):
    """
    G4: raw_word — адъективная форма города AND следующее слово —
    маркер улицы/района.

    Пример срабатывания:
      raw_word='харьковский' word_norm='харьков' words[pos+1]='район' → True
      raw_word='днепровское' word_norm='днепр'   words[pos+1]='шоссе' → True

    НЕ срабатывает:
      raw_word='николаев' word_norm='николаев' → raw==norm → не адъектив → False
      raw_word='харьков'  word_norm='харьков'  → не адъектив → False
      raw_word='харьковский' но справа ничего → False
    """
    # Не адъектив (raw == norm означает чистое имя города, не склоняемое)
    if raw_word == word_norm:
        return False
    if not any(raw_word.endswith(s) for s in _ADJ_SUFFIXES):
        return False
    # Справа — street marker?
    pos = word_positions.get(raw_word, -1)
    if pos < 0 or pos + 1 >= len(words):
        return False
    return words[pos + 1] in STREET_MARKERS


def is_complex_name_context(raw_word, word_positions, words):
    """
    G6: слева от raw_word — complex_marker (жк/мкр/микрорайон).
    Значит raw_word — название ЖК, не независимый топоним.

    Пример:
      words=['курсы','английского','жк','софия'] raw_word='софия'
      → pos=3, words[2]='жк' → True

      words=['курсы','киев','паркленд'] raw_word='паркленд'
      → pos=2, words[1]='киев' → False (киев не complex_marker)
    """
    pos = word_positions.get(raw_word, -1)
    if pos <= 0:
        return False
    return words[pos - 1] in COMPLEX_MARKERS
