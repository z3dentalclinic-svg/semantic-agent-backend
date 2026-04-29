"""
Тесты извлечения хвостов на разных типах сидов.
Запуск: pytest clustering_test/tests/
"""
import pytest

from clustering_test.payload_extractor import (
    extract_payload,
    get_seed_lemmas,
    build_payload_mapping,
)


CASES = [
    # (seed, keyword, expected_payload, comment)
    
    # Двусловный сид, простые случаи
    ('доставка цветов', 'доставка цветов в киеве', 'в киеве', 'базовый'),
    ('доставка цветов', 'в киеве доставка цветов', 'в киеве', 'обратный порядок'),
    ('доставка цветов', 'доставка цветов', '', 'сам сид'),
    
    # Лемматизация — формы сида в любом падеже
    ('доставка цветов', 'доставку цветов заказать', 'заказать', 'доставку→доставка'),
    ('доставка цветов', 'доставка цветом житомир', 'житомир', 'цветом→цвет'),
    ('доставка цветов', 'заказ цветов на дом', 'заказ на дом', 'цветов→цвет (только цветов вычитается)'),
    
    # Однословный сид
    ('нимесил', 'как и зачем принимать нимесил', 'как и зачем принимать', 'однословный'),
    ('нимесил', 'нимесил детям до года', 'детям до года', 'сид в начале'),
    ('нимесил', 'можно ли пить нимесил при простуде', 'можно ли пить при простуде', 'сид в середине'),
    
    # Латиница и цифры в ключе
    ('купить айфон 16', 'купить чехол на айфон 16', 'чехол на', 'цифры остаются если в сиде'),
    ('купить айфон 16', 'айфон 16 цена украина', 'цена украина', 'сид в начале'),
    
    # Спецсимволы и пунктуация
    ('доставка цветов', 'доставка.цветов', '', 'точка как разделитель'),
    ('доставка цветов', 'доставка цветов 24/7 киев', '24 7 киев', 'слэш'),
]


@pytest.mark.parametrize('seed,keyword,expected,comment', CASES)
def test_extract_payload(seed, keyword, expected, comment):
    seed_lemmas = get_seed_lemmas(seed)
    actual = extract_payload(keyword, seed_lemmas)
    assert actual == expected, f'[{comment}] expected "{expected}", got "{actual}"'


def test_dedup():
    """Перестановки слов внутри ключа дают одинаковый хвост."""
    keywords = [
        'доставка цветов в киеве',
        'в киеве доставка цветов',
        'доставку цветов в киеве',
    ]
    unique, mapping = build_payload_mapping(keywords, 'доставка цветов')
    assert len(unique) == 1
    assert unique[0] == 'в киеве'
    assert len(mapping['в киеве']) == 3


def test_empty_payload_collapses():
    """Сам сид и его пунктуационные варианты дают пустой хвост."""
    keywords = ['доставка цветов', 'доставка.цветов', 'цветов доставка']
    unique, mapping = build_payload_mapping(keywords, 'доставка цветов')
    assert len(unique) == 1
    assert unique[0] == ''
    assert len(mapping['']) == 3


def test_seed_lemmas_basic():
    assert get_seed_lemmas('доставка цветов') == frozenset({'доставка', 'цвет'})
    assert get_seed_lemmas('купить айфон 16') == frozenset({'купить', 'айфон', '16'})
    assert get_seed_lemmas('нимесил') == frozenset({'нимесил'})


if __name__ == '__main__':
    # Быстрый прогон без pytest
    failed = 0
    for seed, keyword, expected, comment in CASES:
        seed_lemmas = get_seed_lemmas(seed)
        actual = extract_payload(keyword, seed_lemmas)
        status = 'PASS' if actual == expected else 'FAIL'
        if actual != expected:
            failed += 1
        print(f'[{status}] {comment}')
        print(f'  seed:     "{seed}"')
        print(f'  keyword:  "{keyword}"')
        print(f'  expected: "{expected}"')
        print(f'  actual:   "{actual}"')
        print()
    print(f'Total: {len(CASES)} | Failed: {failed}')
