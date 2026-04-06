"""
shared_morph.py — единственный экземпляр pymorphy3.MorphAnalyzer для всего пакета L0.

Проблема: tail_extractor, function_detectors, tail_function_classifier создавали
каждый СВОЙ MorphAnalyzer → три независимых LRU-кэша → слово "купить" лематизируется
трижды вместо одного раза.

Решение: один синглтон, один LRU-кэш на весь процесс.
Все модули импортируют `morph` отсюда.
"""
import pymorphy3

morph: pymorphy3.MorphAnalyzer = pymorphy3.MorphAnalyzer()
