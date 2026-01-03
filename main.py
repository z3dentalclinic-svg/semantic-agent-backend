"""
DELAY OPTIMIZATION TESTER
Поиск минимальной безопасной задержки между запросами к Google Autocomplete

Цель: Найти минимальный диапазон задержек при котором Google НЕ блокирует запросы

Метод:
1. Тестируем разные диапазоны задержек
2. Делаем 50 запросов с каждым диапазоном
3. Считаем % успешных запросов
4. Фиксируем время выполнения
5. Находим оптимальный баланс (скорость vs блокировки)
"""

import httpx
import asyncio
import random
import time
import json
from datetime import datetime
from typing import List, Tuple, Dict


# ============================================
# USER AGENTS
# ============================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]


# ============================================
# DELAY TESTER CLASS
# ============================================
class DelayTester:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        
        # Модификаторы для тестирования
        self.modifiers = list("абвгдежзийклмнопрстуфхцчшщэюя")
    
    async def fetch_suggestions(
        self, 
        query: str, 
        country: str = "UA", 
        language: str = "ru"
    ) -> Tuple[bool, int, float]:
        """
        Один запрос к Google Autocomplete
        
        Returns:
            (success, results_count, response_time)
        """
        params = {
            "client": "chrome",
            "q": query,
            "gl": country,
            "hl": language
        }
        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }
        
        start = time.time()
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(self.base_url, params=params, headers=headers)
                elapsed = time.time() - start
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list) and len(data) > 1:
                        results_count = len([s for s in data[1] if isinstance(s, str)])
                        return (True, results_count, elapsed)
                    else:
                        return (True, 0, elapsed)
                
                # Не 200 = возможно блокировка
                return (False, 0, elapsed)
                
        except Exception as e:
            elapsed = time.time() - start
            return (False, 0, elapsed)
    
    async def test_delay_range(
        self,
        min_delay: float,
        max_delay: float,
        num_requests: int = 50,
        seed: str = "ремонт пылесосов",
        country: str = "UA",
        language: str = "ru",
        verbose: bool = False
    ) -> Dict:
        """
        Тестируем один диапазон задержек
        
        Args:
            min_delay: минимальная задержка (сек)
            max_delay: максимальная задержка (сек)
            num_requests: количество запросов для теста
            seed: базовый запрос
            country: код страны
            language: код языка
            verbose: выводить детали каждого запроса
            
        Returns:
            dict с результатами теста
        """
        print(f"\n{'='*70}")
        print(f"🧪 ТЕСТ: Задержка {min_delay}-{max_delay} сек")
        print(f"{'='*70}")
        print(f"Количество запросов: {num_requests}")
        print(f"Seed: '{seed}'")
        print()
        
        successes = 0
        failures = 0
        total_results = 0
        response_times = []
        
        start_time = time.time()
        
        # Делаем запросы
        for i in range(num_requests):
            # Используем разные модификаторы
            modifier = self.modifiers[i % len(self.modifiers)]
            query = f"{seed} {modifier}"
            
            # Запрос
            success, results, resp_time = await self.fetch_suggestions(query, country, language)
            
            # Статистика
            if success:
                successes += 1
                total_results += results
            else:
                failures += 1
            
            response_times.append(resp_time)
            
            # Лог
            if verbose:
                status = "✅" if success else "❌"
                print(f"[{i+1}/{num_requests}] {status} '{query}' → {results} results ({resp_time:.3f}s)")
            elif (i + 1) % 10 == 0:
                # Показываем прогресс каждые 10 запросов
                print(f"[{i+1}/{num_requests}] Успешно: {successes}, Неудачно: {failures}")
            
            # Задержка (кроме последнего запроса)
            if i < num_requests - 1:
                delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(delay)
        
        # Общее время
        total_time = time.time() - start_time
        
        # Средние значения
        success_rate = (successes / num_requests) * 100
        avg_response_time = sum(response_times) / len(response_times)
        avg_results_per_request = total_results / num_requests if num_requests > 0 else 0
        
        # Результаты
        result = {
            "delay_range": (min_delay, max_delay),
            "num_requests": num_requests,
            "successes": successes,
            "failures": failures,
            "success_rate": round(success_rate, 2),
            "total_results": total_results,
            "avg_results_per_request": round(avg_results_per_request, 2),
            "total_time": round(total_time, 2),
            "avg_response_time": round(avg_response_time, 3),
            "avg_delay": round((min_delay + max_delay) / 2, 2)
        }
        
        # Вывод результатов
        print(f"\n{'='*70}")
        print(f"📊 РЕЗУЛЬТАТЫ")
        print(f"{'='*70}")
        print(f"Успешно:              {successes}/{num_requests} ({success_rate:.1f}%)")
        print(f"Неудачно:             {failures}/{num_requests} ({100-success_rate:.1f}%)")
        print(f"Всего результатов:    {total_results}")
        print(f"Среднее на запрос:    {avg_results_per_request:.1f} results")
        print(f"Общее время:          {total_time:.2f} сек")
        print(f"Среднее время ответа: {avg_response_time:.3f} сек")
        print(f"Средняя задержка:     {(min_delay + max_delay)/2:.2f} сек")
        
        # Оценка
        if success_rate >= 98:
            print(f"✅ ОТЛИЧНО: {success_rate:.1f}% успешных запросов")
        elif success_rate >= 90:
            print(f"⚠️  ХОРОШО: {success_rate:.1f}% успешных запросов (есть блокировки)")
        elif success_rate >= 70:
            print(f"⚠️  УДОВЛЕТВОРИТЕЛЬНО: {success_rate:.1f}% успешных запросов (много блокировок)")
        else:
            print(f"❌ ПЛОХО: {success_rate:.1f}% успешных запросов (слишком много блокировок)")
        
        print(f"{'='*70}\n")
        
        return result
    
    async def test_all_scenarios(
        self,
        scenarios: List[Tuple[float, float]],
        num_requests_per_scenario: int = 50,
        pause_between_scenarios: float = 30.0,
        seed: str = "ремонт пылесосов",
        country: str = "UA",
        language: str = "ru"
    ) -> List[Dict]:
        """
        Тестируем все сценарии задержек
        
        Args:
            scenarios: список кортежей (min_delay, max_delay)
            num_requests_per_scenario: количество запросов на сценарий
            pause_between_scenarios: пауза между сценариями (чтобы "остыть")
            
        Returns:
            список результатов для каждого сценария
        """
        results = []
        
        print(f"\n{'#'*70}")
        print(f"🚀 ЗАПУСК ПОЛНОГО ТЕСТИРОВАНИЯ ЗАДЕРЖЕК")
        print(f"{'#'*70}")
        print(f"Сценариев: {len(scenarios)}")
        print(f"Запросов на сценарий: {num_requests_per_scenario}")
        print(f"Пауза между сценариями: {pause_between_scenarios} сек")
        print(f"Seed: '{seed}'")
        print(f"{'#'*70}\n")
        
        for i, (min_delay, max_delay) in enumerate(scenarios):
            print(f"\n{'▼'*70}")
            print(f"СЦЕНАРИЙ {i+1}/{len(scenarios)}")
            print(f"{'▼'*70}")
            
            # Тестируем сценарий
            result = await self.test_delay_range(
                min_delay=min_delay,
                max_delay=max_delay,
                num_requests=num_requests_per_scenario,
                seed=seed,
                country=country,
                language=language,
                verbose=False
            )
            
            results.append(result)
            
            # Пауза между сценариями (кроме последнего)
            if i < len(scenarios) - 1:
                print(f"\n⏸️  Пауза {pause_between_scenarios} сек перед следующим сценарием...")
                await asyncio.sleep(pause_between_scenarios)
        
        # Итоговая таблица
        self.print_comparison_table(results)
        
        # Сохраняем результаты в JSON файл
        self.save_results_to_file(results)
        
        return results
    
    def save_results_to_file(self, results: List[Dict]):
        """Сохранение результатов в JSON файл"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"delay_test_results_{timestamp}.json"
        
        # Формируем данные для сохранения
        output = {
            "test_timestamp": datetime.now().isoformat(),
            "test_summary": {
                "total_scenarios": len(results),
                "total_requests": sum(r['num_requests'] for r in results),
                "total_time": sum(r['total_time'] for r in results)
            },
            "scenarios": results,
            "recommendation": self.get_recommendation(results)
        }
        
        # Сохраняем в файл
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Результаты сохранены в файл: {filename}")
    
    def get_recommendation(self, results: List[Dict]) -> Dict:
        """Получить рекомендацию на основе результатов"""
        # Находим самый быстрый сценарий с success_rate >= 95%
        safe_results = [r for r in results if r['success_rate'] >= 95]
        
        if safe_results:
            fastest = min(safe_results, key=lambda x: x['total_time'])
            return {
                "optimal_delay_range": fastest['delay_range'],
                "success_rate": fastest['success_rate'],
                "total_time": fastest['total_time'],
                "avg_results_per_request": fastest['avg_results_per_request'],
                "status": "found"
            }
        else:
            # Если нет безопасных - берём лучший по success_rate
            best = max(results, key=lambda x: x['success_rate'])
            return {
                "optimal_delay_range": best['delay_range'],
                "success_rate": best['success_rate'],
                "total_time": best['total_time'],
                "avg_results_per_request": best['avg_results_per_request'],
                "status": "no_safe_option_found"
            }
        
        return results
    
    def print_comparison_table(self, results: List[Dict]):
        """Вывод сравнительной таблицы всех сценариев"""
        print(f"\n{'#'*70}")
        print(f"📊 СРАВНИТЕЛЬНАЯ ТАБЛИЦА ВСЕХ СЦЕНАРИЕВ")
        print(f"{'#'*70}\n")
        
        print(f"{'Задержка':<15} {'Успех%':<10} {'Время':<10} {'Результаты':<12} {'Оценка':<15}")
        print(f"{'-'*70}")
        
        for r in results:
            delay_str = f"{r['delay_range'][0]}-{r['delay_range'][1]}s"
            success_str = f"{r['success_rate']}%"
            time_str = f"{r['total_time']}s"
            results_str = f"{r['avg_results_per_request']:.1f}/req"
            
            # Оценка
            if r['success_rate'] >= 98:
                rating = "✅ Отлично"
            elif r['success_rate'] >= 90:
                rating = "⚠️  Хорошо"
            elif r['success_rate'] >= 70:
                rating = "⚠️  Удовл."
            else:
                rating = "❌ Плохо"
            
            print(f"{delay_str:<15} {success_str:<10} {time_str:<10} {results_str:<12} {rating:<15}")
        
        print(f"\n{'#'*70}")
        
        # Рекомендация
        best = max(results, key=lambda x: x['success_rate'])
        fastest = min([r for r in results if r['success_rate'] >= 95], 
                     key=lambda x: x['total_time'], 
                     default=None)
        
        print(f"\n🏆 РЕКОМЕНДАЦИИ:")
        print(f"{'='*70}")
        
        if fastest:
            print(f"✅ ОПТИМАЛЬНЫЙ ДИАПАЗОН: {fastest['delay_range'][0]}-{fastest['delay_range'][1]} сек")
            print(f"   - Успех: {fastest['success_rate']}%")
            print(f"   - Время: {fastest['total_time']} сек")
            print(f"   - Результаты: {fastest['avg_results_per_request']:.1f}/запрос")
        else:
            print(f"⚠️  НЕТ БЕЗОПАСНОГО БЫСТРОГО ДИАПАЗОНА")
            print(f"   Лучший результат: {best['delay_range'][0]}-{best['delay_range'][1]} сек ({best['success_rate']}%)")
        
        print(f"{'='*70}\n")


# ============================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================
async def main():
    """Запуск тестирования"""
    
    tester = DelayTester()
    
    # Сценарии для тестирования (от консервативного к агрессивному)
    # Цель: найти МИНИМАЛЬНУЮ безопасную задержку, постепенно уменьшая
    scenarios = [
        (0.5, 2.0),   # Текущий (очень консервативный) - НАЧИНАЕМ С ЭТОГО
        (0.5, 1.5),   # Консервативный
        (0.4, 1.0),   # Умеренный
        (0.3, 0.7),   # Умеренно агрессивный
        (0.2, 0.5),   # Агрессивный
        (0.1, 0.3),   # Очень агрессивный - ЗАКАНЧИВАЕМ ЭТИМ
    ]
    
    # Параметры теста
    num_requests = 50  # Запросов на сценарий
    pause = 30.0       # Пауза между сценариями (секунды)
    
    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║                  DELAY OPTIMIZATION TESTER                       ║
║          Поиск минимальной безопасной задержки                   ║
╚══════════════════════════════════════════════════════════════════╝

📋 Параметры теста:
   - Сценариев: {len(scenarios)}
   - Запросов на сценарий: {num_requests}
   - Пауза между сценариями: {pause} сек
   - Общее примерное время: {len(scenarios) * 2 + (len(scenarios)-1) * pause / 60:.0f} минут

⚙️  Тестируемые диапазоны задержек:
""")
    
    for i, (min_d, max_d) in enumerate(scenarios, 1):
        avg = (min_d + max_d) / 2
        print(f"   {i}. {min_d}-{max_d} сек (среднее: {avg:.2f} сек)")
    
    print(f"\n{'='*70}")
    input("Нажмите Enter для начала тестирования...")
    
    # Запускаем тестирование
    results = await tester.test_all_scenarios(
        scenarios=scenarios,
        num_requests_per_scenario=num_requests,
        pause_between_scenarios=pause,
        seed="ремонт пылесосов",
        country="UA",
        language="ru"
    )
    
    print(f"\n✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!")
    print(f"Результаты сохранены в переменной 'results'")


if __name__ == "__main__":
    asyncio.run(main())
