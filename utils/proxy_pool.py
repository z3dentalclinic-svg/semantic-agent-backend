"""
Proxy Pool Manager v1.2

Управляет пулом IP-адресов для всех парсеров.
Конфигурация через env переменную PROXY_POOL (список через запятую).

Роли внутри батча (10 IP):
  index 0 → infix_chrome
  index 1 → infix_firefox
  index 2 → infix_safari    ← новая роль (v1.2)
  index 3 → prefix_chrome
  index 4 → prefix_firefox
  index 5 → prefix_nonpa
  index 6-9 → suffix/morph (общий пул, 4 IP)

Карусель батчей сделана для НЕПРЕРЫВНОГО парсинга (несколько пользователей).
В режиме research работает один пользователь, и инфикс может забрать ВСЕ IP
из всех батчей минус те что заняты собственной боевой матрицей.

Research-роли (round-robin по ВСЕМУ пулу):
  prefix_research → старая PD/PDL prefix-research сессия
  infix_research  → новая E_LAT/SD/SDL/SDL_REV infix-research (v1.2)

Метод get_research_pool(exclude_roles) — возвращает все IP минус занятые
указанными ролями активного батча. Используется чтобы research-прогон
не пересекался по IP со своей боевой матрицей (~47 IP вместо 50 при пуле в 5 батчей).

Использование в парсере:
  from proxy_pool import ProxyPool
  proxy   = ProxyPool.get("infix_chrome")             # → IP из активного батча
  proxy   = ProxyPool.get("infix_safari")             # → IP из активного батча (v1.2)
  ips     = ProxyPool.get_research_pool(              # → все IP минус боевые инфикс-слоты
              exclude_roles={"infix_chrome", "infix_firefox", "infix_safari"})
  ips_all = ProxyPool.get_all_proxies()               # → весь пул без исключений
"""

import os
import time
import threading
import logging

logger = logging.getLogger(__name__)

# Размер батча и роли
BATCH_SIZE = 10
ROLE_MAP = {
    "infix_chrome":   0,   # всегда первый IP в батче
    "infix_firefox":  1,   # всегда второй IP в батче
    "infix_safari":   2,   # всегда третий IP в батче (v1.2)
    "prefix_chrome":  3,   # PA Chrome — letter-parallel треки
    "prefix_firefox": 4,   # PA FF + G FF
    "prefix_nonpa":   5,   # G+PC Chrome (небольшой semaphore)
    # suffix/morph берут IP с индекса 6 по 9 по round-robin
}
GENERAL_ROLES = {"suffix", "morph"}
GENERAL_START = 6   # IP с этого индекса идут на общие парсеры (4 IP: 6,7,8,9)

# Research роли — для разовых широких прогонов в режиме одного пользователя.
# Берут IP из ВСЕГО пула (все батчи), round-robin.
# Не привязаны к активному батчу — research-прогон нагружает 30-50 IP параллельно,
# и карусель батчей здесь не нужна.
RESEARCH_ROLES = {"prefix_research", "infix_research", "suffix_addon_chrome", "suffix_addon_firefox"}


class ProxyPool:
    _lock = threading.Lock()
    _proxies: list = []          # все IP из env
    _batches: list = []          # батчи по BATCH_SIZE
    _current_batch: int = 0      # индекс активного батча
    _last_rotation: float = 0    # время последней ротации
    _general_counter: int = 0    # round-robin счётчик для suffix/prefix
    _research_counter: int = 0   # round-robin счётчик для research (по всему пулу)
    _initialized: bool = False

    @classmethod
    def _init(cls):
        if cls._initialized:
            return
        raw = os.getenv("PROXY_POOL", "")
        if not raw:
            logger.warning("[ProxyPool] PROXY_POOL не задан — прокси отключены")
            cls._proxies = []
            cls._batches = []
            cls._initialized = True
            return

        cls._proxies = [p.strip() for p in raw.split(",") if p.strip()]
        # Нарезаем на батчи по BATCH_SIZE
        cls._batches = [
            cls._proxies[i:i + BATCH_SIZE]
            for i in range(0, len(cls._proxies), BATCH_SIZE)
        ]
        logger.info(
            f"[ProxyPool] Загружено {len(cls._proxies)} IP "
            f"→ {len(cls._batches)} батчей по {BATCH_SIZE}"
        )
        cls._initialized = True

    @classmethod
    def get(cls, role: str) -> str | None:
        """
        Возвращает proxy URL для указанной роли из текущего активного батча.
        Если пул не настроен — возвращает None (без прокси).
        """
        with cls._lock:
            cls._init()

            if not cls._batches:
                return None

            batch = cls._batches[cls._current_batch]

            # Фиксированные роли
            if role in ROLE_MAP:
                idx = ROLE_MAP[role]
                if idx < len(batch):
                    return batch[idx]
                return None

            # Общие роли (suffix/prefix/morph) — round-robin по IP 2..9
            if role in GENERAL_ROLES:
                general_ips = batch[GENERAL_START:]
                if not general_ips:
                    return None
                ip = general_ips[cls._general_counter % len(general_ips)]
                cls._general_counter += 1
                return ip

            # Research роль — round-robin по ВСЕМУ пулу (все IP, все батчи).
            # Используется для разовых прогонов PD/PDL prefix research.
            # Игнорирует карусель батчей — нагрузка распределяется по всему пулу.
            if role in RESEARCH_ROLES:
                if not cls._proxies:
                    return None
                ip = cls._proxies[cls._research_counter % len(cls._proxies)]
                cls._research_counter += 1
                return ip

            logger.warning(f"[ProxyPool] Неизвестная роль: {role}")
            return None

    @classmethod
    def get_all_proxies(cls) -> list:
        """
        Возвращает копию всего списка IP (без привязки к батчам).
        Используется research-парсером для создания пула httpx-клиентов.
        """
        with cls._lock:
            cls._init()
            return list(cls._proxies)

    @classmethod
    def get_research_pool(cls, exclude_roles: set | None = None) -> list:
        """
        Возвращает все IP пула МИНУС те что закреплены за указанными ролями
        в АКТИВНОМ батче. Используется research-прогонами при параллельном
        запуске всех парсеров (combined_parser.html запускает suffix+prefix+infix
        одновременно через Promise.all).

        exclude_roles может содержать:
          - Фиксированные роли из ROLE_MAP (infix_chrome, prefix_nonpa и т.д.)
            → исключается соответствующий IP активного батча
          - Общие роли (suffix, morph) → исключается весь general_pool
            активного батча (4 IP с индекса GENERAL_START)

        Пример (5 батчей × 10 IP = 50 IP, combined_parser параллельный режим):
          get_research_pool(exclude_roles={
              "infix_chrome", "infix_firefox", "infix_safari",       # 3 IP — свои боевые
              "prefix_chrome", "prefix_firefox", "prefix_nonpa",     # 3 IP — префикс
              "suffix",                                                # 4 IP — суффикс/morph (general)
          })
          → 50 - 3 - 3 - 4 = 40 IP под research

        Если exclude_roles не задан — поведение идентично get_all_proxies().
        После rotate() исключения автоматически сдвигаются на новый активный батч.
        """
        with cls._lock:
            cls._init()
            if not cls._proxies:
                return []

            excluded_ips: set = set()
            if exclude_roles and cls._batches:
                current = cls._batches[cls._current_batch]
                for role in exclude_roles:
                    idx = ROLE_MAP.get(role)
                    if idx is not None and idx < len(current):
                        excluded_ips.add(current[idx])
                    elif role in GENERAL_ROLES:
                        # Исключаем весь general pool активного батча
                        for ip in current[GENERAL_START:]:
                            excluded_ips.add(ip)

            return [ip for ip in cls._proxies if ip not in excluded_ips]

    # Минимальный интервал между ротациями — защита от тройного вызова
    # при параллельном запуске suffix+prefix+infix.
    MIN_ROTATION_INTERVAL: float = 7.0

    @classmethod
    def rotate(cls):
        """
        Ротирует батч — текущий уходит в конец очереди.
        Вызывается после завершения каждого полного прогона сида.

        Защита: при параллельном запуске suffix+prefix+infix все три
        вызывают rotate() почти одновременно. MIN_ROTATION_INTERVAL
        гарантирует что реальная ротация происходит только один раз
        на весь комбинированный прогон (~4-6с).
        """
        with cls._lock:
            cls._init()
            if not cls._batches:
                return

            now = time.time()
            if now - cls._last_rotation < cls.MIN_ROTATION_INTERVAL:
                logger.debug(
                    f"[ProxyPool] rotate() пропущен — "
                    f"прошло {now - cls._last_rotation:.1f}с < {cls.MIN_ROTATION_INTERVAL}с"
                )
                return

            prev = cls._current_batch
            cls._current_batch = (cls._current_batch + 1) % len(cls._batches)
            cls._last_rotation = now
            cls._general_counter = 0
            cls._research_counter = 0
            logger.info(
                f"[ProxyPool] Ротация: батч {prev} → {cls._current_batch} "
                f"(всего {len(cls._batches)} батчей)"
            )

    @classmethod
    def status(cls) -> dict:
        """Возвращает текущее состояние пула для диагностики."""
        with cls._lock:
            cls._init()
            if not cls._batches:
                return {"status": "disabled", "reason": "PROXY_POOL не задан"}

            batch = cls._batches[cls._current_batch]
            return {
                "status": "active",
                "total_proxies": len(cls._proxies),
                "total_batches": len(cls._batches),
                "batch_size": BATCH_SIZE,
                "current_batch": cls._current_batch,
                "current_batch_size": len(batch),
                "roles": {
                    "infix_chrome":   batch[0] if len(batch) > 0 else None,
                    "infix_firefox":  batch[1] if len(batch) > 1 else None,
                    "infix_safari":   batch[2] if len(batch) > 2 else None,
                    "prefix_chrome":  batch[3] if len(batch) > 3 else None,
                    "prefix_firefox": batch[4] if len(batch) > 4 else None,
                    "prefix_nonpa":   batch[5] if len(batch) > 5 else None,
                    "general_pool":   f"{len(batch[GENERAL_START:])} IP (индексы {GENERAL_START}-{len(batch)-1})",
                },
                "last_rotation": cls._last_rotation,
                "cooldown_per_batch_sec": round(
                    (len(cls._batches) - 1) * 3, 1
                ),  # примерно при 3с на прогон
            }
