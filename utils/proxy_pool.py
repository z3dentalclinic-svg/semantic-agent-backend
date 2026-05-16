"""
Proxy Pool Manager v2.0 — жёсткая 2-batch архитектура для бета-теста.

Конфигурация: 50 IP всего → 2 батча по 25 IP.
Карусель батчей: при ротации текущий батч уходит в конец, следующий становится активным.

Роли внутри одного батча (25 IP):
  индексы 0-3   (4 IP) → suffix_boevoy_chrome  ← round-robin
  индексы 4-5   (2 IP) → suffix_boevoy_firefox ← round-robin
  индексы 6-7   (2 IP) → suffix_addon_chrome   ← round-robin
  индексы 8-9   (2 IP) → suffix_addon_firefox  ← round-robin
  индексы 10-19 (10 IP) → infix_research       ← round-robin (10 IP под единый research pool)
  индексы 20-22 (3 IP) → prefix_research (chrome bucket)  ← первые 3 IP из 5 prefix-research
  индексы 23-24 (2 IP) → prefix_research (firefox bucket) ← последние 2 IP из 5 prefix-research

Итого на батч:
  suffix: 10 (4 boevoy chr + 2 boevoy ff + 2 addon chr + 2 addon ff)
  infix:  10
  prefix: 5 (3 chr + 2 ff)
  ────────
  25

При 2 одновременных пользователях → 2 × 25 = 50 IP без конфликтов.

API:
  ProxyPool.get("suffix_boevoy_chrome")  → round-robin IP из своих 4 слотов
  ProxyPool.get("infix_research")        → round-robin IP из своих 10 слотов
  ProxyPool.get("prefix_research")       → round-robin IP из своих 5 слотов
  ProxyPool.rotate()                     → переключение на следующий батч
"""

import os
import time
import threading
import logging

logger = logging.getLogger(__name__)

# Размер батча
BATCH_SIZE = 25

# Карта ролей: role → (start_index, count)
# Каждая роль занимает контигуозный диапазон слотов внутри батча.
ROLE_SLOTS = {
    "suffix_boevoy_chrome":  (0, 4),    # слоты 0-3
    "suffix_boevoy_firefox": (4, 2),    # слоты 4-5
    "suffix_addon_chrome":   (6, 2),    # слоты 6-7
    "suffix_addon_firefox":  (8, 2),    # слоты 8-9
    "infix_research":        (10, 10),  # слоты 10-19
    "prefix_research":       (20, 5),   # слоты 20-24 (первые 3 chrome, последние 2 firefox)
}

# Алиасы для обратной совместимости со старым кодом.
# Старые роли "suffix"/"prefix_chrome" и т.д. мапятся на новые.
LEGACY_ALIASES = {
    "suffix":          "suffix_boevoy_chrome",   # старый suffix general → boevoy chrome
    "infix_chrome":    "infix_research",          # старый infix_chrome   → research pool
    "infix_firefox":   "infix_research",
    "infix_safari":    "infix_research",
    "prefix_chrome":   "prefix_research",
    "prefix_firefox":  "prefix_research",
    "prefix_nonpa":    "prefix_research",
}


class ProxyPool:
    _lock = threading.Lock()
    _proxies: list = []                 # все IP из env (плоский список)
    _batches: list = []                  # батчи по BATCH_SIZE
    _current_batch: int = 0              # индекс активного батча
    _last_rotation: float = 0            # время последней ротации
    _role_counters: dict = {}            # role → round-robin counter внутри роли
    _initialized: bool = False

    MIN_ROTATION_INTERVAL: float = 7.0  # минимум между ротациями

    @classmethod
    def _init(cls):
        if cls._initialized:
            return
        raw = os.getenv("PROXY_POOL", "")
        if not raw:
            logger.warning("[ProxyPool v2] PROXY_POOL не задан — прокси отключены")
            cls._proxies = []
            cls._batches = []
            cls._initialized = True
            return

        cls._proxies = [p.strip() for p in raw.split(",") if p.strip()]
        cls._batches = [
            cls._proxies[i:i + BATCH_SIZE]
            for i in range(0, len(cls._proxies), BATCH_SIZE)
        ]
        # Инициализируем счётчики
        for role in ROLE_SLOTS:
            cls._role_counters[role] = 0

        logger.info(
            f"[ProxyPool v2] Загружено {len(cls._proxies)} IP "
            f"→ {len(cls._batches)} батчей по {BATCH_SIZE}"
        )
        if len(cls._proxies) < BATCH_SIZE:
            logger.warning(
                f"[ProxyPool v2] Внимание: всего {len(cls._proxies)} IP < BATCH_SIZE={BATCH_SIZE}. "
                f"Некоторые роли получат меньше IP чем ожидается."
            )
        cls._initialized = True

    @classmethod
    def get(cls, role: str) -> str | None:
        """
        Возвращает proxy URL для указанной роли из текущего активного батча.
        Внутри роли — round-robin по её слотам.

        Принимает как новые роли (suffix_boevoy_chrome и т.п.), так и старые
        алиасы для совместимости (suffix, infix_chrome и т.п.).
        """
        with cls._lock:
            cls._init()
            if not cls._batches:
                return None

            # Резолвим алиас
            resolved_role = LEGACY_ALIASES.get(role, role)

            if resolved_role not in ROLE_SLOTS:
                logger.warning(f"[ProxyPool v2] Неизвестная роль: {role!r}")
                return None

            start, count = ROLE_SLOTS[resolved_role]
            batch = cls._batches[cls._current_batch]

            # Извлекаем слоты роли из батча
            role_slots = batch[start:start + count]
            if not role_slots:
                logger.warning(
                    f"[ProxyPool v2] Роль {resolved_role!r} требует слоты {start}-{start+count-1}, "
                    f"но в активном батче только {len(batch)} IP"
                )
                return None

            # Round-robin внутри роли
            counter = cls._role_counters.setdefault(resolved_role, 0)
            ip = role_slots[counter % len(role_slots)]
            cls._role_counters[resolved_role] = counter + 1
            return ip

    @classmethod
    def get_all_proxies(cls) -> list:
        """Возвращает копию всего списка IP (без привязки к батчам)."""
        with cls._lock:
            cls._init()
            return list(cls._proxies)

    @classmethod
    def get_research_pool(cls, exclude_roles: set | None = None) -> list:
        """
        Возвращает все IP пула МИНУС те что закреплены за указанными ролями
        в АКТИВНОМ батче. Для обратной совместимости.

        В новой архитектуре V2 предпочтительно использовать ProxyPool.get("infix_research")
        и ProxyPool.get("prefix_research") вместо этого метода.
        """
        with cls._lock:
            cls._init()
            if not cls._proxies:
                return []

            excluded_ips: set = set()
            if exclude_roles and cls._batches:
                current = cls._batches[cls._current_batch]
                for role in exclude_roles:
                    resolved = LEGACY_ALIASES.get(role, role)
                    if resolved in ROLE_SLOTS:
                        start, count = ROLE_SLOTS[resolved]
                        for ip in current[start:start + count]:
                            excluded_ips.add(ip)

            return [ip for ip in cls._proxies if ip not in excluded_ips]

    @classmethod
    def rotate(cls):
        """
        Ротирует батч — текущий уходит в конец очереди.
        Защита MIN_ROTATION_INTERVAL предотвращает тройной вызов
        при параллельном запуске suffix+prefix+infix.
        """
        with cls._lock:
            cls._init()
            if not cls._batches:
                return

            now = time.time()
            if now - cls._last_rotation < cls.MIN_ROTATION_INTERVAL:
                logger.debug(
                    f"[ProxyPool v2] rotate() пропущен — "
                    f"прошло {now - cls._last_rotation:.1f}с < {cls.MIN_ROTATION_INTERVAL}с"
                )
                return

            prev = cls._current_batch
            cls._current_batch = (cls._current_batch + 1) % len(cls._batches)
            cls._last_rotation = now
            # Сбрасываем счётчики ролей для нового батча
            for role in cls._role_counters:
                cls._role_counters[role] = 0
            logger.info(
                f"[ProxyPool v2] Ротация: батч {prev} → {cls._current_batch} "
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
            role_view = {}
            for role, (start, count) in ROLE_SLOTS.items():
                role_view[role] = {
                    "slots": f"{start}-{start+count-1}",
                    "ips": batch[start:start + count] if start < len(batch) else [],
                    "n_ips": len(batch[start:start + count]) if start < len(batch) else 0,
                    "counter": cls._role_counters.get(role, 0),
                }

            return {
                "status": "active",
                "version": "2.0",
                "total_proxies": len(cls._proxies),
                "total_batches": len(cls._batches),
                "batch_size": BATCH_SIZE,
                "current_batch": cls._current_batch,
                "current_batch_size": len(batch),
                "roles": role_view,
                "last_rotation": cls._last_rotation,
            }
