"""
Proxy Pool Manager v1.0

Управляет пулом IP-адресов для всех парсеров.
Конфигурация через env переменную PROXY_POOL (список через запятую).

Роли внутри батча (10 IP):
  index 0 → infix_chrome
  index 1 → infix_firefox
  index 2-9 → suffix/prefix (общий пул)

Карусель батчей:
  - Один батч активен, остальные остывают
  - После каждого прогона батч уходит в конец очереди
  - Минимальное время остывания = (кол-во батчей - 1) × время_прогона

Использование в парсере:
  from proxy_pool import ProxyPool
  proxy = ProxyPool.get("infix_chrome")   # → "http://user:pass@ip:port" или None
  proxy = ProxyPool.get("infix_firefox")
  proxy = ProxyPool.get("suffix")
"""

import os
import time
import threading
import logging

logger = logging.getLogger(__name__)

# Размер батча и роли
BATCH_SIZE = 10
ROLE_MAP = {
    "infix_chrome":  0,   # всегда первый IP в батче
    "infix_firefox": 1,   # всегда второй IP в батче
    # suffix/prefix берут IP с индекса 2 по 9 по round-robin
}
GENERAL_ROLES = {"suffix", "prefix", "morph"}
GENERAL_START = 2   # IP с этого индекса идут на общие парсеры


class ProxyPool:
    _lock = threading.Lock()
    _proxies: list = []          # все IP из env
    _batches: list = []          # батчи по BATCH_SIZE
    _current_batch: int = 0      # индекс активного батча
    _last_rotation: float = 0    # время последней ротации
    _general_counter: int = 0    # round-robin счётчик для suffix/prefix
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

            logger.warning(f"[ProxyPool] Неизвестная роль: {role}")
            return None

    @classmethod
    def rotate(cls):
        """
        Ротирует батч — текущий уходит в конец очереди.
        Вызывается после завершения каждого полного прогона сида.
        """
        with cls._lock:
            cls._init()
            if not cls._batches:
                return
            prev = cls._current_batch
            cls._current_batch = (cls._current_batch + 1) % len(cls._batches)
            cls._last_rotation = time.time()
            cls._general_counter = 0
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
                    "infix_chrome":  batch[0] if len(batch) > 0 else None,
                    "infix_firefox": batch[1] if len(batch) > 1 else None,
                    "general_pool":  f"{len(batch[GENERAL_START:])} IP (индексы {GENERAL_START}-{len(batch)-1})",
                },
                "last_rotation": cls._last_rotation,
                "cooldown_per_batch_sec": round(
                    (len(cls._batches) - 1) * 3, 1
                ),  # примерно при 3с на прогон
            }
