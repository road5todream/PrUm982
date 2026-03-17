"""
Высокоуровневый пакет для работы с приёмником UM982.

Содержит:
- базовый класс работы с портом и обменом данными (`core`);
- утилиты и модели парсинга (`utils`, `models`);
- доменные модули для отдельных наборов команд (`config`, `mask`, `data_output`, `system`, `mode`).

Фасадный класс `UM982UART` остаётся в модуле верхнего уровня `um982_uart.py`
для обратной совместимости и использует эти модули под капотом.
"""

from .core import Um982Core
from . import models as models
from . import utils as utils

__all__ = [
    "Um982Core",
    "models",
    "utils",
]

