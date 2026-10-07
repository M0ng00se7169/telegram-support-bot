"""Счётчики конвейера потребления New-Message Event (карточка №5).

Наблюдаемость: сколько событий отсеяно как битые (билет 01) и сколько как дубликаты
(билет 02). Счётчики живут в процессе consumer'а; для дашборда их достаточно, а
смоуки могут читать и сбрасывать их напрямую.
"""

from dataclasses import dataclass


@dataclass
class EventMetrics:
	broken_events: int = 0
	duplicate_events: int = 0

	def record_broken(self) -> None:
		self.broken_events += 1

	def record_duplicate(self) -> None:
		self.duplicate_events += 1

	def reset(self) -> None:
		self.broken_events = 0
		self.duplicate_events = 0


metrics = EventMetrics()

__all__ = ('EventMetrics', 'metrics')
