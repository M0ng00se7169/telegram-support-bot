"""Event Deduplicator: идемпотентность подписки new-message (карточка №5).

Deep Module: скрывает протокол «пометить событие как виденное» за одной
**атомарной** операцией `mark_if_new` — признак «новое / уже видели» возвращается
тем же вызовом. Наивная пара «спросить → потом пометить» не принимается: два
события, обрабатываемых одновременно, прошли бы проверку до того, как отметки
друг друга появились.

Идентичность — `event_id` из payload события (ADR-0002), не Kafka Key.
Хранилище — Valkey: процессы раздельные (ADR-0001), общей памяти нет, а Valkey
уже в стеке RouteRelay. TTL задаётся настройкой `EVENT_DEDUP_TTL` и приходит
в бэкенд через DI; дефолт (сутки — окно перекрытия переотправок outbox) живёт
только в `settings`, в этом модуле он не дублируется.
"""

from abc import (
    ABC,
    abstractmethod,
)
from uuid import UUID

from valkey.asyncio import Valkey


EVENT_DEDUP_PREFIX = 'event-dedup:'


class BaseEventDeduplicator(ABC):
	"""Одна атомарная операция: пометить событие и сказать, было ли оно новым."""

	@abstractmethod
	async def mark_if_new(self, event_id: UUID) -> bool:
		"""True — событие новое, отметка поставлена именно этим вызовом.

		False — уже видели; проверка и пометка не разделяются на два вызова.
		"""
		...


class ValkeyEventDeduplicator(BaseEventDeduplicator):
	"""Valkey backend: один атомарный `SET key value EX ttl NX`."""

	def __init__(self, client: Valkey, ttl: int) -> None:
		self._client = client
		self._ttl = ttl

	@property
	def ttl(self) -> int:
		"""Время жизни отметки в секундах (приходит из настроек через DI)."""
		return self._ttl

	async def mark_if_new(self, event_id: UUID) -> bool:
		# `SET ... EX ... NX` выполняется атомарно на сервере: True — ключа не
		# было (событие новое), None — ключ уже стоял (дубликат). Признак
		# возвращается тем же вызовом, второго round-trip не требуется.
		result = await self._client.set(
			self._key(event_id),
			str(event_id),
			ex=self._ttl,
			nx=True,
		)
		return result is True

	def _key(self, event_id: UUID) -> str:
		return EVENT_DEDUP_PREFIX + str(event_id)


class InMemoryEventDeduplicator(BaseEventDeduplicator):
	"""In-process backend для офлайн-смоука (один общий event-loop)."""

	def __init__(self) -> None:
		self._seen: set[UUID] = set()

	async def mark_if_new(self, event_id: UUID) -> bool:
		# Read-modify-write — одна синхронная ступень на event-loop'е, без
		# await внутри: конкурентные вызовы сериализуются, ровно один True.
		if event_id in self._seen:
			return False
		self._seen.add(event_id)
		return True
