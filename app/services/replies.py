"""Reply Registry: уведомитель ↔ reply-флоу (карточка №1).

Deep Module: скрывает протокол «tg_message_id → chat_oid» за двумя симметричными
операциями — `remember` (уведомитель, процесс consumer'а) и `resolve` (reply-флоу,
процесс бота). Процессы раздельные (ADR-0001), общей памяти между ними нет →
общее хранилище — Valkey (уже в стеке RouteRelay). TTL записи = окно на ответ.
"""

from abc import (
	ABC,
	abstractmethod,
)

from valkey.asyncio import Valkey

REPLY_REGISTRY_TTL = 60 * 60  # окно на ответ менеджера, секунд
REPLY_REGISTRY_PREFIX = 'reply-registry:'


class BaseReplyRegistry(ABC):
	@abstractmethod
	async def remember(self, message_id: int, chat_oid: str) -> None: ...

	@abstractmethod
	async def resolve(self, message_id: int) -> str | None: ...

	@abstractmethod
	async def forget(self, message_id: int) -> None: ...


class ValkeyReplyRegistry(BaseReplyRegistry):
	"""Valkey backend: SET key value EX ttl, GET, DEL."""

	def __init__(self, client: Valkey) -> None:
		self._client = client

	async def remember(self, message_id: int, chat_oid: str) -> None:
		await self._client.set(
			self._key(message_id),
			chat_oid,
			ex=REPLY_REGISTRY_TTL,
		)

	async def resolve(self, message_id: int) -> str | None:
		data = await self._client.get(self._key(message_id))
		if data is None:
			return None
		return data.decode() if isinstance(data, bytes) else str(data)

	async def forget(self, message_id: int) -> None:
		await self._client.delete(self._key(message_id))

	def _key(self, message_id: int) -> str:
		return REPLY_REGISTRY_PREFIX + str(message_id)


class InMemoryReplyRegistry(BaseReplyRegistry):
	"""In-process backend для офлайн-смоука (один общий event-loop)."""

	def __init__(self) -> None:
		self._data: dict[int, str] = {}

	async def remember(self, message_id: int, chat_oid: str) -> None:
		self._data[message_id] = chat_oid

	async def resolve(self, message_id: int) -> str | None:
		return self._data.get(message_id)

	async def forget(self, message_id: int) -> None:
		self._data.pop(message_id, None)
