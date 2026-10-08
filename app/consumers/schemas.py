"""Схема New-Message Event: точное зеркало серверного `BaseEvent` (карточка №5).

Серверный `BaseEvent` — stdlib `@dataclass` с `event_id: UUID`, `occurred_at: datetime`
(оба `kw_only`) и `event_title: ClassVar[str]`. `OPT_SERIALIZE_DATACLASS` выбрасывает
`ClassVar` из JSON, поэтому на проводе ровно пять полей: `event_id`, `occurred_at`,
`message_text`, `message_oid`, `chat_oid`.

Клиентская схема воспроизводит эти пять полей с настоящими типами: именование —
«зеркало», поле в поле, чтобы переименование на сервере ломало клиент явно, а не молча.
Поле `event_title` не добавляется: на проводе его нет, отсутствие — норма.
Неизвестные поля разрешены: обогащение события на стороне сервера (в том числе
добавление `title` в payload) должно быть неломающим изменением для уже развёрнутого
consumer'а. Обязательные поля — строго: событие без них считается битым.

Те же правила воспроизводят зеркала событий жизненного цикла чата:
`NewChatCreatedEvent` (топик `new-chats-topic`, поле заголовка — `chat_title`,
как на сервере) и `ChatDeletedEvent` (топик `chat-deleted-topic`, без заголовка).
Типы, обязательность и терпимость к неизвестным полям — как у New-Message Event;
наивное `occurred_at` описано в `NewMessageEvent`.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
)


class NewMessageEvent(BaseModel):
	"""Зеркало серверного `BaseEvent`: пять полей, настоящие типы, неизвестные поля разрешены."""

	model_config = ConfigDict(extra='allow')

	event_id: UUID
	# `occurred_at` — наивное локальное время сервера без зоны (см. спеку): серверный
	# `BaseEvent` держит `datetime` без tzinfo, а `OPT_SERIALIZE_DATACLASS` пишет его
	# без суффикса зоны. Значение **не** интерпретируется как UTC, не используется для
	# вычислений и не должно выводиться как «время получения» без явного пояснения.
	# Схема принимает любой разбираемый `datetime` и отвергает только неразбираемый:
	# жёсткая проверка «только naive» отбросила бы все события, если сервер когда-нибудь
	# станет сериализовать время с зоной, — а это отказ доставки, а не диагностика.
	occurred_at: datetime
	message_text: str
	message_oid: str
	chat_oid: str


class NewChatCreatedEvent(BaseModel):
	"""Зеркало серверного `NewChatCreatedEvent`: единственный серверный источник заголовка.

	На проводе: `event_id`, `occurred_at`, `chat_oid`, `chat_title` — поле
	заголовка называется `chat_title` именно так (зеркало, не «title»).
	`occurred_at` — наивное локальное время сервера без зоны: см. комментарий
	в `NewMessageEvent`, правила те же.
	"""

	model_config = ConfigDict(extra='allow')

	event_id: UUID
	occurred_at: datetime
	chat_oid: str
	chat_title: str


class ChatDeletedEvent(BaseModel):
	"""Зеркало серверного `ChatDeletedEvent`: удаление чата, заголовка в payload'е нет."""

	model_config = ConfigDict(extra='allow')

	event_id: UUID
	occurred_at: datetime
	chat_oid: str


# Типы, которые consumer'у нужны без повторной проверки: граница процесса — единственное
# место, где типы могут быть ложными. После валидации `event_id` — `UUID`,
# `occurred_at` — `datetime`, `chat_oid` — `str`.
EventId = UUID
OccurredAt = datetime

__all__ = (
	'NewMessageEvent',
	'NewChatCreatedEvent',
	'ChatDeletedEvent',
	'EventId',
	'OccurredAt',
	'ValidationError',
)
