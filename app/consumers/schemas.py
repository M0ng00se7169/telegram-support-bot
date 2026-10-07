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


# Типы, которые consumer'у нужны без повторной проверки: граница процесса — единственное
# место, где типы могут быть ложными. После валидации `event_id` — `UUID`,
# `occurred_at` — `datetime`, `chat_oid` — `str`.
EventId = UUID
OccurredAt = datetime

__all__ = (
	'NewMessageEvent',
	'EventId',
	'OccurredAt',
	'ValidationError',
)
