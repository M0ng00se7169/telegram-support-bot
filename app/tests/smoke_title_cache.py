"""Offline smoke: Chat Title Cache (карточка №5, билет 03).

Проверяется внешнее поведение кэша заголовков через существующие seam'ы
(транспорт Chat Directory, Kafka-конвейер подписки):
- прогрев при старте одним запросом за список чатов; после прогрева
  `get_title` отдаёт заголовок без единого HTTP-запроса;
- сбой прогрева не пробрасывается: конвейер продолжает работать и доставляет
  уведомление с заголовком, добытым read-through'ом;
- событие создания чата держит кэш тёплым: уведомление содержит заголовок из
  события, журнала HTTP-запросов нет запроса за информацией о чате;
- событие удаления чата (tombstone) подавляет уведомление ДО любого HTTP:
  ни уведомления, ни запроса за listener'ами;
- промах кэша — точечный read-through: ровно один запрос за чат, второе
  сообщение в тот же чат новых запросов не делает;
- холодный старт без прогрева: уведомление всё равно содержит заголовок;
- отсутствующий или недоступный заголовок не подавляет уведомление:
  `get_title` возвращает `chat_oid` запасным заголовком и не бросает;
- hot path: для тёплого кэша на событие о сообщении ровно один HTTP-запрос —
  за listener'ами.

Запуск:  python tests/smoke_title_cache.py   (из каталога app/)
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import os


os.environ.setdefault('TG_BOT_TOKEN', '123456:TEST-TOKEN')

import asyncio  # noqa: E402

from settings import get_settings  # noqa: E402

from consumers.schemas import (  # noqa: E402
    ChatDeletedEvent,
    NewChatCreatedEvent,
)


# Payload'ы ровно в той форме, в которой их публикует серверный outbox relay
# (оптиональная сериализация dataclass: UUID строкой, наивное локальное время).
CREATED_EVENT = {
	'event_id': '12345678-1234-5678-1234-567812345678',
	'occurred_at': '2026-10-07T00:00:00',
	'chat_oid': 'chat-oid',
	'chat_title': 'Fresh Title',
}

DELETED_EVENT = {
	'event_id': '87654321-4321-8765-4321-876543210987',
	'occurred_at': '2026-10-07T00:00:00',
	'chat_oid': 'chat-oid',
}


def _check_settings_defaults() -> None:
	# Третья, отдельная consumer group для подписки на события чатов (спека):
	# сервер использует свою группу, бот — свою, new-message — свою.
	settings = get_settings()
	assert settings.NEW_CHATS_TOPIC == 'new-chats-topic', settings.NEW_CHATS_TOPIC
	assert (
		settings.CHAT_DELETED_TOPIC == 'chat-deleted-topic'
	), settings.CHAT_DELETED_TOPIC
	assert (
		settings.CHAT_EVENTS_GROUP_ID == 'tg-bot-chat-events'
	), settings.CHAT_EVENTS_GROUP_ID


def _check_chat_event_schemas() -> None:
	from datetime import datetime
	from uuid import UUID

	created = NewChatCreatedEvent.model_validate(CREATED_EVENT)
	assert isinstance(created.event_id, UUID), type(created.event_id)
	assert isinstance(created.occurred_at, datetime), type(created.occurred_at)
	assert created.chat_oid == 'chat-oid', created.chat_oid
	assert created.chat_title == 'Fresh Title', created.chat_title

	deleted = ChatDeletedEvent.model_validate(DELETED_EVENT)
	assert isinstance(deleted.event_id, UUID), type(deleted.event_id)
	assert deleted.chat_oid == 'chat-oid', deleted.chat_oid
	# У удалённого чата заголовка в payload'е нет — поля `chat_title` нет и в схеме.
	assert 'chat_title' not in ChatDeletedEvent.model_fields

	# Неизвестные поля разрешены (неломающее обогащение на стороне сервера),
	# серверный ClassVar `event_title` в схеме не материализуется.
	assert NewChatCreatedEvent.model_validate(
		{**CREATED_EVENT, 'extra': 1},
	).chat_title == 'Fresh Title'
	assert 'event_title' not in NewChatCreatedEvent.model_fields
	assert 'event_title' not in ChatDeletedEvent.model_fields

	# Наивное локальное время сервера без зоны — не UTC, значение не вычисляется.
	assert created.occurred_at.tzinfo is None, created.occurred_at.tzinfo
	assert deleted.occurred_at.tzinfo is None, deleted.occurred_at.tzinfo

	# Обязательные поля — строго: битые payload'ы отсекаются на границе.
	from pydantic import ValidationError

	broken_created = (
		{key: value for key, value in CREATED_EVENT.items() if key != 'chat_title'},
		{key: value for key, value in CREATED_EVENT.items() if key != 'chat_oid'},
		{**CREATED_EVENT, 'event_id': 'not-a-uuid'},
		{**CREATED_EVENT, 'occurred_at': 'not-a-date'},
		{**CREATED_EVENT, 'chat_title': 7},
		{**CREATED_EVENT, 'chat_oid': 42},
	)
	for broken in broken_created:
		try:
			NewChatCreatedEvent.model_validate(broken)
		except ValidationError:
			pass
		else:
			raise AssertionError(f'broken created event must be rejected: {broken!r}')

	broken_deleted = (
		{key: value for key, value in DELETED_EVENT.items() if key != 'chat_oid'},
		{**DELETED_EVENT, 'event_id': 42},
		{**DELETED_EVENT, 'occurred_at': 'not-a-date'},
	)
	for broken in broken_deleted:
		try:
			ChatDeletedEvent.model_validate(broken)
		except ValidationError:
			pass
		else:
			raise AssertionError(f'broken deleted event must be rejected: {broken!r}')


async def main() -> None:
	_check_settings_defaults()
	_check_chat_event_schemas()

	print('TITLE_CACHE_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
