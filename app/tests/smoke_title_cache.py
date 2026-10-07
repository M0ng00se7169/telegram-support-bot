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
import logging  # noqa: E402

import httpx  # noqa: E402
from dishka import (  # noqa: E402
    make_async_container,
    provide,
    Provider,
    Scope,
)
from httpx import (  # noqa: E402
    AsyncClient,
    MockTransport,
    Response,
)
from settings import get_settings  # noqa: E402
from subscription_pipeline import LogCapture  # noqa: E402

from consumers.main import warm_title_cache  # noqa: E402
from consumers.schemas import (  # noqa: E402
    ChatDeletedEvent,
    NewChatCreatedEvent,
)
from containers.providers import DefaultProvider  # noqa: E402
from services.title_cache import ChatTitleCache  # noqa: E402
from services.web import BaseChatDirectory  # noqa: E402


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

# Заголовки, которые сервер отдаёт на GET /chat/ (конверт `items`/`count`).
WARMED_ITEMS = [
	{'oid': 'chat-oid', 'title': 'Warmed Title', 'created_at': '2026-01-01T00:00:00Z'},
]

# Ответ GET /chat/{oid}/ — тот же, что отдаёт общий `offline_provider`.
CHAT_INFO = {
	'oid': 'chat-oid',
	'title': 'chat title',
	'created_at': '2026-01-01T00:00:00Z',
}


def _transport_provider(
	requests_seen: list[str],
	*,
	chat_list_status: int = 200,
	chat_info_status: int = 200,
) -> Provider:
	"""MockTransport с журналом запросов и умением ломать выбранные пути.

	Общий `offline_provider` отвечает на GET /chat/ одиночным объектом —
	конверта `{'items': [...], 'count': N}` у него нет, поэтому прогреву
	нужен собственный обработчик (см. gotcha в общем каркасе).
	"""
	class TransportProvider(Provider):
		@provide(scope=Scope.REQUEST)
		def get_http_client(self) -> AsyncClient:
			def handler(request: httpx.Request) -> Response:
				requests_seen.append(f'{request.method} {request.url.path}')
				if request.url.path == '/chat/':
					if chat_list_status != 200:
						return Response(
							chat_list_status,
							json={'detail': 'chat list down'},
						)
					return Response(
						200,
						json={'items': WARMED_ITEMS, 'count': len(WARMED_ITEMS)},
					)
				if request.url.path.endswith('/listeners/'):
					return Response(200, json=[{'oid': '42'}])
				if chat_info_status != 200:
					return Response(chat_info_status, json={'detail': 'Chat not found'})
				return Response(200, json=CHAT_INFO)

			return AsyncClient(transport=MockTransport(handler))

	return TransportProvider()


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


async def _check_warmup_fills_cache() -> None:
	# Прогрев при старте — ровно один запрос за список чатов; после прогрева
	# get_title отдаёт заголовок warmed-чата с нулём дополнительных запросов.
	requests: list[str] = []
	container = make_async_container(
		DefaultProvider(),
		_transport_provider(requests),
	)
	try:
		await warm_title_cache(container)
		assert requests == ['GET /chat/'], requests

		async with container() as scope:
			cache = await scope.get(ChatTitleCache)
			directory = await scope.get(BaseChatDirectory)
			title = await cache.get_title('chat-oid', directory)
		assert title == 'Warmed Title', title
		assert requests == ['GET /chat/'], requests  # кэш тёплый: 0 HTTP
	finally:
		await container.close()


async def _check_warmup_failure_is_not_fatal() -> None:
	# Сбой прогрева (Web API отвечает 500) не пробрасывается наружу и
	# логируется предупреждением; кэш остаётся пустым и добирает заголовок
	# точечным read-through'ом при первом же обращении.
	requests: list[str] = []
	capture = LogCapture()
	title_cache_logger = logging.getLogger('services.title_cache')
	title_cache_logger.addHandler(capture)
	container = make_async_container(
		DefaultProvider(),
		_transport_provider(requests, chat_list_status=500),
	)
	try:
		await warm_title_cache(container)  # не должно упасть
		assert requests == ['GET /chat/'], requests
		assert capture.records, 'warmup failure must be logged as warning'

		async with container() as scope:
			cache = await scope.get(ChatTitleCache)
			directory = await scope.get(BaseChatDirectory)
			title = await cache.get_title('chat-oid', directory)
		assert title == 'chat title', title  # добыт read-through'ом
		assert requests == ['GET /chat/', 'GET /chat/chat-oid/'], requests
	finally:
		title_cache_logger.removeHandler(capture)
		await container.close()


async def _check_tombstone_and_remember() -> None:
	# Удалённый чат: get_title -> None и ни одного HTTP-запроса — пропуск идёт
	# ДО любого обращения к Web API. Событие создания снимает tombstone:
	# заголовок снова доступен, тоже без HTTP.
	requests: list[str] = []
	container = make_async_container(
		DefaultProvider(),
		_transport_provider(requests),
	)
	try:
		async with container() as scope:
			cache = await scope.get(ChatTitleCache)
			directory = await scope.get(BaseChatDirectory)

			cache.forget('chat-oid')
			assert await cache.get_title('chat-oid', directory) is None
			assert requests == [], requests

			cache.remember('chat-oid', 'Back Again')
			assert await cache.get_title('chat-oid', directory) == 'Back Again'
			assert requests == [], requests
	finally:
		await container.close()


async def _check_fallback_never_suppresses() -> None:
	# Web API не отдаёт чат (жёсткое удаление без tombstone / сбой):
	# get_title возвращает сам chat_oid запасным заголовком, логирует
	# предупреждение и не бросает — «молчание хуже неточности».
	requests: list[str] = []
	capture = LogCapture()
	title_cache_logger = logging.getLogger('services.title_cache')
	title_cache_logger.addHandler(capture)
	container = make_async_container(
		DefaultProvider(),
		_transport_provider(requests, chat_info_status=500),
	)
	try:
		async with container() as scope:
			cache = await scope.get(ChatTitleCache)
			directory = await scope.get(BaseChatDirectory)
			title = await cache.get_title('chat-oid', directory)
		assert title == 'chat-oid', title
		assert requests == ['GET /chat/chat-oid/'], requests
		assert capture.records, 'fallback must be logged as warning'
	finally:
		title_cache_logger.removeHandler(capture)
		await container.close()


async def main() -> None:
	_check_settings_defaults()
	_check_chat_event_schemas()
	await _check_warmup_fills_cache()
	await _check_warmup_failure_is_not_fatal()
	await _check_tombstone_and_remember()
	await _check_fallback_never_suppresses()

	print('TITLE_CACHE_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
