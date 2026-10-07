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
  `get_title` возвращает `chat_oid` запасным заголовком и не бросает при
  граничных ошибках Web API/парсинга;
- программный баг (не ошибка границы) внутри `warm_up`/`get_title`
  НЕ маскируется под фолбэк/предупреждение — исключение всплывает
  (ADR-0004), а хук `warm_title_cache` на старте — широкая страховка:
  ловит всё и продолжает подписку с отдельным сообщением в логе (п. 11);
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
import uuid  # noqa: E402

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
from subscription_pipeline import (  # noqa: E402
    LogCapture,
    run_pipeline,
    VALID_EVENT,
)

from consumers.main import warm_title_cache  # noqa: E402
from consumers.metrics import metrics  # noqa: E402
from consumers.schemas import (  # noqa: E402
    ChatDeletedEvent,
    NewChatCreatedEvent,
)
from containers.providers import DefaultProvider  # noqa: E402
from dtos.messages import (  # noqa: E402
    ChatListenerDTO,
    ChatListItemDTO,
)
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
	chat_list_malformed: bool = False,
	chat_info_malformed: bool = False,
) -> Provider:
	"""MockTransport с журналом запросов и умением ломать выбранные пути.

	Общий `offline_provider` отвечает на GET /chat/ одиночным объектом —
	конверта `{'items': [...], 'count': N}` у него нет, поэтому прогреву
	нужен собственный обработчик (см. gotcha в общем каркасе).

	`chat_list_malformed`/`chat_info_malformed` — 200 с телом, которое не
	проходит DTO: такое тело даёт `ValidationError` парсинга, то есть
	граничный сбой (наравне с `WebApiError`), а не программный баг.
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
					if chat_list_malformed:
						return Response(
							200,
							json={
								'items': [{'oid': 5, 'title': 7, 'created_at': 'not-a-date'}],
								'count': 1,
							},
						)
					return Response(
						200,
						json={'items': WARMED_ITEMS, 'count': len(WARMED_ITEMS)},
					)
				if request.url.path.endswith('/listeners/'):
					return Response(200, json=[{'oid': '42'}])
				if chat_info_status != 200:
					return Response(chat_info_status, json={'detail': 'Chat not found'})
				if chat_info_malformed:
					return Response(200, json={'oid': 5, 'title': 7, 'created_at': 'not-a-date'})
				return Response(200, json=CHAT_INFO)

			return AsyncClient(transport=MockTransport(handler))

	return TransportProvider()


class _BrokenDirectory(BaseChatDirectory):
	"""Стаб границы, падающий TypeError — программный баг, а не ошибка Web API.

	Нужен, чтобы отличить «граничный сбой → фолбэк/предупреждение» от
	«баг → исключение всплывает» (ADR-0004: баг не должен маскироваться
	под штатное поведение).
	"""

	def __init__(self, failing_method: str) -> None:
		self._failing_method = failing_method

	async def get_all_chats(self) -> list[ChatListItemDTO]:
		if self._failing_method == 'get_all_chats':
			raise TypeError('programming bug inside directory')
		return []

	async def get_chat_listeners(self, chat_oid: str) -> list[ChatListenerDTO]:
		return []

	async def add_listener(self, telegram_chat_id: int, chat_oid: str) -> None:
		return None

	async def get_chat_info(self, chat_oid: str) -> ChatListItemDTO:
		if self._failing_method == 'get_chat_info':
			raise TypeError('programming bug inside directory')
		raise AssertionError('стаб не ожидал get_chat_info')


def _buggy_directory_provider() -> Provider:
	"""Поздний провайдер перекрывает BaseChatDirectory сломанным стабом.

	В dishka поздний провайдер выигрывает — тот же приём, что в
	`run_pipeline` для транспорта.
	"""
	class BuggyDirectoryProvider(Provider):
		@provide(scope=Scope.REQUEST)
		def get_chat_directory(self) -> BaseChatDirectory:
			return _BrokenDirectory('get_all_chats')

	return BuggyDirectoryProvider()


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


async def _check_cold_start_notification() -> None:
	# Холодный старт без прогрева: уведомление всё равно содержит заголовок —
	# read-through добывает его одним запросом и кладёт в кэш.
	requests: list[str] = []
	metrics.reset()
	sink, _capture = await run_pipeline([VALID_EVENT], requests_seen=requests)
	assert len(sink.notifications) == 1, sink.notifications
	_tg_chat_id, notification = sink.notifications[0]
	assert notification.title == 'chat title', notification.title
	assert requests == ['GET /chat/chat-oid/', 'GET /chat/chat-oid/listeners/'], requests


async def _check_read_through_once() -> None:
	# Промах кэша — точечный read-through: два сообщения в один чат дают
	# ровно один GET /chat/{oid}/; второе сообщение живёт за счёт кэша.
	requests: list[str] = []
	metrics.reset()
	second = {**VALID_EVENT, 'event_id': str(uuid.uuid4()), 'message_oid': 'm2'}
	sink, _capture = await run_pipeline([VALID_EVENT, second], requests_seen=requests)
	assert len(sink.notifications) == 2, sink.notifications
	chat_info_requests = [entry for entry in requests if entry == 'GET /chat/chat-oid/']
	assert len(chat_info_requests) == 1, requests
	assert requests.count('GET /chat/chat-oid/listeners/') == 2, requests


async def _check_hot_path_single_request() -> None:
	# Тёплый кэш (прогрев на старте) + событие о сообщении = ровно один
	# HTTP-запрос, за listener'ами; chat-info в журнале нет, заголовок —
	# из прогретого списка.
	requests: list[str] = []
	metrics.reset()
	sink, _capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests,
		before_publish=warm_title_cache,
		extra_providers=[_transport_provider(requests)],
	)
	assert len(sink.notifications) == 1, sink.notifications
	_tg_chat_id, notification = sink.notifications[0]
	assert notification.title == 'Warmed Title', notification.title
	assert requests == ['GET /chat/', 'GET /chat/chat-oid/listeners/'], requests


async def _check_pipeline_survives_warmup_failure() -> None:
	# Спека, тест 10: прогрев падает 500-м — consumer всё равно принимает
	# события и отправляет уведомление с заголовком, добытым по промаху.
	requests: list[str] = []
	capture = LogCapture()
	title_cache_logger = logging.getLogger('services.title_cache')
	title_cache_logger.addHandler(capture)
	metrics.reset()
	try:
		sink, _capture = await run_pipeline(
			[VALID_EVENT],
			requests_seen=requests,
			before_publish=warm_title_cache,
			extra_providers=[_transport_provider(requests, chat_list_status=500)],
		)
	finally:
		title_cache_logger.removeHandler(capture)

	assert capture.records, 'warmup failure must be logged as warning'
	assert len(sink.notifications) == 1, sink.notifications
	_tg_chat_id, notification = sink.notifications[0]
	assert notification.title == 'chat title', notification.title
	assert 'GET /chat/' in requests, requests
	assert 'GET /chat/chat-oid/' in requests, requests


async def _check_created_event_keeps_cache_warm() -> None:
	# Событие создания чата попадает в кэш без перезапуска процесса:
	# уведомление содержит заголовок из события, журнала HTTP — только
	# listener'ы, запроса за информацией о чате нет (спека, тест 7).
	requests: list[str] = []
	metrics.reset()
	sink, capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests,
		lifecycle_events=[(get_settings().NEW_CHATS_TOPIC, CREATED_EVENT)],
	)
	assert len(sink.notifications) == 1, sink.notifications
	_tg_chat_id, notification = sink.notifications[0]
	assert notification.title == 'Fresh Title', notification.title
	assert not capture.records, capture.records
	assert requests == ['GET /chat/chat-oid/listeners/'], requests
	assert metrics.broken_events == 0, metrics.broken_events


async def _check_deleted_event_suppresses_notification() -> None:
	# Событие удаления чата ставит tombstone: уведомление по несуществующему
	# чату не отправляется, и пропуск идёт ДО HTTP — ни chat-info, ни
	# listener'ами (спека, тест 9; билет, п. «удаление»).
	requests: list[str] = []
	metrics.reset()
	sink, capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests,
		lifecycle_events=[(get_settings().CHAT_DELETED_TOPIC, DELETED_EVENT)],
	)
	assert not sink.notifications, sink.notifications
	assert not requests, requests
	assert metrics.broken_events == 0, metrics.broken_events
	# Пропуск виден в логе — не молчаливое исчезновение уведомления.
	assert capture.records, 'suppressed notification must be logged'
	messages = ' | '.join(record.getMessage() for record in capture.records)
	assert 'Уведомление пропущено' in messages, messages
	assert 'chat-oid' in messages, messages


async def _check_broken_lifecycle_event_is_logged() -> None:
	# Битое событие жизненного цикла не роняет consumer'а: лог с причиной,
	# метка «битое», конвейер продолжает работать (п. «невалидный payload»).
	# Точная формулировка закреплена: общий хелпер обязан сохранять её
	# байт-в-байт для каждого потока (new-message/chat-created/chat-deleted).
	requests: list[str] = []
	metrics.reset()
	broken = {key: value for key, value in CREATED_EVENT.items() if key != 'chat_title'}
	sink, capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests,
		lifecycle_events=[(get_settings().NEW_CHATS_TOPIC, broken)],
	)
	assert metrics.broken_events == 1, metrics.broken_events
	assert capture.records, 'broken lifecycle event must be logged'
	broken_logs = [
		record.getMessage()
		for record in capture.records
		if record.getMessage().startswith('Битое событие')
	]
	assert broken_logs == [
		'Битое событие chat-created отсеяно: '
		'event_id=12345678-1234-5678-1234-567812345678 причина=chat_title: Field required',
	], broken_logs
	messages = ' | '.join(record.getMessage() for record in capture.records)
	assert 'chat_title' in messages, messages
	# Сообщение дошло несмотря на битое событие — заголовок добран read-through'ом.
	assert len(sink.notifications) == 1, sink.notifications
	_tg_chat_id, notification = sink.notifications[0]
	assert notification.title == 'chat title', notification.title


async def _check_get_title_propagates_programming_bug() -> None:
	# F1: TypeError — программный баг, а не граничный сбой: он НЕ должен
	# превращаться в chat_oid-заглушку. Ловятся только WebApiError и
	# ValidationError парсинга; прочее всплывает (ADR-0004 — не маскировать).
	cache = ChatTitleCache()
	try:
		await cache.get_title('chat-oid', _BrokenDirectory('get_chat_info'))
	except TypeError:
		pass
	else:
		raise AssertionError('programming bug inside get_title must propagate')


async def _check_warm_up_propagates_programming_bug() -> None:
	# F1: то же для warm_up: баг всплывает наружу, широкая страховка от
	# любого падения живёт отдельным слоем — в warm_title_cache (п. 11).
	cache = ChatTitleCache()
	try:
		await cache.warm_up(_BrokenDirectory('get_all_chats'))
	except TypeError:
		pass
	else:
		raise AssertionError('programming bug inside warm_up must propagate')


async def _check_startup_hook_swallows_bug_with_distinct_log() -> None:
	# F2: хук warm_title_cache — широкая страховка: ловит ВСЁ, включая баги,
	# всплывшие из warm_up, и отказы DI — подписка не блокируется (п. 11).
	# Сообщение обязано отличаться от внутреннего («…не удался»), явно
	# заявлять продолжение подписки и нести исключение с трейсбеком.
	capture = LogCapture()
	main_logger = logging.getLogger('consumers.main')
	main_logger.addHandler(capture)
	container = make_async_container(
		DefaultProvider(),
		_buggy_directory_provider(),
	)
	try:
		await warm_title_cache(container)  # не должно упасть
	finally:
		main_logger.removeHandler(capture)
		await container.close()
	assert capture.records, 'startup hook must log the failure'
	record = capture.records[0]
	message = record.getMessage()
	assert 'Прогрев Chat Title Cache не удался' not in message, message
	assert 'подписка продолжается' in message, message
	assert record.exc_info is not None, 'failure must travel with traceback'


async def _check_malformed_warmup_list_is_boundary_failure() -> None:
	# Охрана F1: 200 с битым телом списка — ValidationError парсинга DTO,
	# то есть граничный сбой: тонкий warm_up логирует и не пробрасывает
	# (ловится именно им, а не только внешним хуком).
	requests: list[str] = []
	capture = LogCapture()
	title_cache_logger = logging.getLogger('services.title_cache')
	title_cache_logger.addHandler(capture)
	container = make_async_container(
		DefaultProvider(),
		_transport_provider(requests, chat_list_malformed=True),
	)
	try:
		async with container() as scope:
			cache = await scope.get(ChatTitleCache)
			directory = await scope.get(BaseChatDirectory)
			await cache.warm_up(directory)  # не должно упасть
		assert capture.records, 'malformed warmup body must be logged as warning'
		assert requests == ['GET /chat/'], requests
	finally:
		title_cache_logger.removeHandler(capture)
		await container.close()


async def _check_malformed_chat_info_falls_back() -> None:
	# Охрана F1: битое тело GET /chat/{oid}/ — ValidationError парсинга,
	# get_title ловит его наравне с WebApiError: chat_oid запасным + warning.
	requests: list[str] = []
	capture = LogCapture()
	title_cache_logger = logging.getLogger('services.title_cache')
	title_cache_logger.addHandler(capture)
	container = make_async_container(
		DefaultProvider(),
		_transport_provider(requests, chat_info_malformed=True),
	)
	try:
		async with container() as scope:
			cache = await scope.get(ChatTitleCache)
			directory = await scope.get(BaseChatDirectory)
			title = await cache.get_title('chat-oid', directory)
		assert title == 'chat-oid', title
		assert requests == ['GET /chat/chat-oid/'], requests
		assert capture.records, 'malformed chat info must be logged as warning'
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
	await _check_cold_start_notification()
	await _check_read_through_once()
	await _check_hot_path_single_request()
	await _check_pipeline_survives_warmup_failure()
	await _check_created_event_keeps_cache_warm()
	await _check_deleted_event_suppresses_notification()
	await _check_broken_lifecycle_event_is_logged()
	await _check_get_title_propagates_programming_bug()
	await _check_warm_up_propagates_programming_bug()
	await _check_startup_hook_swallows_bug_with_distinct_log()
	await _check_malformed_warmup_list_is_boundary_failure()
	await _check_malformed_chat_info_falls_back()

	print('TITLE_CACHE_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
