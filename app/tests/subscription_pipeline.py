"""Общий каркас офлайн-смоуков конвейера подписки new-message (карточка №5).

Содержит то, что копировалось между `smoke_event_contract` и `smoke_dedup`:
валидный payload события, перехватчик логов consumer'а и запуск настоящего
конвейера (реальный роутер + `TestKafkaBroker` + DI-контейнер) с офлайн-подменами
выхода: in-memory Reply Registry, заданный дедупликатор, MockTransport HTTP и
sink-уведомитель. Подключается смоуками после bootstrap `sys.path` — так же,
как `notifier_fakes`.
"""

import logging
from collections.abc import (
    Awaitable,
    Callable,
    Sequence,
)

import httpx
from dishka import (
    AsyncContainer,
    make_async_container,
    provide,
    Provider,
    Scope,
)
from dishka.integrations.faststream import setup_dishka
from faststream import FastStream
from faststream.kafka import (
    KafkaBroker,
    TestKafkaBroker,
)
from httpx import (
    AsyncClient,
    MockTransport,
    Response,
)
from notifier_fakes import (
    notifier_override_provider,
    RecordingNotifier,
)
from settings import get_settings

from consumers.handlers import router
from containers.providers import DefaultProvider
from services.dedup import (
    BaseEventDeduplicator,
    InMemoryEventDeduplicator,
)
from services.notifier import (
    NewMessageNotification,
    TelegramAddress,
)
from services.replies import (
    BaseReplyRegistry,
    InMemoryReplyRegistry,
)


VALID_EVENT = {
	'event_id': '12345678-1234-5678-1234-567812345678',
	'occurred_at': '2026-10-07T00:00:00',
	'message_text': 'hello',
	'message_oid': 'm1',
	'chat_oid': 'chat-oid',
}


class LogCapture(logging.Handler):
	def __init__(self) -> None:
		super().__init__()
		self.records: list[logging.LogRecord] = []

	def emit(self, record: logging.LogRecord) -> None:
		self.records.append(record)


class RegistryWritingNotifier(RecordingNotifier):
	"""Sink, который после отправки пишет Reply Registry — как `TelegramNotifier`.

	Без этого запись в Reply Registry в принципе не наблюдалась бы (обычный sink
	пишет только список уведомлений), и проверка «дубликат не пишет Reply
	Registry» ничего бы не доказывала.
	"""

	def __init__(self, reply_registry: BaseReplyRegistry) -> None:
		super().__init__()
		self._reply_registry = reply_registry
		self._next_message_id = 1

	async def notify_new_message(
		self,
		tg_chat_id: TelegramAddress,
		notification: NewMessageNotification,
	) -> None:
		await super().notify_new_message(tg_chat_id, notification)
		await self._reply_registry.remember(
			self._next_message_id,
			notification.chat_oid,
		)
		self._next_message_id += 1


def offline_provider(
	requests_seen: list[str],
	*,
	reply_registry: BaseReplyRegistry,
) -> Provider:
	"""Офлайн-подмены выхода конвейера: Reply Registry и MockTransport HTTP."""
	class OfflineProvider(Provider):
		@provide(scope=Scope.APP)
		def get_reply_registry(self) -> BaseReplyRegistry:
			return reply_registry

		@provide(scope=Scope.REQUEST)
		def get_http_client(self) -> AsyncClient:
			def transport_handler(request: httpx.Request) -> Response:
				requests_seen.append(f'{request.method} {request.url.path}')
				if request.url.path.endswith('/listeners/'):
					return Response(200, json=[{'oid': '42'}])
				return Response(
					200,
					json={
						'oid': 'chat-oid',
						'title': 'chat title',
						'created_at': '2026-01-01T00:00:00Z',
					},
				)

			return AsyncClient(transport=MockTransport(transport_handler))

	return OfflineProvider()


def deduplicator_override_provider(dedup: BaseEventDeduplicator) -> Provider:
	"""Подменяет дедупликатор (в оффлайне — in-memory бэкенд)."""
	class DeduplicatorOverrideProvider(Provider):
		@provide(scope=Scope.APP)
		def get_event_deduplicator(self) -> BaseEventDeduplicator:
			return dedup

	return DeduplicatorOverrideProvider()


async def run_pipeline(
	events: list[dict],
	*,
	requests_seen: list[str],
	dedup: BaseEventDeduplicator | None = None,
	reply_registry: BaseReplyRegistry | None = None,
	before_publish: Callable[[AsyncContainer], Awaitable[None]] | None = None,
	extra_providers: Sequence[Provider] = (),
) -> tuple[RecordingNotifier, LogCapture]:
	"""Прогоняет payload'ы через настоящий конвейер подписки (без Kafka-сервера).

	На каждый прогон собираются новый брокер, новый DI-контейнер и новый
	перехватчик логов — на уровне конвейера прогон эквивалентен перезапуску
	процесса. Сценариям, которым нужно общее состояние между прогонами (хранилище
	дедупликации, Reply Registry), свои экземпляры передаются явно; по умолчанию
	создаются свежие in-memory.

	Дополнительные ручки (билет 03, только добавлены — дефолты не меняют
	поведения существующих смоуков):
	* `before_publish` — вызывается с контейнером после сборки и до публикации
	  (например, прогрев Chat Title Cache: FastStream-хуки `run_pipeline`
	  не запускает);
	* `extra_providers` — провайдеры, добавляемые ПОСЛЕ общей оффлайн-подмены:
	  в dishka поздний провайдер выигрывает, ими перекрывается транспорт
	  (envelope для GET /chat/, инъекция сбоя прогрева и т.п.).
	"""
	settings = get_settings()
	if dedup is None:
		dedup = InMemoryEventDeduplicator()
	if reply_registry is None:
		reply_registry = InMemoryReplyRegistry()

	sink = RegistryWritingNotifier(reply_registry)

	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)

	container = make_async_container(
		DefaultProvider(),
		offline_provider(requests_seen, reply_registry=reply_registry),
		deduplicator_override_provider(dedup),
		notifier_override_provider(sink),
		*extra_providers,
	)
	setup_dishka(container, app)

	if before_publish is not None:
		await before_publish(container)

	capture = LogCapture()
	consumer_logger = logging.getLogger('consumers.handlers')
	consumer_logger.addHandler(capture)
	consumer_logger.setLevel(logging.WARNING)

	async with TestKafkaBroker(broker) as test_broker:
		for event in events:
			await test_broker.publish(
				event,
				topic=settings.NEW_MESSAGE_TOPIC,
				key=b'chat-oid',
			)

	await container.close()
	consumer_logger.removeHandler(capture)
	return sink, capture
