"""Offline smoke: Event Deduplicator (карточка №5, билет 02).

Проверяется внешнее поведение дедупликации:
- операция хранилища `mark_seen` атомарна: N конкурентных вызовов для одного
  `event_id` дают ровно один признак «новое»;
- Valkey-бэкенд ставит отметку одной командой `SET ... EX ... NX` с TTL
  (бэкенд пропускается с явным сообщением, если сервер недоступен);
- TTL по умолчанию — сутки (86400), настраивается через `EVENT_DEDUP_TTL`;
- повторная доставка того же события через конвейер подписки не порождает
  побочных эффектов: ровно одно уведомление, HTTP-запросов на дубликат нет,
  метрика «дубликат» увеличивается, в лог попадает `event_id`;
- дедупликация переживает пересоздание контейнера/конвейера: состояние живёт
  в хранилище, а не в обработчике (same storage instance, new pipeline —
  повторное событие снова отсекается; это же покрывает переработку backlog'а
  outbox: redelivered строки — это дубликаты).

Запуск:  python tests/smoke_dedup.py   (из каталога app/)
Для Valkey-сценария:  VALKEY_URL=valkey://localhost:6379/0
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
from dishka.integrations.faststream import setup_dishka  # noqa: E402
from faststream import FastStream  # noqa: E402
from faststream.kafka import (  # noqa: E402
    KafkaBroker,
    TestKafkaBroker,
)
from httpx import (  # noqa: E402
    AsyncClient,
    MockTransport,
    Response,
)
from notifier_fakes import (  # noqa: E402
    notifier_override_provider,
    RecordingNotifier,
)
from settings import get_settings  # noqa: E402
from valkey.asyncio import Valkey  # noqa: E402

from consumers.handlers import router  # noqa: E402
from consumers.metrics import metrics  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402
from services.dedup import (  # noqa: E402
    BaseEventDeduplicator,
    EVENT_DEDUP_PREFIX,
    EVENT_DEDUP_TTL,
    InMemoryEventDeduplicator,
    ValkeyEventDeduplicator,
)
from services.replies import (  # noqa: E402
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

CONCURRENCY = 50


class _LogCapture(logging.Handler):
	def __init__(self) -> None:
		super().__init__()
		self.records: list[logging.LogRecord] = []

	def emit(self, record: logging.LogRecord) -> None:
		self.records.append(record)


def _check_settings_default() -> None:
	# Кодовая константа и настройка по умолчанию — сутки (86400 секунд).
	assert EVENT_DEDUP_TTL == 86400, EVENT_DEDUP_TTL
	assert get_settings().EVENT_DEDUP_TTL == 86400
	assert EVENT_DEDUP_PREFIX == 'event-dedup:'


async def _check_in_memory_backend() -> None:
	dedup = InMemoryEventDeduplicator()
	event_id = uuid.uuid4()
	assert await dedup.mark_seen(event_id) is True, 'первое событие — новое'
	assert await dedup.mark_seen(event_id) is False, 'повтор — дубликат'
	other = uuid.uuid4()
	assert await dedup.mark_seen(other) is True, 'другой event_id независим'

	# Уже виденное событие: все конкурентные участники получают False.
	results = await asyncio.gather(
		*(dedup.mark_seen(event_id) for _ in range(CONCURRENCY)),
	)
	assert not any(results), results

	# Атомарность: N конкурентных mark_seen для одного нового event_id →
	# ровно один признак «новое» (хранилище, без Kafka).
	fresh = uuid.uuid4()
	results = await asyncio.gather(
		*(dedup.mark_seen(fresh) for _ in range(CONCURRENCY)),
	)
	assert sum(results) == 1, results


async def _check_valkey_backend() -> None:
	url = os.environ.get('VALKEY_URL', 'valkey://localhost:6379/0')
	client = Valkey.from_url(url)
	try:
		try:
			await client.ping()
		except Exception as error:
			print(f'VALKEY_SKIP: {type(error).__name__}: {error}')
			return

		dedup = ValkeyEventDeduplicator(client=client, ttl=EVENT_DEDUP_TTL)
		event_id = uuid.uuid4()
		other = uuid.uuid4()
		try:
			assert await dedup.mark_seen(event_id) is True, 'SET NX — новое'
			assert await dedup.mark_seen(event_id) is False, 'ключ уже был'

			# TTL реально применён к ключу отметки.
			key = EVENT_DEDUP_PREFIX + str(event_id)
			ttl = await client.ttl(key)
			assert 0 < ttl <= EVENT_DEDUP_TTL, ttl

			# Атомарность на сервере: конкурентные SET NX → ровно один True.
			results = await asyncio.gather(
				*(dedup.mark_seen(other) for _ in range(CONCURRENCY)),
			)
			assert sum(results) == 1, results
		finally:
			await client.delete(EVENT_DEDUP_PREFIX + str(event_id))
			await client.delete(EVENT_DEDUP_PREFIX + str(other))
	finally:
		await client.aclose()


def _pipeline_providers(
	dedup: BaseEventDeduplicator,
	requests_seen: list[str],
) -> Provider:
	class OfflineProvider(Provider):
		@provide(scope=Scope.APP)
		def get_reply_registry(self) -> BaseReplyRegistry:
			return InMemoryReplyRegistry()

		@provide(scope=Scope.APP)
		def get_event_deduplicator(self) -> BaseEventDeduplicator:
			return dedup

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


async def _run_pipeline(
	events: list[dict],
	dedup: BaseEventDeduplicator,
	requests_seen: list[str],
) -> tuple[RecordingNotifier, _LogCapture]:
	settings = get_settings()
	sink = RecordingNotifier()

	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)

	container = make_async_container(
		DefaultProvider(),
		_pipeline_providers(dedup, requests_seen),
		notifier_override_provider(sink),
	)
	setup_dishka(container, app)

	capture = _LogCapture()
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


async def _check_pipeline_duplicates() -> None:
	# Одно и то же событие дважды в одном конвейере: ровно одно уведомление,
	# второй доставке не соответствует ни одного HTTP-запроса.
	requests_seen: list[str] = []
	dedup = InMemoryEventDeduplicator()
	metrics.reset()
	sink, _capture = await _run_pipeline([VALID_EVENT, VALID_EVENT], dedup, requests_seen)
	assert len(sink.notifications) == 1, sink.notifications
	# get_chat_info + get_chat_listeners — только у первой доставки.
	assert len(requests_seen) == 2, requests_seen
	assert metrics.broken_events == 0, metrics.broken_events
	assert metrics.duplicate_events == 1, metrics.duplicate_events


async def _check_pipeline_restart() -> None:
	# Пересоздание контейнера/конвейера при том же экземпляре хранилища:
	# состояние живёт в хранилище, а не в обработчике или модуле.
	shared_dedup = InMemoryEventDeduplicator()
	requests_seen: list[str] = []

	requests_seen.clear()
	metrics.reset()
	sink, _capture = await _run_pipeline([VALID_EVENT], shared_dedup, requests_seen)
	assert len(sink.notifications) == 1, sink.notifications
	assert len(requests_seen) == 2, requests_seen
	assert metrics.duplicate_events == 0, metrics.duplicate_events

	requests_seen.clear()
	metrics.reset()
	sink, capture = await _run_pipeline([VALID_EVENT], shared_dedup, requests_seen)
	assert not sink.notifications, sink.notifications
	assert not requests_seen, requests_seen
	assert metrics.duplicate_events == 1, metrics.duplicate_events
	assert capture.records, 'duplicate must be logged'
	message = capture.records[0].getMessage()
	assert 'Дубликат' in message, message
	assert VALID_EVENT['event_id'] in message, message


async def main() -> None:
	_check_settings_default()
	await _check_in_memory_backend()
	await _check_valkey_backend()
	await _check_pipeline_duplicates()
	await _check_pipeline_restart()

	print('DEDUP_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
