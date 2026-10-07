"""Offline smoke: Event Deduplicator (карточка №5, билет 02).

Проверяется внешнее поведение дедупликации:
- операция хранилища `mark_if_new` атомарна: N конкурентных вызовов для одного
  `event_id` дают ровно один признак «новое»;
- Valkey-бэкенд ставит отметку одной командой `SET ... EX ... NX` с TTL
  (бэкенд пропускается с явным сообщением, если сервер недоступен);
- TTL по умолчанию — сутки (86400): дефолт живёт только в settings и приходит
  в Valkey-бэкенд через DI — проверяется резолвом из контейнера на DefaultProvider;
- повторная доставка того же события через конвейер подписки не порождает
  побочных эффектов: ровно одно уведомление, ни одного HTTP-запроса, ни одной
  записи в Reply Registry, метрика «дубликат» увеличивается, в лог попадает
  `event_id`;
- backlog-лавина: батч из N уже виденных событий (redelivered строки outbox)
  даёт N дубликатов, ноль уведомлений, ноль HTTP-запросов, без исключений;
- два сценария рестарта:
  * то же хранилище + заново собранный конвейер → событие всё ещё дубликат
    (состояние живёт в хранилище, а не в обработчике);
  * пересозданный in-memory override + новый конвейер → событие снова новое
    (реализация не полагается на in-process состояние; in-memory не durable).

Общий каркас конвейера (payload, логи, запуск) — `subscription_pipeline.py`.

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
import uuid  # noqa: E402

from dishka import make_async_container  # noqa: E402
from notifier_fakes import (  # noqa: E402
    notifier_override_provider,
    RecordingNotifier,
)
from settings import get_settings  # noqa: E402
from subscription_pipeline import (  # noqa: E402
    offline_provider,
    run_pipeline,
    VALID_EVENT,
)
from valkey.asyncio import Valkey  # noqa: E402

from consumers.metrics import metrics  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402
from services.dedup import (  # noqa: E402
    BaseEventDeduplicator,
    EVENT_DEDUP_PREFIX,
    InMemoryEventDeduplicator,
    ValkeyEventDeduplicator,
)
from services.replies import InMemoryReplyRegistry  # noqa: E402


CONCURRENCY = 50
BACKLOG_SIZE = 5


def _check_settings_default() -> None:
	# Единственный источник дефолта — настройка: сутки (86400 секунд).
	# Имена полей реализации (например, ключевой префикс) не проверяются.
	assert get_settings().EVENT_DEDUP_TTL == 86400


async def _check_in_memory_backend() -> None:
	dedup = InMemoryEventDeduplicator()
	event_id = uuid.uuid4()
	assert await dedup.mark_if_new(event_id) is True, 'первое событие — новое'
	assert await dedup.mark_if_new(event_id) is False, 'повтор — дубликат'
	other = uuid.uuid4()
	assert await dedup.mark_if_new(other) is True, 'другой event_id независим'

	# Уже виденное событие: все конкурентные участники получают False.
	results = await asyncio.gather(
		*(dedup.mark_if_new(event_id) for _ in range(CONCURRENCY)),
	)
	assert not any(results), results

	# Атомарность: N конкурентных mark_if_new для одного нового event_id →
	# ровно один признак «новое» (хранилище, без Kafka).
	fresh = uuid.uuid4()
	results = await asyncio.gather(
		*(dedup.mark_if_new(fresh) for _ in range(CONCURRENCY)),
	)
	assert sum(results) == 1, results


async def _check_valkey_backend() -> None:
	settings = get_settings()
	url = os.environ.get('VALKEY_URL', 'valkey://localhost:6379/0')
	client = Valkey.from_url(url)
	try:
		try:
			await client.ping()
		except Exception as error:
			print(f'VALKEY_SKIP: {type(error).__name__}: {error}')
			return

		dedup = ValkeyEventDeduplicator(client=client, ttl=settings.EVENT_DEDUP_TTL)
		event_id = uuid.uuid4()
		other = uuid.uuid4()
		try:
			assert await dedup.mark_if_new(event_id) is True, 'SET NX — новое'
			assert await dedup.mark_if_new(event_id) is False, 'ключ уже был'

			# TTL реально применён к ключу отметки.
			key = EVENT_DEDUP_PREFIX + str(event_id)
			ttl = await client.ttl(key)
			assert 0 < ttl <= settings.EVENT_DEDUP_TTL, ttl

			# Атомарность на сервере: конкурентные SET NX → ровно один True.
			results = await asyncio.gather(
				*(dedup.mark_if_new(other) for _ in range(CONCURRENCY)),
			)
			assert sum(results) == 1, results
		finally:
			await client.delete(EVENT_DEDUP_PREFIX + str(event_id))
			await client.delete(EVENT_DEDUP_PREFIX + str(other))
	finally:
		await client.aclose()


async def _check_ttl_from_settings() -> None:
	# Резолв из DI-контейнера на DefaultProvider: Valkey-бэкенд дедупликатора
	# получает TTL из настроек. Соединение при резолве не открывается —
	# valkey-py подключается лениво, при первом командном вызове; живой сервер
	# для этой проверки не нужен.
	settings = get_settings()
	requests_seen: list[str] = []
	container = make_async_container(
		DefaultProvider(),
		offline_provider(requests_seen, reply_registry=InMemoryReplyRegistry()),
		notifier_override_provider(RecordingNotifier()),
	)
	try:
		deduplicator = await container.get(BaseEventDeduplicator)
		assert isinstance(deduplicator, ValkeyEventDeduplicator), type(deduplicator)
		assert deduplicator.ttl == settings.EVENT_DEDUP_TTL, deduplicator.ttl
	finally:
		await container.close()


async def _check_pipeline_duplicates() -> None:
	# Одно и то же событие дважды в одном конвейере: ровно одно уведомление,
	# второй доставке не соответствует ни одного HTTP-запроса и ни одной
	# записи в Reply Registry (спека, тест 4).
	requests_seen: list[str] = []
	dedup = InMemoryEventDeduplicator()
	registry = InMemoryReplyRegistry()
	metrics.reset()
	sink, _capture = await run_pipeline(
		[VALID_EVENT, VALID_EVENT],
		requests_seen=requests_seen,
		dedup=dedup,
		reply_registry=registry,
	)
	assert len(sink.notifications) == 1, sink.notifications
	# get_chat_info + get_chat_listeners — только у первой доставки.
	assert len(requests_seen) == 2, requests_seen
	assert metrics.broken_events == 0, metrics.broken_events
	assert metrics.duplicate_events == 1, metrics.duplicate_events

	# Первая доставка записала Reply Registry — иначе проверка ниже была бы
	# пустой (sink пишет registry только когда его действительно вызвали).
	assert await registry.resolve(1) == 'chat-oid'
	# Дубликат не дошёл до уведомителя: sink пишет registry подрядовыми id,
	# вторая запись имела бы id 2 — её нет, состояние не изменилось.
	assert await registry.resolve(2) is None


async def _check_backlog_avalanche() -> None:
	# Переработка застрявших строк outbox: хранилище уже содержит отметки для
	# BACKLOG_SIZE событий; redelivered батч не порождает ни уведомлений, ни
	# HTTP-запросов, каждый дубликат учтён в метрике, исключений нет.
	dedup = InMemoryEventDeduplicator()
	events = [
		{**VALID_EVENT, 'event_id': str(uuid.uuid4()), 'message_oid': f'm{i}'}
		for i in range(BACKLOG_SIZE)
	]
	for event in events:
		is_new = await dedup.mark_if_new(uuid.UUID(event['event_id']))
		assert is_new, event['event_id']

	requests_seen: list[str] = []
	metrics.reset()
	sink, capture = await run_pipeline(
		events,
		requests_seen=requests_seen,
		dedup=dedup,
	)
	assert not sink.notifications, sink.notifications
	assert not requests_seen, requests_seen
	assert metrics.broken_events == 0, metrics.broken_events
	assert metrics.duplicate_events == BACKLOG_SIZE, metrics.duplicate_events

	# Каждое событие батча залогировано как дубликат со своим event_id.
	assert len(capture.records) == BACKLOG_SIZE, len(capture.records)
	logged = ' | '.join(record.getMessage() for record in capture.records)
	for event in events:
		assert event['event_id'] in logged, event['event_id']


async def _check_pipeline_restart_same_storage() -> None:
	# Рестарт-выживание: тот же экземпляр хранилища + заново собранный
	# конвейер (новые брокер и контейнер — на уровне конвейера это рестарт
	# процесса). Отметка лежит в хранилище, а не в обработчике или модуле,
	# поэтому повторная доставка снова отсекается.
	shared_dedup = InMemoryEventDeduplicator()
	requests_seen: list[str] = []

	metrics.reset()
	sink, _capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests_seen,
		dedup=shared_dedup,
	)
	assert len(sink.notifications) == 1, sink.notifications
	assert len(requests_seen) == 2, requests_seen
	assert metrics.duplicate_events == 0, metrics.duplicate_events

	requests_seen.clear()
	metrics.reset()
	sink, capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests_seen,
		dedup=shared_dedup,
	)
	assert not sink.notifications, sink.notifications
	assert not requests_seen, requests_seen
	assert metrics.duplicate_events == 1, metrics.duplicate_events
	assert capture.records, 'duplicate must be logged'
	message = capture.records[0].getMessage()
	assert 'Дубликат' in message, message
	assert VALID_EVENT['event_id'] in message, message


async def _check_pipeline_restart_fresh_storage() -> None:
	# Пересоздание in-memory override: хранилище и конвейер собраны заново —
	# событие, доставленное до пересоздания, снова обрабатывается как новое.
	# Доказывает, что реализация не полагается на in-process состояние, и
	# документирует: in-memory бэкенд не durable (в проде рестарт переживает
	# Valkey-бэкенд — отметки живут в Valkey с TTL).
	requests_seen: list[str] = []
	metrics.reset()
	sink, _capture = await run_pipeline([VALID_EVENT], requests_seen=requests_seen)
	assert len(sink.notifications) == 1, sink.notifications
	assert len(requests_seen) == 2, requests_seen
	assert metrics.duplicate_events == 0, metrics.duplicate_events

	requests_seen.clear()
	metrics.reset()
	fresh_dedup = InMemoryEventDeduplicator()  # пересоздание хранилища
	sink, _capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests_seen,
		dedup=fresh_dedup,
	)
	assert len(sink.notifications) == 1, sink.notifications  # снова новое
	assert len(requests_seen) == 2, requests_seen
	assert metrics.duplicate_events == 0, metrics.duplicate_events


async def main() -> None:
	_check_settings_default()
	await _check_in_memory_backend()
	await _check_valkey_backend()
	await _check_ttl_from_settings()
	await _check_pipeline_duplicates()
	await _check_backlog_avalanche()
	await _check_pipeline_restart_same_storage()
	await _check_pipeline_restart_fresh_storage()

	print('DEDUP_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
