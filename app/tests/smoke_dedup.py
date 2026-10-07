"""Offline smoke: Event Deduplicator (карточка №5, билет 02).

Проверяется внешнее поведение дедупликации:
- операция хранилища `mark_if_new` атомарна: N конкурентных вызовов для одного
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

from settings import get_settings  # noqa: E402
from subscription_pipeline import (  # noqa: E402
    run_pipeline,
    VALID_EVENT,
)
from valkey.asyncio import Valkey  # noqa: E402

from consumers.metrics import metrics  # noqa: E402
from services.dedup import (  # noqa: E402
    EVENT_DEDUP_PREFIX,
    InMemoryEventDeduplicator,
    ValkeyEventDeduplicator,
)


CONCURRENCY = 50


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


async def _check_pipeline_duplicates() -> None:
	# Одно и то же событие дважды в одном конвейере: ровно одно уведомление,
	# второй доставке не соответствует ни одного HTTP-запроса.
	requests_seen: list[str] = []
	dedup = InMemoryEventDeduplicator()
	metrics.reset()
	sink, _capture = await run_pipeline(
		[VALID_EVENT, VALID_EVENT],
		requests_seen=requests_seen,
		dedup=dedup,
	)
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


async def main() -> None:
	_check_settings_default()
	await _check_in_memory_backend()
	await _check_valkey_backend()
	await _check_pipeline_duplicates()
	await _check_pipeline_restart()

	print('DEDUP_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
