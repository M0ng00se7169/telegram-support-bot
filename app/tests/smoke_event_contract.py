"""Offline smoke: контракт New-Message Event (карточка №5, билет 01).

Проверяется внешнее поведение контракта:
- типы: `event_id` — `UUID`, `occurred_at` — `datetime`, остальные поля — `str`;
- схема терпима к неизвестным полям (обогащение payload на сервере не ломает consumer'а);
- `event_title` (серверный `ClassVar`) отсутствует в клиентской схеме;
- `occurred_at` разбирается в наивное локальное время без зоны (сервер шлёт его без tzinfo);
- битое событие не доставляется, не пробрасывает исключение наружу, увеличивает
  метку «битое» и логируется с `event_id` и причиной.

Запуск:  python tests/smoke_event_contract.py   (из каталога app/)
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import asyncio  # noqa: E402

from pydantic import ValidationError  # noqa: E402
from subscription_pipeline import (  # noqa: E402
    run_pipeline,
    VALID_EVENT,
)

from consumers.metrics import metrics  # noqa: E402
from consumers.schemas import NewMessageEvent  # noqa: E402


def _check_schema_contract() -> None:
	event = NewMessageEvent.model_validate(VALID_EVENT)
	from datetime import datetime
	from uuid import UUID

	assert isinstance(event.event_id, UUID), type(event.event_id)
	assert isinstance(event.occurred_at, datetime), type(event.occurred_at)
	assert isinstance(event.chat_oid, str), type(event.chat_oid)

	# Неизвестные поля разрешены — в том числе отклонённое обогащение `title`.
	enriched = {**VALID_EVENT, 'title': 'route 77', 'extra_number': 1}
	assert NewMessageEvent.model_validate(enriched).chat_oid == 'chat-oid'

	# `event_title` — серверный ClassVar: его нет в клиентской схеме.
	assert 'event_title' not in NewMessageEvent.model_fields

	# Сервер шлёт наивное локальное время: значение разбирается в `datetime` без зоны.
	assert event.occurred_at.tzinfo is None, event.occurred_at.tzinfo

	# Обязательные поля — строго; неверные типы и неразбираемая дата отсекаются.
	for broken in (
		{k: v for k, v in VALID_EVENT.items() if k != 'chat_oid'},
		{**VALID_EVENT, 'event_id': 'not-a-uuid'},
		{**VALID_EVENT, 'event_id': 42},
		{**VALID_EVENT, 'occurred_at': 'not-a-date'},
		{**VALID_EVENT, 'message_text': 5},
		{**VALID_EVENT, 'message_oid': 7},
		{**VALID_EVENT, 'chat_oid': 42},
	):
		try:
			NewMessageEvent.model_validate(broken)
		except ValidationError:
			pass
		else:
			raise AssertionError(f'broken event must be rejected: {broken!r}')


requests_seen: list[str] = []


async def main() -> None:
	_check_schema_contract()

	# Битое событие: нет `chat_oid` — уведомление не доставлено, HTTP не тронут,
	# в лог попали `event_id` и причина, исключение наружу не вышло, метка «битое» +1.
	requests_seen.clear()
	metrics.reset()
	missing_chat = {k: v for k, v in VALID_EVENT.items() if k != 'chat_oid'}
	sink, capture = await run_pipeline([missing_chat], requests_seen=requests_seen)
	assert not sink.notifications, sink.notifications
	assert not requests_seen, requests_seen
	assert metrics.broken_events == 1, metrics.broken_events
	assert capture.records, 'broken event must be logged'
	message = capture.records[0].getMessage()
	assert '12345678-1234-5678-1234-567812345678' in message, message
	assert 'chat_oid' in message, message

	# Битый `event_id`: логируется сырое значение, не UUID.
	requests_seen.clear()
	metrics.reset()
	sink, capture = await run_pipeline(
		[{**VALID_EVENT, 'event_id': 'e1'}],
		requests_seen=requests_seen,
	)
	assert not sink.notifications, sink.notifications
	assert not requests_seen, requests_seen
	assert metrics.broken_events == 1, metrics.broken_events
	assert capture.records, 'bad event_id must be logged'
	assert 'e1' in capture.records[0].getMessage(), capture.records[0].getMessage()

	# Неразбираемая дата — тоже битое событие.
	requests_seen.clear()
	metrics.reset()
	sink, capture = await run_pipeline(
		[{**VALID_EVENT, 'occurred_at': 'not-a-date'}],
		requests_seen=requests_seen,
	)
	assert not sink.notifications, sink.notifications
	assert not requests_seen, requests_seen
	assert metrics.broken_events == 1, metrics.broken_events
	assert capture.records, 'unparseable occurred_at must be logged as broken'

	# Валидное событие проходит конвейер и доставляет уведомление.
	requests_seen.clear()
	metrics.reset()
	sink, capture = await run_pipeline([VALID_EVENT], requests_seen=requests_seen)
	assert len(sink.notifications) == 1, sink.notifications
	assert metrics.broken_events == 0, metrics.broken_events
	tg_chat_id, notification = sink.notifications[0]
	assert tg_chat_id == '42', tg_chat_id
	assert notification.chat_oid == 'chat-oid', notification
	assert notification.title == 'chat title', notification
	assert notification.text == 'hello', notification
	assert not capture.records, capture.records

	print('EVENT_CONTRACT_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
