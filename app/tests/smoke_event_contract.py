"""Offline smoke: контракт New-Message Event (карточка №5, билет 01).

Проверяется внешнее поведение контракта:
- форма на проводе: ровно пять полей в серверном порядке (закрепляется на
  фикстуре, которая уходит в конвейер настоящими байтами);
- типы на ГРАНИЦЕ конвейера: `event_id` — `UUID` (наблюдение на seam'е
  дедупликации), `occurred_at` — `datetime` (наблюдение через битую дату) —
  тест не зависит от имени клиентского класса схемы;
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
from uuid import UUID  # noqa: E402

from pydantic import ValidationError  # noqa: E402
from subscription_pipeline import (  # noqa: E402
    run_pipeline,
    VALID_EVENT,
)

from consumers.metrics import metrics  # noqa: E402
from consumers.schemas import NewMessageEvent  # noqa: E402
from services.dedup import BaseEventDeduplicator  # noqa: E402


class _RecordingDeduplicator(BaseEventDeduplicator):
	"""Seam-наблюдение: запоминает, что именно пришло в `mark_if_new`.

	Возвращает True — событие доходит до конца конвейера, а тест смотрит на
	объект, выходящий из границы (реальные байты -> handler -> валидация ->
	бизнес-код), не зная имени клиентского класса схемы: переименование класса
	этот наблюдатель не ломает.
	"""

	def __init__(self) -> None:
		self.marked: list[object] = []

	async def mark_if_new(self, event_id: UUID) -> bool:
		self.marked.append(event_id)
		return True


def _check_wire_form() -> None:
	# Закрепление формы на проводе (билет 05, «Контракт: типы»). Серверный
	# `@dataclass` (BaseEvent + payload-поля) orjson с OPT_SERIALIZE_DATACLASS
	# сериализует в порядке объявления полей: event_id, occurred_at, затем
	# payload. Фикстура VALID_EVENT воспроизводит эту форму и публикуется
	# настоящими байтами (TestKafkaBroker), поэтому её порядок и есть порядок
	# на проводе. Пин ТОЧНЫЙ — ровно эти пять полей, ровно в этом порядке:
	# для dev-stack контракта перестановка/переименование/добавление поля
	# обязаны падать осознанно, а не проходить молча. Цена точного пина:
	# добавление сервером ОПЦИОНАЛЬНОГО поля (неломающее изменение) уронит пин,
	# и фикстуру придётся обновить зеркально; терпимость клиента к лишним
	# полям доказывается отдельно (сценарий «неизвестные поля» ниже), поэтому
	# пин не ослабляем до startswith/супермножества. «Падает, если сервер
	# поменяет тип»: при зеркальном обновлении фикстуры под новый тип ломаются
	# валидация и строгие типы ниже — контрактный тест падает.
	assert list(VALID_EVENT) == [
		'event_id',
		'occurred_at',
		'message_text',
		'message_oid',
		'chat_oid',
	], list(VALID_EVENT)


def _check_schema_contract() -> None:
	# Типы полей здесь не проверяются: контракт типов наблюдается на границе
	# конвейера — `UUID` на seam'е дедупликации, `datetime` через битую дату
	# (сценарии в main). Контрактные утверждения не ссылаются на имя
	# клиентского класса схемы — переименование его их не роняет.
	event = NewMessageEvent.model_validate(VALID_EVENT)

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
	_check_wire_form()
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
	# Формулировка байт-в-байт закреплена: общий хелпер битого payload'а
	# обязан сохранять её для new-message (рефакторинг дедупликации блока).
	assert message == (
		'Битое событие new-message отсеяно: '
		'event_id=12345678-1234-5678-1234-567812345678 причина=chat_oid: Field required'
	), message

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

	# Неразбираемая дата — тоже битое событие. Это и есть наблюдение типа
	# `occurred_at` = datetime ПОВЕДЕНИЕМ на границе: прямого потребителя
	# `occurred_at` ниже нет, но если схему откатить в `str`, payload
	# провалидируется и уведомление уйдёт — тогда сразу упадут проверки
	# «нет уведомления» и метка «битое». Здесь дополнительно байт-в-байт
	# закреплена причина с формулировкой pydantic для datetime: пустой лог или
	# другая причина тоже роняют тест.
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
	message = capture.records[0].getMessage()
	assert message == (
		'Битое событие new-message отсеяно: '
		'event_id=12345678-1234-5678-1234-567812345678 причина=occurred_at: '
		'Input should be a valid datetime or date, invalid character in year'
	), message

	# Валидное событие проходит конвейер и доставляет уведомление.
	requests_seen.clear()
	metrics.reset()
	recorder = _RecordingDeduplicator()
	sink, capture = await run_pipeline(
		[VALID_EVENT],
		requests_seen=requests_seen,
		dedup=recorder,
	)
	assert len(sink.notifications) == 1, sink.notifications
	assert metrics.broken_events == 0, metrics.broken_events
	tg_chat_id, notification = sink.notifications[0]
	assert tg_chat_id == '42', tg_chat_id
	assert notification.chat_oid == 'chat-oid', notification
	assert notification.title == 'chat title', notification
	assert notification.text == 'hello', notification
	assert not capture.records, capture.records

	# Наружу — типизированный объект: после ПОЛНОГО прохода конвейера
	# (реальные байты -> handler -> валидация -> бизнес-код) в seam
	# дедупликации приходит `UUID`, а не строка. Наблюдение не ссылается на
	# имя клиентского класса схемы (переименование его не ломает тест);
	# смена сервером типа event_id при зеркальном обновлении фикстуры роняет
	# валидацию выше — контрактный тест падает.
	assert len(recorder.marked) == 1, recorder.marked
	marked = recorder.marked[0]
	assert isinstance(marked, UUID), type(marked)
	assert str(marked) == VALID_EVENT['event_id'], marked

	print('EVENT_CONTRACT_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
