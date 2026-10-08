"""Offline smoke: контракт New-Message Event (карточка №5, билеты 01 и 05).

Проверяется внешнее поведение контракта. Утверждения не ссылаются на имя
клиентского класса схемы и на имена его полей: в файле нет ни одного такого
упоминания — переименование приватного класса схемы этот смоук не роняет
(file-wide, проверяется статически):
- форма на проводе: ровно пять полей в серверном порядке (закрепляется на
  фикстуре, которая уходит в конвейер настоящими байтами), значение
  `occurred_at` — наивное локальное время сервера без зоны;
- типы на ГРАНИЦЕ конвейера: `event_id` — `UUID` (наблюдение на seam'е
  дедупликации), `occurred_at` — `datetime` (наблюдение через битую дату);
- конвейер терпим к неизвестным полям: отклонённое обогащение `title`,
  лишнее поле и серверный `event_title` на проводе — ровно одно уведомление,
  ни одного «битого» и ни одного предупреждения в логе;
- обязательные поля строго: payload без любого из пяти обязательных полей
  не доставляется, считается битым, логируется с `event_id` и причиной;
- битое событие не доставляется, не пробрасывает исключение наружу,
  увеличивает метку «битое» и логируется с `event_id` и причиной.

Запуск:  python tests/smoke_event_contract.py   (из каталога app/)
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import asyncio  # noqa: E402
from datetime import datetime  # noqa: E402
from uuid import UUID  # noqa: E402

from notifier_fakes import RecordingNotifier  # noqa: E402
from subscription_pipeline import (  # noqa: E402
    LogCapture,
    run_pipeline,
    VALID_EVENT,
)

from consumers.metrics import metrics  # noqa: E402
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


def _check_naive_occurred_at() -> None:
	# Наивное локальное время без зоны закрепляется на фикстуре (та же
	# природа, что у пина формы: сервер шлёт `datetime` без tzinfo, и его
	# зеркало на проводе обязано разбираться в naive datetime — смена
	# сервера на aware-формат падает здесь). Поведенческая цепочка дальше,
	# без имени класса схемы: наивное значение обязано и дальше доставляться —
	# в сценарии валидного события (main) ровно одно уведомление и
	# `broken == 0`; если бы схема откатилась в вариант, требующий
	# aware-время, VALID_EVENT стал бы битым и эти проверки упали бы.
	# Тип `datetime` для поля байт-в-байт закреплён причиной в сценарии
	# неразбираемой даты (main): иная формулировка или пустой лог тоже
	# роняют тест.
	stamp = datetime.fromisoformat(VALID_EVENT['occurred_at'])
	assert stamp.tzinfo is None, stamp


requests_seen: list[str] = []


async def _run_scenario(events: list[dict]) -> tuple[RecordingNotifier, LogCapture]:
	"""Общий пролог сценария: чистый журнал HTTP, сброшенные метрики, свежий конвейер."""
	requests_seen.clear()
	metrics.reset()
	return await run_pipeline(events, requests_seen=requests_seen)


async def _check_required_fields() -> None:
	# Строгая обязательность — через конвейер, без ссылок на схему: каждое
	# из пяти обязательных полей по очереди убирается из валидного payload'а
	# (один прогон на поле). Битое событие не доставляется, HTTP не тронут,
	# метка «битое» +1, в логе маркер `event_id` и причина с именем поля —
	# тот же внешний seam, что у сценария битого события ниже.
	for field in VALID_EVENT:
		broken = {key: value for key, value in VALID_EVENT.items() if key != field}
		sink, capture = await _run_scenario([broken])
		assert not sink.notifications, (field, sink.notifications)
		assert not requests_seen, (field, requests_seen)
		assert metrics.broken_events == 1, (field, metrics.broken_events)
		assert capture.records, (field, 'broken event must be logged')
		message = capture.records[0].getMessage()
		# `event_id` в логе: сам UUID, а для отсутствующего поля — явная
		# метка `<unknown>` (hdlr не может извлечь идентификатор из payload'а).
		logged_id = '<unknown>' if field == 'event_id' else VALID_EVENT['event_id']
		assert logged_id in message, (field, message)
		assert f'причина={field}: Field required' in message, (field, message)


async def _check_unknown_fields() -> None:
	# Неизвестные поля на уровне КОНВЕЙЕРА: отклонённое обогащение `title`,
	# лишнее поле и серверный `event_title` (ClassVar — само его появление
	# на проводе обязано быть неломающим: спека обещает, что обогащение
	# payload на сервере клиент переживёт). Публикуются настоящие байты:
	# ровно одно уведомление, ни одного «битого», ни одного предупреждения
	# в логе — проверяется поведение, а не устройство схемы (фиксация
	# неломающего изменения контракта).
	enriched = {
		**VALID_EVENT,
		'title': 'route 77',
		'extra_number': 1,
		'event_title': 'Server Title',
	}
	sink, capture = await _run_scenario([enriched])
	assert len(sink.notifications) == 1, sink.notifications
	assert metrics.broken_events == 0, metrics.broken_events
	assert not capture.records, capture.records


async def main() -> None:
	_check_wire_form()
	_check_naive_occurred_at()
	await _check_required_fields()

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

	await _check_unknown_fields()

	print('EVENT_CONTRACT_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
