"""Обработчик подписки new-message consumer'а (карточка №5).

Типы и валидация происходят на границе процесса: обработчик берёт **сырой payload**
(dict), валидирует его через `NewMessageEvent` и только потом передаёт
типизированный объект в бизнес-логику. Внутри бизнес-кода типы уже достоверные —
`event_id` это `UUID`, `occurred_at` — `datetime`, `chat_oid` — `str`.

Битое событие не падает consumer'ом: ошибка валидации ловится на одном сообщении,
считает метку «битое», логируется с `event_id` и причиной и **не пробрасывается
наружу**. Основание: relay на сервере переотправляет неограниченно и без dead-letter
queue, поэтому consumer, падающий на плохом payload, превращает одну плохую строку в
остановку всего топика. Событие без надёжной идентичности отправлять нельзя —
уведомление без адресата хуже, чем его отсутствие.

Доставка событий at-least-once, поэтому сразу после валидации событие помечается
как виденное (Event Deduplicator): повторная доставка того же `event_id`
завершается без единого побочного эффекта — ни HTTP-запроса, ни отправки в
Telegram, ни записи в Reply Registry.

Тем же роутером подписаны события жизненного цикла чата (создание/удаление) —
они поддерживают Chat Title Cache тёплым. Дедупликации они не требуют:
`remember`/`forget` идемпотентны при at-least-once доставке.
"""

from __future__ import annotations

import logging

from dishka.integrations.faststream import (
    FromDishka,
    inject,
)
from faststream.kafka import KafkaRouter
from settings import get_settings

from consumers.metrics import metrics
from consumers.schemas import (
    ChatDeletedEvent,
    NewChatCreatedEvent,
    NewMessageEvent,
    ValidationError,
)
from services.dedup import BaseEventDeduplicator
from services.notifier import (
    BaseNotifier,
    NewMessageNotification,
)
from services.title_cache import ChatTitleCache
from services.web import BaseChatDirectory


logger = logging.getLogger(__name__)

settings = get_settings()
router = KafkaRouter()


@router.subscriber(settings.NEW_MESSAGE_TOPIC, group_id=settings.KAFKA_GROUP_ID)
@inject
async def new_message_subscription_handler(
	raw_payload: dict,
	directory: FromDishka[BaseChatDirectory],
	notifier: FromDishka[BaseNotifier],
	deduplicator: FromDishka[BaseEventDeduplicator],
	cache: FromDishka[ChatTitleCache],
) -> None:
	# Валидация контракта — на границе, до любого побочного эффекта.
	try:
		event = NewMessageEvent.model_validate(raw_payload)
	except ValidationError as error:
		_record_broken_event(raw_payload, error, 'new-message')
		return

	# Дедупликация — сразу после валидации и строго до любого побочного эффекта:
	# до HTTP-запросов за chat info/listeners и до отправки в Telegram. Идентичность
	# берётся из `event_id` в payload, а не из Kafka Key (ADR-0002). Единственный
	# вызов `mark_if_new` = проверка + пометка атомарно; is_new — «событие новое».
	is_new = await deduplicator.mark_if_new(event.event_id)
	if not is_new:
		metrics.record_duplicate()
		logger.warning(
			'Дубликат new-message отсеяно: event_id=%s',
			event.event_id,
		)
		return

	# Реальный продюсер кладёт в key event_id, а не chat_oid: единственный надёжный
	# источник chat_oid — payload события.
	chat_oid = event.chat_oid

	# Заголовок — из Chat Title Cache: тёплый кэш отдаёт его без HTTP, промах —
	# точечный read-through с дозаписью. `None` означает tombstone (чат
	# удалён): уведомление пропускается целиком, ДО запроса listener'ов.
	title = await cache.get_title(chat_oid=chat_oid, directory=directory)
	if title is None:
		logger.warning('Уведомление пропущено: чат удалён, chat_oid=%s', chat_oid)
		return

	# Chat Listener'ы остаются в горячем пути: подписки меняются динамично,
	# актуальность важнее кэша — на событие ровно один HTTP-запрос, за ними.
	listeners = await directory.get_chat_listeners(chat_oid=chat_oid)
	notification = NewMessageNotification(
		chat_oid=chat_oid,
		title=title,
		text=event.message_text,
	)

	for listener in listeners:
		# Формат уведомления, parse_mode и запись в Reply Registry — забота
		# Notifier'а; здесь остаётся только бизнес-факт: кому и о чём сообщать.
		await notifier.notify_new_message(
			tg_chat_id=listener.oid,
			notification=notification,
		)


@router.subscriber(
	settings.NEW_CHATS_TOPIC,
	group_id=settings.CHAT_EVENTS_GROUP_ID,
)
@inject
async def chat_created_subscription_handler(
	raw_payload: dict,
	cache: FromDishka[ChatTitleCache],
) -> None:
	# Валидация на границе — как у new-message: битое событие логируется
	# с причиной и не пробрасывается, чтобы одна плохая строка не остановила
	# подписку на события чатов.
	try:
		event = NewChatCreatedEvent.model_validate(raw_payload)
	except ValidationError as error:
		_record_broken_event(raw_payload, error, 'chat-created')
		return

	# Дедупликации здесь нет нарочно: remember/forget идемпотентны при
	# at-least-once — повторная доставка просто перезаписывает тот же
	# заголовок, наружу побочных эффектов нет.
	cache.remember(event.chat_oid, event.chat_title)


@router.subscriber(
	settings.CHAT_DELETED_TOPIC,
	group_id=settings.CHAT_EVENTS_GROUP_ID,
)
@inject
async def chat_deleted_subscription_handler(
	raw_payload: dict,
	cache: FromDishka[ChatTitleCache],
) -> None:
	try:
		event = ChatDeletedEvent.model_validate(raw_payload)
	except ValidationError as error:
		_record_broken_event(raw_payload, error, 'chat-deleted')
		return

	# См. комментарий выше: tombstone ставится идемпотентно, дедуп не нужна.
	cache.forget(event.chat_oid)


def _record_broken_event(
	raw_payload: dict,
	error: ValidationError,
	stream: str,
) -> None:
	"""Считает метку «битое» и логирует битый payload — одна форма на все потоки.

	`stream` — человекочитаемое имя потока (new-message/chat-created/
	chat-deleted), вставляется в шаблон сообщения; сама формулировка
	зафиксирована смоуками и обязана сохраняться байт-в-байт. Хелпер делает
	ровно record + log и ничего больше: `return` из обработчика и его место
	в контроле потока не меняются.
	"""
	metrics.record_broken()
	logger.warning(
		'Битое событие %s отсеяно: event_id=%s причина=%s',
		stream,
		_extract_event_id(raw_payload),
		_format_validation_reason(error),
	)


def _extract_event_id(raw_payload: object) -> str:
	"""Извлекает `event_id` из сырых данных для логирования битого события.

	`event_id` может отсутствовать или иметь неверный тип — тогда идентификатор
	неизвестен, и логирование отмечает это явно.
	"""
	if isinstance(raw_payload, dict):
		event_id = raw_payload.get('event_id')
		if event_id is not None:
			return str(event_id)
	return '<unknown>'


def _format_validation_reason(error: ValidationError) -> str:
	"""Формирует читаемую причину валидации: поле и тип ошибки."""
	reasons = [
		f'{".".join(str(part) for part in err["loc"])}: {err["msg"]}'
		for err in error.errors()
	]
	return '; '.join(reasons)
