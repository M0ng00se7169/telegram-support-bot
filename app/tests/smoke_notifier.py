"""Offline smoke of the Notifier gate (card #3, ticket 02).

Two independent seams, both offline (no Telegram API, no Kafka, no Web API):

1. Kafka -> Notifier. The real consumer pipeline (TestKafkaBroker) runs against
   a Notifier override that only records what it was told. The assertion is the
   business triple: chat, title, message text. Nothing about parse_mode, markup
   or `telegram.Bot` is visible to the consumer here.
2. The real `TelegramNotifier` through the Telegram transport seam
   (tests/telegram_transport.py): what actually goes to Telegram is escaped per
   the format chosen inside the module, and the (tg_message_id -> chat_oid) pair
   lands in the Reply Registry after a successful send.
3. The bot-side operations of the same module: the chat list is rendered and
   escaped as MarkdownV2 inside the module (a title full of reserved characters
   cannot break it, and the chat oid stays readable inside a code span), while
   a confirmation goes out as plain text with no parse_mode at all.

Run:  python tests/smoke_notifier.py   (from the app/ directory)
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import asyncio  # noqa: E402

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
from markdown_v2 import assert_valid_markdownv2  # noqa: E402
from notifier_fakes import (  # noqa: E402
    assert_single_notification,
    notifier_override_provider,
    RecordingNotifier,
)
from settings import get_settings  # noqa: E402
from telegram_transport import (  # noqa: E402
    offline_telegram_provider,
    OfflineTelegramTransport,
)

from containers.providers import DefaultProvider  # noqa: E402
from dtos.messages import ChatListItemDTO  # noqa: E402
from services.notifier import (  # noqa: E402
    BaseNotifier,
    NewMessageNotification,
)
from services.replies import (  # noqa: E402
    BaseReplyRegistry,
    InMemoryReplyRegistry,
)


CHAT_OID = 'chat-oid-9c'
# Заголовок и текст набиты спецсимволами HTML (<, >, &) - до билета Telegram
# отвечал BadRequest "Can't parse entities", потому что заголовок не экранировался.
HOSTILE_TITLE = 'Отдел «Продажи» <MVP> & Co — отчёт #1.2'
HOSTILE_TEXT = 'клиент пишет: 5 < 7 && 7 > 5, "цитаты" тоже'

# Для MarkdownV2 набор зарезервированных символов другой: \_*[]()~`>#+-=|{}.!
# (у HTML-символов `&` среди них нет). Ожидаемое сообщение выписано руками по
# спецификации Telegram, а не пересчитано тем же хелпером, что и реализация.
MARKDOWN_HOSTILE_TITLE = r'a&b\_c*d[e]f(g)h~i`j>k#l+m-n=o|p{q}r.s!t'
MARKDOWN_ESCAPED_TITLE = r'a&b\\\_c\*d\[e\]f\(g\)h\~i\`j\>k\#l\+m\-n\=o\|p\{q\}r\.s\!t'
MARKDOWN_CHAT_OID = '3f2b1c8e-5a4d-4b7c-9e10-2d3f4a5b6c7d'
# Внутри `code` зарезервированы только обратный слэш и кавычка, поэтому дефисы и
# точка UUID остаются читаемыми, а кавычка экранируется.
MARKDOWN_ODD_OID = r'a\b`c-d.e'
MARKDOWN_ODD_OID_ESCAPED = r'a\\b\`c-d.e'

_EVENT = {
    'event_id': '12345678-1234-5678-1234-567812345678',
    'occurred_at': '2026-10-07T00:00:00',
    'message_text': HOSTILE_TEXT,
    'message_oid': 'm-notifier',
    'chat_oid': CHAT_OID,
}


def _http_provider(chat_payload: dict, listener_oid: str) -> Provider:
	class _WebApiProvider(Provider):
		@provide(scope=Scope.REQUEST)
		def get_http_client(self) -> AsyncClient:
			def transport_handler(request: httpx.Request) -> Response:
				if request.url.path.endswith('/listeners/'):
					return Response(200, json=[{'oid': listener_oid}])
				return Response(200, json=chat_payload)

			return AsyncClient(transport=MockTransport(transport_handler))

	return _WebApiProvider()


async def _publish_event(container) -> None:
	"""Прогоняет событие через настоящий consumer-пайплайн Kafka."""
	settings = get_settings()
	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	from consumers.handlers import router

	broker.include_router(router=router)
	app = FastStream(broker=broker)
	setup_dishka(container, app)

	async with TestKafkaBroker(broker) as test_broker:
		await test_broker.publish(
			_EVENT,
			topic=settings.NEW_MESSAGE_TOPIC,
			key=CHAT_OID.encode(),
		)


async def _smoke_kafka_to_notifier(chat_payload: dict, listener_oid: str) -> None:
	"""Шов 1: событие доходит до Notifier'а ровно тройкой бизнес-полей."""
	sink = RecordingNotifier()

	class _NotifierOverrideProvider(Provider):
		@provide(scope=Scope.APP)
		def get_reply_registry(self) -> BaseReplyRegistry:
			return InMemoryReplyRegistry()

	container = make_async_container(
		DefaultProvider(),
		_http_provider(chat_payload, listener_oid),
		_NotifierOverrideProvider(),
		notifier_override_provider(sink),
		offline_telegram_provider(OfflineTelegramTransport()),
	)

	await _publish_event(container)

	assert_single_notification(
		sink,
		expected_tg_chat_id=listener_oid,
		expected_notification=NewMessageNotification(
			chat_oid=CHAT_OID,
			title=HOSTILE_TITLE,
			text=HOSTILE_TEXT,
		),
		context='Kafka → Notifier (chat_oid из payload события, не из Kafka key)',
	)

	await container.close()


async def _smoke_real_notifier_over_transport(
	chat_payload: dict,
	listener_oid: str,
	transport: OfflineTelegramTransport,
	registry: BaseReplyRegistry,
) -> None:
	"""Шов 2: настоящая реализация - что уходит в Telegram и что в реестр."""

	class _RegistryOverrideProvider(Provider):
		@provide(scope=Scope.APP)
		def get_reply_registry(self) -> BaseReplyRegistry:
			return registry

	container = make_async_container(
		DefaultProvider(),
		_http_provider(chat_payload, listener_oid),
		_RegistryOverrideProvider(),
		offline_telegram_provider(transport),
	)

	await _publish_event(container)

	assert len(transport.sent_messages) == 1, transport.sent_messages
	sent = transport.sent_messages[0]
	assert sent['chat_id'] == listener_oid, sent

	# Формат выбран внутри модуля, поэтому и порядок полей, и экранирование -
	# ожидаемое поведение чёрного ящика, а не деталь реализации.
	assert sent['parse_mode'] == 'HTML', sent
	assert sent['text'] == (
		'New message in chat <b>Отдел «Продажи» &lt;MVP&gt; &amp; Co — отчёт #1.2</b>'
		'<blockquote>клиент пишет: 5 &lt; 7 &amp;&amp; 7 &gt; 5, "цитаты" тоже</blockquote>'
	), sent['text']

	# Пара (TG-уведомление -> chat_oid) рождается внутри модуля отправки.
	message_id = transport.sent_calls[-1].result['message_id']
	assert await registry.resolve(message_id) == CHAT_OID, message_id

	await container.close()


async def _bot_side_notifier():
	"""Настоящий Notifier, говорящий через офлайн-транспорт Telegram."""
	transport = OfflineTelegramTransport()
	container = make_async_container(
		DefaultProvider(),
		offline_telegram_provider(transport),
	)
	notifier = await container.get(BaseNotifier)
	return notifier, transport, container


async def _smoke_chat_list_over_transport() -> None:
	"""Список чатов: MarkdownV2 собирается и экранируется внутри модуля."""
	notifier, transport, container = await _bot_side_notifier()

	try:
		chats = [
			ChatListItemDTO(
				oid=MARKDOWN_CHAT_OID,
				title=MARKDOWN_HOSTILE_TITLE,
				created_at='2026-01-01T00:00:00Z',
			),
			ChatListItemDTO(
				oid=MARKDOWN_ODD_OID,
				title='(пустой) заголовок',
				created_at='2026-01-01T00:00:00Z',
			),
		]
		await notifier.notify_chat_list(tg_chat_id=12345, chats=chats)

		assert len(transport.sent_messages) == 1, transport.sent_messages
		sent = transport.sent_messages[0]
		assert sent['chat_id'] == 12345, sent
		assert sent['parse_mode'] == 'MarkdownV2', sent

		# Список чатов больше не собирается хендлером: модуль отдал готовый текст.
		assert sent['text'] == (
			'*List of all available chats:*\n'
			f'ChatOID: `{MARKDOWN_CHAT_OID}`\n'
			f'Problem: *{MARKDOWN_ESCAPED_TITLE}*\n'
			f'ChatOID: `{MARKDOWN_ODD_OID_ESCAPED}`\n'
			'Problem: *\\(пустой\\) заголовок*'
		), sent['text']

		# Худший случай: заголовок со спецсимволами не должен ронять команду.
		assert_valid_markdownv2(sent['text'])
	finally:
		await container.close()

	# Пустой каталог - тоже валидное сообщение, а не пустая отправка.
	empty_notifier, empty_transport, empty_container = await _bot_side_notifier()
	try:
		await empty_notifier.notify_chat_list(tg_chat_id=12345, chats=[])
		assert empty_transport.sent_messages[0]['text'] == (
			'*List of all available chats:*\n_No chats yet\\._'
		), empty_transport.sent_messages[0]['text']
		assert_valid_markdownv2(empty_transport.sent_messages[0]['text'])
	finally:
		await empty_container.close()


async def _smoke_acknowledge_over_transport() -> None:
	"""Подтверждение менеджеру - обычный текст: разметки в нём нет."""
	notifier, transport, container = await _bot_side_notifier()

	try:
		# chat_oid в тексте подтверждения остаётся как есть: без parse_mode экранирование
		# не нужно, а с ним символы UUID превратились бы в слэши в тексте у менеджера.
		await notifier.acknowledge(
			tg_chat_id=12345,
			text=f'Reply accepted for chat {MARKDOWN_CHAT_OID}.',
		)

		assert len(transport.sent_messages) == 1, transport.sent_messages
		sent = transport.sent_messages[0]
		assert sent['chat_id'] == 12345, sent
		assert sent['text'] == f'Reply accepted for chat {MARKDOWN_CHAT_OID}.', sent
		assert sent.get('parse_mode') is None, sent
	finally:
		await container.close()


async def main() -> None:
	chat_payload = {
		'oid': CHAT_OID,
		'title': HOSTILE_TITLE,
		'created_at': '2026-01-01T00:00:00Z',
	}

	await _smoke_kafka_to_notifier(chat_payload, listener_oid='4242')
	await _smoke_real_notifier_over_transport(
		chat_payload,
		listener_oid='5555',
		transport=OfflineTelegramTransport(),
		registry=InMemoryReplyRegistry(),
	)
	await _smoke_chat_list_over_transport()
	await _smoke_acknowledge_over_transport()

	print('NOTIFIER_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
