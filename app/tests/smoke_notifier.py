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

Run:  python tests/smoke_notifier.py   (from the app/ directory)
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import asyncio  # noqa: E402
from collections.abc import Sequence  # noqa: E402
from dataclasses import (  # noqa: E402
	dataclass,
	field,
)

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
from settings import get_settings  # noqa: E402
from telegram_transport import (  # noqa: E402
	offline_telegram_provider,
	OfflineTelegramTransport,
)

from containers.providers import DefaultProvider  # noqa: E402
from dtos.messages import ChatListItemDTO  # noqa: E402
from services.notifier import BaseNotifier  # noqa: E402
from services.replies import (  # noqa: E402
	BaseReplyRegistry,
	InMemoryReplyRegistry,
)


CHAT_OID = 'chat-oid-9c'
# Заголовок и текст набиты спецсимволами HTML (<, >, &) - до билета Telegram
# отвечал BadRequest "Can't parse entities", потому что заголовок не экранировался.
HOSTILE_TITLE = 'Отдел «Продажи» <MVP> & Co — отчёт #1.2'
HOSTILE_TEXT = 'клиент пишет: 5 < 7 && 7 > 5, "цитаты" тоже'

_EVENT = {
	'event_id': 'e-notifier',
	'occurred_at': '2026-10-07T00:00:00Z',
	'message_text': HOSTILE_TEXT,
	'message_oid': 'm-notifier',
	'chat_oid': CHAT_OID,
}


@dataclass
class _NewMessageCall:
	"""Что бизнес-операция «уведомить о новом сообщении» получила снаружи."""

	tg_chat_id: str
	chat_oid: str
	title: str
	text: str


@dataclass
class _RecordingNotifier(BaseNotifier):
	"""Подменённый Notifier: in-memory sink вместо Telegram."""

	calls: list[_NewMessageCall] = field(default_factory=list)

	async def notify_new_message(
		self,
		tg_chat_id: str,
		chat_oid: str,
		title: str,
		text: str,
	) -> None:
		self.calls.append(
			_NewMessageCall(
				tg_chat_id=tg_chat_id,
				chat_oid=chat_oid,
				title=title,
				text=text,
			),
		)

	async def notify_chat_list(
		self,
		tg_chat_id: int,
		chats: Sequence[ChatListItemDTO],
	) -> None:
		raise AssertionError('операция «показать список чатов» вне этого билета')

	async def acknowledge(self, tg_chat_id: int, text: str) -> None:
		raise AssertionError('операция «подтвердить действие» вне этого билета')


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
	sink = _RecordingNotifier()

	class _NotifierOverrideProvider(Provider):
		@provide(scope=Scope.APP)
		def get_reply_registry(self) -> BaseReplyRegistry:
			return InMemoryReplyRegistry()

		@provide(scope=Scope.APP)
		def get_notifier(self) -> BaseNotifier:
			return sink

	container = make_async_container(
		DefaultProvider(),
		_http_provider(chat_payload, listener_oid),
		_NotifierOverrideProvider(),
		offline_telegram_provider(OfflineTelegramTransport()),
	)

	await _publish_event(container)

	assert len(sink.calls) == 1, f'expected 1 notification, got {sink.calls}'
	call = sink.calls[0]
	assert call.tg_chat_id == listener_oid, call
	assert call.title == HOSTILE_TITLE, call
	assert call.text == HOSTILE_TEXT, call
	assert call.chat_oid == CHAT_OID, 'реестр получает oid из события, не из key'

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

	print('NOTIFIER_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
