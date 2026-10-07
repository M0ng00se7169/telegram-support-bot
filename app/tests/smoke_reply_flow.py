"""Offline smoke of the consumer event and Manager Reply flow (card #1).

The consumer side uses TestKafkaBroker + MockTransport + an in-memory notifier.
The manager side calls the real reply handler with an explicit registry fixture;
the fake notifier does not create Reply Registry records.

Run:  python tests/smoke_reply_flow.py   (from the app/ directory)
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import os


os.environ.setdefault('TG_BOT_TOKEN', '123456:TEST-TOKEN')

import asyncio  # noqa: E402
from types import SimpleNamespace  # noqa: E402

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

from consumers.handlers import router  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402
from notifier_fakes import (  # noqa: E402
	RecordingNotifier,
	assert_single_notification,
	notifier_override_provider,
)
from services.notifier import NewMessageNotification  # noqa: E402
from services.replies import (  # noqa: E402
	BaseReplyRegistry,
	InMemoryReplyRegistry,
)


CHAT_OID = 'chat-oid-77'
MANAGER_TG_CHAT_ID = 12345
NOTIFICATION_MESSAGE_ID = 777_001
REGISTRY_OVERRIDE_VALUE = InMemoryReplyRegistry()


class _OfflineProvider(Provider):
	@provide(scope=Scope.REQUEST)
	def get_http_client(self) -> AsyncClient:
		chat = {
			'oid': CHAT_OID,
			'title': 'route 77',
			'created_at': '2026-01-01T00:00:00Z',
		}

		def transport_handler(request: httpx.Request) -> Response:
			if request.url.path.endswith('/listeners/'):
				return Response(200, json=[{'oid': '12345'}])
			return Response(200, json=chat)

		return AsyncClient(transport=MockTransport(transport_handler))

	@provide(scope=Scope.APP)
	def get_reply_registry(self) -> BaseReplyRegistry:
		return REGISTRY_OVERRIDE_VALUE


class _FakeContext:
	def __init__(self, container) -> None:
		self.application = SimpleNamespace(bot_data={'container': container})


class _FakeUpdate:
	effective_chat = SimpleNamespace(id=MANAGER_TG_CHAT_ID)

	def __init__(self, reply_to_message_id: int | None) -> None:
		reply_to_message = None
		if reply_to_message_id is not None:
			reply_to_message = SimpleNamespace(message_id=reply_to_message_id)
		self.message = SimpleNamespace(reply_to_message=reply_to_message)


async def main() -> None:
	settings = get_settings()
	sink = RecordingNotifier()

	container = make_async_container(
		DefaultProvider(),
		_OfflineProvider(),
		notifier_override_provider(sink),
	)

	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)
	setup_dishka(container=container, app=app)

	async with TestKafkaBroker(broker) as test_broker:
		await test_broker.publish(
			{
				'event_id': 'e1',
				'occurred_at': '2026-10-07T00:00:00Z',
				'message_text': 'hello from route 77',
				'message_oid': 'm1',
				'chat_oid': CHAT_OID,
			},
			topic=settings.NEW_MESSAGE_TOPIC,
			key=CHAT_OID.encode(),
		)

	assert_single_notification(
		sink,
		expected_tg_chat_id='12345',
		expected_notification=NewMessageNotification(
			chat_oid=CHAT_OID,
			title='route 77',
			text='hello from route 77',
		),
		context='reply-flow consumer notification',
	)
	assert not sink.acknowledgements, sink.acknowledgements

	# Fixture the successful delivery mapping explicitly: a fake notifier has no
	# Telegram message id and must not be credited with writing this registry pair.
	await REGISTRY_OVERRIDE_VALUE.remember(
		message_id=NOTIFICATION_MESSAGE_ID,
		chat_oid=CHAT_OID,
	)

	from handlers.chats import send_message_to_chat

	context = _FakeContext(container)
	await send_message_to_chat(_FakeUpdate(reply_to_message_id=None), context)
	missing_reply_ack = sink.acknowledgements[-1]
	assert missing_reply_ack.tg_chat_id == MANAGER_TG_CHAT_ID
	assert (
		missing_reply_ack.text == 'Error! Please choose the message you want to reply.'
	)
	assert missing_reply_ack.reply_to_message_id is None

	await send_message_to_chat(
		_FakeUpdate(reply_to_message_id=NOTIFICATION_MESSAGE_ID),
		context,
	)
	accepted_ack = sink.acknowledgements[-1]
	assert accepted_ack.tg_chat_id == MANAGER_TG_CHAT_ID
	assert accepted_ack.text == f'Reply accepted for chat {CHAT_OID}.'
	assert accepted_ack.reply_to_message_id is None

	await send_message_to_chat(_FakeUpdate(reply_to_message_id=424_242), context)
	unknown_reply_ack = sink.acknowledgements[-1]
	assert unknown_reply_ack.tg_chat_id == MANAGER_TG_CHAT_ID
	assert unknown_reply_ack.text.startswith(
		'Please reply to a chat notification (or the record has expired).',
	)
	assert unknown_reply_ack.reply_to_message_id is None

	await container.close()
	print('REPLY_FLOW_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
