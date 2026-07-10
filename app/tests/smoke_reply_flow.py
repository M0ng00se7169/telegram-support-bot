"""Offline end-to-end smoke of the Manager Reply flow (card #1).

Chain under test, all offline (TestKafkaBroker + MockTransport, PTB stubs):
1. consumer side — real Kafka pipeline: TestKafkaBroker delivers the event,
   DishkaMiddleware opens the request scope, the real subscriber sends the
   notification (send_message stubbed) and registers
   (tg_message_id -> chat_oid) in the registry override;
2. bot side — real ConversationHandler callback `send_message_to_chat`
   resolves chat_oid through _request_scope + BaseReplyRegistry;
   no regex anywhere.

Run:  python tests/smoke_reply_flow.py   (from the app/ directory)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os

os.environ.setdefault('TG_BOT_TOKEN', '123456:TEST-TOKEN')

import asyncio  # noqa: E402

import httpx  # noqa: E402
import telegram  # noqa: E402

from faststream import FastStream  # noqa: E402
from faststream.kafka import KafkaBroker, TestKafkaBroker  # noqa: E402
from dishka.integrations.faststream import setup_dishka  # noqa: E402

_sent_messages = []
_counter = {'n': 0}


async def _stub(self) -> None:  # noqa: ANN001
	pass


async def _fake_send(self, **kwargs):  # noqa: ANN001
	_counter['n'] += 1
	_sent_messages.append(kwargs)
	return telegram.Message(
		message_id=_counter['n'],
		date=None,
		chat=telegram.Chat(id=1, type='private'),
	)


telegram.Bot.initialize = _stub
telegram.Bot.shutdown = _stub
telegram.Bot.send_message = _fake_send

from dishka import (  # noqa: E402
	Provider,
	Scope,
	make_async_container,
	provide,
)
from httpx import AsyncClient, MockTransport, Response  # noqa: E402

import containers.factories as factories  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402
from consumers.handlers import router  # noqa: E402
from services.replies import (  # noqa: E402
	BaseReplyRegistry,
	InMemoryReplyRegistry,
)
from settings import get_settings  # noqa: E402

REGISTRY_OVERRIDE_VALUE = InMemoryReplyRegistry()
CHAT_OID = 'chat-oid-77'


class _OfflineProvider(Provider):
	@provide(scope=Scope.REQUEST)
	def get_http_client(self) -> AsyncClient:
		chat = {
			'oid': CHAT_OID,
			'title': 'route 77',
			'created_at': '2026-01-01T00:00:00Z',
		}

		def transport_handler(request: httpx.Request) -> Response:
			path = request.url.path
			if path.endswith('/listeners/'):
				return Response(200, json=[{'oid': '12345'}])
			return Response(200, json=chat)

		return AsyncClient(transport=MockTransport(transport_handler))

	@provide(scope=Scope.APP)
	def get_reply_registry(self) -> BaseReplyRegistry:
		return REGISTRY_OVERRIDE_VALUE


class _FakeContext:
	def __init__(self, application) -> None:
		self.bot = application.bot
		self.application = application


def _message(message_id: int) -> telegram.Message:
	return telegram.Message(
		message_id=message_id,
		date=None,
		chat=telegram.Chat(id=1, type='private'),
	)


class _FakeChat:
	id = 12345


class _FakeUpdate:
	effective_chat = _FakeChat()

	def __init__(self, reply_to) -> None:
		self.message = telegram.Message(
			message_id=99_999,
			date=None,
			chat=telegram.Chat(id=12345, type='private'),
			reply_to_message=reply_to,
		)


async def main() -> None:
	settings = get_settings()
	container = make_async_container(DefaultProvider(), _OfflineProvider())
	factories._container = container

	from main import get_app

	application = get_app()
	context = _FakeContext(application)

	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)
	setup_dishka(container=container, app=app)

	# --- Consumer side: notification + registry write. ---
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

	notification = _sent_messages[-1]
	assert notification['chat_id'] == '12345'
	assert '<blockquote>hello from route 77</blockquote>' in notification['text']
	assert CHAT_OID not in notification['text'], 'oid marker must be gone'
	notification_message_id = _counter['n']

	registry_record = await REGISTRY_OVERRIDE_VALUE.resolve(notification_message_id)
	assert registry_record == CHAT_OID, registry_record

	# --- Bot side: manager replies to the TG notification. ---
	from handlers.chats import send_message_to_chat

	await send_message_to_chat(_FakeUpdate(reply_to=None), context)
	assert 'choose the message' in _sent_messages[-1]['text']

	await send_message_to_chat(
		_FakeUpdate(reply_to=_message(notification_message_id)), context
	)
	ack = _sent_messages[-1]
	assert ack['chat_id'] == 12345
	assert CHAT_OID in ack['text'], ack

	# --- Reply target unknown to the registry (expired / foreign). ---
	await send_message_to_chat(_FakeUpdate(reply_to=_message(424_242)), context)
	warning = _sent_messages[-1]
	assert warning['text'].startswith('Please reply to a chat notification'), warning

	await container.close()
	print('REPLY_FLOW_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
