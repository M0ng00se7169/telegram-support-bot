"""Offline smoke test for the composition root (ADR-0001, idiomatic dishka).

Runs the real Kafka subscriber end to end without any network:
- faststream TestKafkaBroker replaces the Kafka connection;
- httpx MockTransport serves canned API responses;
- PTB Bot lifecycle hooks and send_message are stubbed.

Run:  python tests/smoke_composition_root.py   (from the app/ directory)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio

import httpx
import telegram

_sent_messages = []


async def _stub(self) -> None:  # noqa: ANN001
	pass


async def _fake_send(self, **kwargs):  # noqa: ANN001
	# Real PTB sendMessage returns the delivered Message; the notifier uses
	# message_id to register the pair in the reply registry (card #1).
	_sent_messages.append(kwargs)
	return telegram.Message(
		message_id=len(_sent_messages),
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
from dishka.integrations.faststream import setup_dishka  # noqa: E402
from faststream import FastStream  # noqa: E402
from faststream.kafka import KafkaBroker, TestKafkaBroker  # noqa: E402
from httpx import AsyncClient, MockTransport, Response  # noqa: E402

from consumers.handlers import router  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402
from services.replies import (  # noqa: E402
	BaseReplyRegistry,
	InMemoryReplyRegistry,
)
from settings import get_settings  # noqa: E402


class OfflineProvider(Provider):
	# Card #1: the real DefaultProvider wires the Valkey-backed registry;
	# offline smoke must ride on the in-memory override instead.
	@provide(scope=Scope.APP)
	def get_reply_registry(self) -> BaseReplyRegistry:
		return InMemoryReplyRegistry()

	@provide(scope=Scope.REQUEST)
	def get_http_client(self) -> AsyncClient:
		def transport_handler(request: httpx.Request) -> Response:
			if request.url.path.endswith('/listeners/'):
				return Response(200, json=[{'oid': '42'}])
			return Response(
				200,
				json={
					'oid': 'chat-oid',
					'title': 'chat title',
					'created_at': '2026-01-01T00:00:00Z',
				},
			)

		return AsyncClient(transport=MockTransport(transport_handler))


async def main() -> None:
	settings = get_settings()
	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)

	container = make_async_container(DefaultProvider(), OfflineProvider())
	setup_dishka(container, app)

	async with TestKafkaBroker(broker) as test_broker:
		await test_broker.publish(
			{
				'event_id': 'e1',
				'occurred_at': '2026-10-06T00:00:00Z',
				'message_text': 'hello',
				'message_oid': 'm1',
				'chat_oid': 'chat-oid',
			},
			topic=settings.NEW_MESSAGE_TOPIC,
			key=b'chat-oid',
		)

	assert len(_sent_messages) == 1, f'expected 1 message, sent {_sent_messages}'
	assert _sent_messages[0]['chat_id'] == '42'
	assert '<blockquote>hello</blockquote>' in _sent_messages[0]['text']
	await container.close()
	print('INJECTION_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
