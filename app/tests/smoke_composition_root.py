"""Offline smoke test for the composition root (ADR-0001, idiomatic dishka).

Runs the real Kafka subscriber end to end without any network:
- faststream TestKafkaBroker replaces the Kafka connection;
- httpx MockTransport serves canned API responses;
- an in-memory BaseNotifier records the consumer's business event.

Run:  python tests/smoke_composition_root.py   (from the app/ directory)
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
from settings import get_settings  # noqa: E402

from consumers.handlers import router  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402
from notifier_fakes import (  # noqa: E402
	RecordingNotifier,
	notifier_override_provider,
)
from services.replies import (  # noqa: E402
	BaseReplyRegistry,
	InMemoryReplyRegistry,
)


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
	sink = RecordingNotifier()

	broker = KafkaBroker(settings.KAFKA_BROKER_URL)
	broker.include_router(router=router)
	app = FastStream(broker=broker)

	container = make_async_container(
		DefaultProvider(),
		OfflineProvider(),
		notifier_override_provider(sink),
	)
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

	assert len(sink.notifications) == 1, sink.notifications
	tg_chat_id, notification = sink.notifications[0]
	assert tg_chat_id == '42', sink.notifications[0]
	assert notification.chat_oid == 'chat-oid', sink.notifications[0]
	assert notification.title == 'chat title', sink.notifications[0]
	assert notification.text == 'hello', sink.notifications[0]
	await container.close()
	print('INJECTION_SMOKE_OK')


if __name__ == '__main__':
	asyncio.run(main())
