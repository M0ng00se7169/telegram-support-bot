"""Offline smoke: bot handlers resolve services through the PTB bot_data container.

Covers the composition-root wiring that the Kafka smoke does not:
- get_app() must store the process container in application.bot_data['container'];
- a PTB handler opens a REQUEST scope via handlers.chats._request_scope and
  resolves BaseChatDirectory from it;
- no network: httpx MockTransport serves canned API responses, PTB Bot
  lifecycle hooks and send_message are stubbed.

Run:  python tests/smoke_bot_handlers.py   (from the app/ directory)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os

os.environ.setdefault('TG_BOT_TOKEN', '123456:TEST-TOKEN')

import asyncio  # noqa: E402

import httpx  # noqa: E402
import telegram  # noqa: E402

_sent_messages = []


async def _stub(self) -> None:  # noqa: ANN001
	pass


async def _fake_send(self, **kwargs):  # noqa: ANN001
	_sent_messages.append(kwargs)


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


class _OfflineProvider(Provider):
	@provide(scope=Scope.REQUEST)
	def get_http_client(self) -> AsyncClient:
		def transport_handler(request: httpx.Request) -> Response:
			path = request.url.path
			chat = {
				'oid': 'chat-oid',
				'title': 'chat title',
				'created_at': '2026-01-01T00:00:00Z',
			}
			if path.endswith('/listeners/'):
				return Response(200, json=[{'oid': '42'}])
			if path == '/chat/':
				return Response(200, json={'items': [chat]})
			return Response(200, json=chat)

		return AsyncClient(transport=MockTransport(transport_handler))


class _FakeChat:
	id = 12345


class _FakeUpdate:
	effective_chat = _FakeChat()


class _FakeContext:
	def __init__(self, application):
		self.bot = application.bot
		self.application = application


async def main() -> None:
	# Pre-seed the process-singleton container so that get_app() composes
	# exactly what the bot handlers will see at runtime.
	factories._container = make_async_container(DefaultProvider(), _OfflineProvider())

	from main import get_app

	application = get_app()

	container = application.bot_data.get('container')
	assert container is not None, 'bot_data["container"] must be set by get_app()'
	assert container is factories._container, 'bot_data must hold the process container'

	from handlers.chats import get_all_chats_handler

	fake_context = _FakeContext(application)

	await get_all_chats_handler(_FakeUpdate(), fake_context)

	assert len(_sent_messages) == 1, f'expected 1 message, sent {_sent_messages}'
	assert _sent_messages[0]['chat_id'] == 12345
	message_text = _sent_messages[0]['text']
	assert message_text.startswith('List of all available chats:'), message_text
	assert 'ChatOID: `chat-oid`' in message_text, message_text

	await factories._container.close()
	print('BOT_HANDLERS_OK')


if __name__ == '__main__':
	asyncio.run(main())
