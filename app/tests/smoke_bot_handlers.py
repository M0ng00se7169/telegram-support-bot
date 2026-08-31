"""Offline smoke of bot handlers and their REQUEST-scoped Notifier wiring.

The real Dishka container and Telegram bot run against offline HTTP and Telegram
transports. Every user-facing handler message is observed at the outgoing
Telegram transport seam.

Run:  python tests/smoke_bot_handlers.py   (from the app/ directory)
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
from httpx import (  # noqa: E402
	AsyncClient,
	MockTransport,
	Response,
)
from markdown_v2 import assert_valid_markdownv2  # noqa: E402
from telegram_transport import (  # noqa: E402
	offline_telegram_provider,
	OfflineTelegramTransport,
)

import containers.factories as factories  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402
from exceptions.web import WebApiError  # noqa: E402


CHAT_OID = '3f2b1c8e-5a4d-4b7c-9e10-2d3f4a5b6c7d'
HOSTILE_TITLE = 'route 77 — problem (login) #1_ok'
ERROR_MESSAGE_ID = 67890


class _OfflineProvider(Provider):
	@provide(scope=Scope.REQUEST)
	def get_http_client(self) -> AsyncClient:
		def transport_handler(request: httpx.Request) -> Response:
			if request.url.path.endswith('/listeners/'):
				return Response(200, json=[{'oid': '42'}])
			if request.url.path == '/chat/':
				return Response(
					200,
					json={
						'items': [
							{
								'oid': CHAT_OID,
								'title': HOSTILE_TITLE,
								'created_at': '2026-01-01T00:00:00Z',
							},
						],
					},
				)
			return Response(200, json={})

		return AsyncClient(transport=MockTransport(transport_handler))


class _FakeChat:
	id = 12345


class _FakeUpdate:
	effective_chat = _FakeChat()


class _FakeContext:
	def __init__(self, application, args=None, error=None):
		self.application = application
		self.args = args or []
		self.error = error


def _assert_plain_message(sent: dict, text: str) -> None:
	assert sent['chat_id'] == 12345, sent
	assert sent['text'] == text, sent
	assert sent.get('parse_mode') is None, sent
	assert 'reply_to_message_id' not in sent, sent
	assert 'reply_parameters' not in sent, sent


async def main() -> None:
	transport = OfflineTelegramTransport()
	container = make_async_container(
		DefaultProvider(),
		_OfflineProvider(),
		offline_telegram_provider(transport),
	)
	factories._container = container

	try:
		from main import get_app

		application = get_app(request=transport)
		assert application.bot_data.get('container') is container
		context = _FakeContext(application)

		from handlers.base import start_handler
		from handlers.chats import (
			get_all_chats_handler,
			quit_chat,
			set_chat_listener,
			start_dialog,
		)
		from handlers.errors import error_handler
		from settings import get_settings

		# /chats: hostile Web API title remains valid MarkdownV2, with readable OID.
		await get_all_chats_handler(_FakeUpdate(), context)
		chat_list = transport.sent_messages[-1]
		message_text = chat_list['text']
		assert message_text.startswith('*List of all available chats:*'), message_text
		assert f'`{CHAT_OID}`' in message_text, message_text
		assert r'*route 77 — problem \(login\) \#1\_ok*' in message_text, message_text
		assert_valid_markdownv2(message_text)

		# /start greeting and chat/listener/reply acknowledgements are plain text.
		await start_handler(_FakeUpdate(), context)
		_assert_plain_message(
			transport.sent_messages[-1], get_settings().GREETINGS_TEXT
		)

		await set_chat_listener(
			_FakeUpdate(), _FakeContext(application, args=[CHAT_OID])
		)
		_assert_plain_message(transport.sent_messages[-1], 'You connected to the chat.')

		state = await start_dialog(_FakeUpdate(), context)
		from handlers.constants import SEND_MESSAGE_STATE

		assert state == SEND_MESSAGE_STATE
		_assert_plain_message(
			transport.sent_messages[-1],
			'Now you are answering questions. Please choose the message and reply on it.'
			'User will see your answer on the web site.',
		)

		await quit_chat(_FakeUpdate(), context)
		_assert_plain_message(
			transport.sent_messages[-1],
			'You quited from the dialogue with client',
		)

		# Message target is taken from effective_message, preserving reply behavior
		# even when PTB has no effective_chat for this update type.
		error_context = _FakeContext(
			application,
			error=WebApiError(
				method='GET',
				endpoint='/chat/',
				status_code=503,
				detail='temporarily unavailable',
			),
		)
		error_update = SimpleNamespace(
			effective_chat=None,
			effective_message=SimpleNamespace(
				chat=_FakeChat(),
				message_id=ERROR_MESSAGE_ID,
			),
		)
		await error_handler(error_update, error_context)
		error_message = transport.sent_messages[-1]
		assert error_message['chat_id'] == 12345, error_message
		assert error_message['text'] == (
			'Web API GET /chat/ failed with 503\ntemporarily unavailable'
		), error_message
		assert error_message.get('parse_mode') is None, error_message
		assert error_message.get('reply_parameters') == {
			'message_id': ERROR_MESSAGE_ID,
		}, error_message
	finally:
		await container.close()
		factories._container = None

	print('BOT_HANDLERS_OK')


if __name__ == '__main__':
	asyncio.run(main())
