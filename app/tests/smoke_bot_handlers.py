"""Offline smoke: bot handlers resolve services through the PTB bot_data container.

Covers the composition-root wiring that the Kafka smoke does not:
- get_app() must store the process container in application.bot_data['container'];
- a PTB handler opens a REQUEST scope via handlers.chats._request_scope and
  resolves BaseChatDirectory from it;
- no network: httpx MockTransport serves canned API responses, настоящий
  PTB Bot говорит через заглушку транспорта Telegram (tests/telegram_transport.py).

Run:  python tests/smoke_bot_handlers.py   (from the app/ directory)
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import os
import re


os.environ.setdefault('TG_BOT_TOKEN', '123456:TEST-TOKEN')

import asyncio  # noqa: E402

import httpx  # noqa: E402
from telegram_transport import OfflineTelegramTransport  # noqa: E402


# Символы, которые Telegram считает зарезервированными в MarkdownV2.
# Ищем незаэкранированные вхождения: перед символом не должно быть слэша.
_UNESCAPED_RESERVED = re.compile(r'(?<!\\)[_*\[\]()~`>#+\-=|{}.!]')
_CODE_SPAN = re.compile(r'`(?:\\.|[^`\\])*`')
# Наша собственная разметка: `*bold*`, `*italic*`, `_italic_`.
# Данные экранируются, поэтому неэкранированные `*` и `_` — только маркеры.
_OWN_MARKUP = re.compile(r'(?<!\\)[*_]')


def assert_valid_markdownv2(text: str) -> None:
	"""Telegram отклоняет сообщение с незаэкранированными зарезервированными
	символами MarkdownV2. PTB v21 не умеет парсить entities локально, поэтому
	проверяем инвариант вручную:

	- внутри `code` зарезервированы только слэш и кавычка — вырезаем такие
	  фрагменты целиком, они не должны валить сообщение;
	- неэкранированные `*` и `_` вне `code` — это наша разметка, убираем её;
	- всё остальное обязано быть экранировано.
	"""
	without_code = _CODE_SPAN.sub('', text)
	without_markup = _OWN_MARKUP.sub('', without_code)
	assert not _UNESCAPED_RESERVED.findall(
		without_markup,
	), f'unescaped MarkdownV2 reserved chars in {without_markup!r} (full: {text!r})'


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

import containers.factories as factories  # noqa: E402
from containers.providers import DefaultProvider  # noqa: E402


class _OfflineProvider(Provider):
	@provide(scope=Scope.REQUEST)
	def get_http_client(self) -> AsyncClient:
		def transport_handler(request: httpx.Request) -> Response:
			path = request.url.path
			chat = {
				'oid': '3f2b1c8e-5a4d-4b7c-9e10-2d3f4a5b6c7d',
				'title': 'route 77 — problem (login) #1_ok',
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
	transport = OfflineTelegramTransport()

	# Pre-seed the process-singleton container so that get_app() composes
	# exactly what the bot handlers will see at runtime.
	factories._container = make_async_container(DefaultProvider(), _OfflineProvider())

	from main import get_app

	application = get_app(request=transport)

	container = application.bot_data.get('container')
	assert container is not None, 'bot_data["container"] must be set by get_app()'
	assert container is factories._container, 'bot_data must hold the process container'

	from handlers.chats import get_all_chats_handler

	fake_context = _FakeContext(application)

	await get_all_chats_handler(_FakeUpdate(), fake_context)

	sent_messages = transport.sent_messages
	assert len(sent_messages) == 1, f'expected 1 message, sent {sent_messages}'
	assert sent_messages[0]['chat_id'] == 12345
	assert sent_messages[0]['parse_mode'] == 'MarkdownV2', sent_messages[0]
	message_text = sent_messages[0]['text']
	assert message_text.startswith('*List of all available chats:*'), message_text

	# Регрессия: oid и title приходят из Web API и содержат символы,
	# зарезервированные MarkdownV2 ('-', '.', '(', ')', '#', '_', '!'),
	# из-за чего Telegram отвечал BadRequest "Can't parse entities".
	# Внутри `code` экранируется только слэш и кавычка, поэтому UUID
	# должен остаться читаемым, без слэшей перед дефисами.
	assert '`3f2b1c8e-5a4d-4b7c-9e10-2d3f4a5b6c7d`' in message_text, message_text
	assert r'*route 77 — problem \(login\) \#1\_ok*' in message_text, message_text

	assert_valid_markdownv2(message_text)

	await factories._container.close()
	print('BOT_HANDLERS_OK')


if __name__ == '__main__':
	asyncio.run(main())
