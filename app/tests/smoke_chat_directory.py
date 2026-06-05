"""Offline smoke: ChatDirectory — глубокий модуль Web API (карточка №2).

Вся логика проверяется через транспортный seam (httpx.MockTransport):
- конвертация ответов в DTO (список чатов, чат, слушатели);
- единая ошибка WebApiError с method/endpoint/status/detail для обоих
  форматов `detail` FastAPI (строка и объект) и для не-JSON тела;
- сетевая ошибка (server закрыт) -> WebApiError(status_code=0);
- таймаут пробрасывается в каждый запрос (раньше httpx ждал вечно).

Run:  python tests/smoke_chat_directory.py   (from the app/ directory)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio  # noqa: E402
import contextlib  # noqa: E402

import httpx  # noqa: E402

from exceptions.web import WebApiError  # noqa: E402
from services.web import (  # noqa: E402
    REQUEST_TIMEOUT,
    BaseChatDirectory,
    ChatDirectory,
)

CHAT = {
    'oid': 'chat-oid-1',
    'title': 'route 77',
    'created_at': '2026-01-01T00:00:00Z',
}
PATHS_SEEN: list[str] = []


def _handler(request: httpx.Request) -> httpx.Response:
    PATHS_SEEN.append(f'{request.method} {request.url.path}')
    path = request.url.path

    if path == '/chat/':
        return httpx.Response(200, json={'items': [CHAT], 'count': 1})
    if path.endswith('/listeners/'):
        return httpx.Response(200, json=[{'oid': '42'}])
    if path.endswith('/string-detail/'):
        return httpx.Response(404, json={'detail': 'Chat not found'})
    if path.endswith('/object-detail/'):
        return httpx.Response(500, json={'detail': {'error': 'db down'}})
    if path.endswith('/html-detail/'):
        return httpx.Response(502, text='<html>bad gateway</html>')
    return httpx.Response(200, json=CHAT)


async def main() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))
    directory: BaseChatDirectory = ChatDirectory(
        http_client=client,
        base_url='http://main-app:8000',
    )

    # --- Happy path: DTO вместо dict/Response. ---
    chats = await directory.get_all_chats()
    assert len(chats) == 1, chats
    assert chats[0].oid == 'chat-oid-1'
    assert chats[0].title == 'route 77'
    assert chats[0].created_at.isoformat().startswith('2026-01-01')

    listeners = await directory.get_chat_listeners(chat_oid='chat-oid-1')
    assert [listener.oid for listener in listeners] == ['42']

    chat_info = await directory.get_chat_info(chat_oid='chat-oid-1')
    assert chat_info.oid == 'chat-oid-1'

    await directory.add_listener(telegram_chat_id=12345, chat_oid='chat-oid-1')
    assert 'POST /chat/chat-oid-1/listeners/' in PATHS_SEEN

    # --- Таймаут пробрасывается в каждый запрос. ---
    timeout_seen: list[float] = []

    def _timeout_handler(request: httpx.Request) -> httpx.Response:
        timeout_seen.append(request.extensions['timeout']['read'])
        return httpx.Response(200, json=CHAT)

    timed_client = httpx.AsyncClient(transport=httpx.MockTransport(_timeout_handler))
    timed_directory = ChatDirectory(http_client=timed_client, base_url='http://x')
    await timed_directory.get_chat_info(chat_oid='chat-oid-1')
    assert timeout_seen == [REQUEST_TIMEOUT], timeout_seen

    # --- Ошибки: один тип наружу, детали разобраны внутри. ---
    async def _expect_error(chat_oid: str, status: int, detail: str) -> None:
        endpoint = f'/chat/{chat_oid}/'
        try:
            await directory.get_chat_info(chat_oid=chat_oid)
        except WebApiError as error:
            assert error.status_code == status, error
            assert error.endpoint == endpoint, error.endpoint
            assert error.detail == detail, error.detail
            assert error.message, 'message must be human readable'
        else:
            raise AssertionError(f'{endpoint} must raise WebApiError')

    await _expect_error('string-detail', 404, 'Chat not found')
    await _expect_error('object-detail', 500, 'db down')
    await _expect_error('html-detail', 502, '<html>bad gateway</html>')

    # --- Обрыв соединения -> та же ошибка, status_code = 0. ---
    def _refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('connection refused', request=request)

    refused = ChatDirectory(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(_refuse)),
        base_url='http://main-app:8000',
    )
    try:
        await refused.get_chat_info(chat_oid='any')
    except WebApiError as error:
        assert error.status_code == 0, error.status_code
        assert 'unreachable' in error.message, error.message
    else:
        raise AssertionError('transport failure must raise WebApiError')

    # --- Исключение обязано пережить проброс через contextlib-менеджер
    # (ADR-0004): именно там падал FrozenInstanceError вместо реальной ошибки. ---
    @contextlib.contextmanager
    def _scope():
        yield

    try:
        with _scope():
            raise WebApiError(
                method='GET',
                endpoint='/chat/x/',
                status_code=404,
                detail='Chat not found',
            )
    except WebApiError as error:
        assert error.status_code == 404, error
    except Exception as error:
        raise AssertionError(
            f'exception type must survive contextlib, got {type(error).__name__}',
        ) from error
    else:
        raise AssertionError('WebApiError must be raised')

    await client.aclose()
    print('CHAT_DIRECTORY_SMOKE_OK')


if __name__ == '__main__':
    asyncio.run(main())
