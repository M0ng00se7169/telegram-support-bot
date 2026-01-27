"""ChatDirectory — глубокий модуль доступа к Web API (карточка №2).

Наружу — четыре сценария, DTO и один тип ошибки `WebApiError`.
Внутри — httpx, URL'ы, разбор ответов и ошибок, таймауты: ни один
потребитель не знает про статусы, `dict` и `Response`.

Транспортный seam для тестов: контейнер отдаёт `httpx.AsyncClient` с
`MockTransport`, поэтому вся логика конвертации и ошибок проверяется
без сети и без мока самого модуля.
"""

from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import httpx
from httpx import AsyncClient

from dtos.messages import (
    ChatListenerDTO,
    ChatListItemDTO,
)
from exceptions.web import WebApiError

CHAT_LIST_URI = '/chat/'
CHAT_LISTENERS_URI = '/chat/{chat_oid}/listeners/'
CHAT_INFO_URI = '/chat/{chat_oid}/'
DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 10

# Без явного таймаута httpx ждёт ответ вечно и подвешивает и бота, и consumer.
REQUEST_TIMEOUT = 10.0


class BaseChatDirectory(ABC):
    @abstractmethod
    async def get_all_chats(self) -> list[ChatListItemDTO]: ...

    @abstractmethod
    async def get_chat_listeners(self, chat_oid: str) -> list[ChatListenerDTO]: ...

    @abstractmethod
    async def add_listener(self, telegram_chat_id: int, chat_oid: str) -> None: ...

    @abstractmethod
    async def get_chat_info(self, chat_oid: str) -> ChatListItemDTO: ...


@dataclass
class ChatDirectory(BaseChatDirectory):
    http_client: AsyncClient
    base_url: str

    async def get_all_chats(self) -> list[ChatListItemDTO]:
        payload = await self._request(
            'GET',
            CHAT_LIST_URI,
            params={'limit': DEFAULT_LIMIT, 'offset': DEFAULT_OFFSET},
        )
        return [
            ChatListItemDTO.model_validate(chat_data)
            for chat_data in (payload or {}).get('items', [])
        ]

    async def get_chat_listeners(self, chat_oid: str) -> list[ChatListenerDTO]:
        payload = await self._request(
            'GET',
            CHAT_LISTENERS_URI.format(chat_oid=chat_oid),
        )
        return [
            ChatListenerDTO.model_validate(listener_data)
            for listener_data in (payload or [])
        ]

    async def add_listener(self, telegram_chat_id: int, chat_oid: str) -> None:
        await self._request(
            'POST',
            CHAT_LISTENERS_URI.format(chat_oid=chat_oid),
            json={'telegram_chat_id': str(telegram_chat_id)},
        )

    async def get_chat_info(self, chat_oid: str) -> ChatListItemDTO:
        payload = await self._request(
            'GET',
            CHAT_INFO_URI.format(chat_oid=chat_oid),
        )
        return ChatListItemDTO.model_validate(payload)

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        url = urljoin(base=self.base_url, url=path)
        try:
            response = await self.http_client.request(
                method=method,
                url=url,
                timeout=REQUEST_TIMEOUT,
                **kwargs,
            )
        except httpx.HTTPError as error:
            raise WebApiError(
                method=method,
                endpoint=path,
                status_code=0,
                detail=str(error),
            ) from error

        if not response.is_success:
            raise WebApiError(
                method=method,
                endpoint=path,
                status_code=response.status_code,
                detail=_extract_error_detail(response),
            )

        if not response.content:
            return None
        return response.json()


def _extract_error_detail(response: httpx.Response) -> str:
    """FastAPI отдаёт `detail` и строкой, и объектом — понимаем оба формата."""
    try:
        payload = response.json()
    except ValueError:
        return response.text[:500]

    detail = payload.get('detail', '') if isinstance(payload, dict) else payload
    if isinstance(detail, dict):
        return str(detail.get('error', detail))
    return str(detail)
