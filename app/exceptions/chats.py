import json
from dataclasses import dataclass

from exceptions.base import ApplicationException


@dataclass(frozen=True, eq=False)
class BaseWebException(ApplicationException):
    status_code: int
    response_content: str

    @property
    def response_json(self) -> dict:
        return json.loads(self.response_content)

    @property
    def error_text(self) -> str:
        return self.response_json.get('detail', {}).get('error', '')


@dataclass(frozen=True, eq=False)
class ChatListRequestException(BaseWebException):
    @property
    def message(self):
        return 'Failed to retrieve list of all chats'


@dataclass(frozen=True, eq=False)
class ListenerListRequestException(BaseWebException):

    @property
    def message(self):
        return "Failed to retrieve all listeners from this chat."


@dataclass(frozen=True, eq=False)
class ListenerAddRequestException(BaseWebException):
    @property
    def message(self):
        return 'Failed to add listener to the chat.'


@dataclass(frozen=True, eq=False)
class ChatInfoRequestException(BaseWebException):
    @property
    def message(self):
        return 'Failed to get information about chat.'
