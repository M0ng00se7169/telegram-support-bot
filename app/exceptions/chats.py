from dataclasses import dataclass

from exceptions.base import ApplicationException


@dataclass(frozen=True, eq=False)
class ChatListRequestException(ApplicationException):
    status_code: int
    response_content: str

    @property
    def message(self):
        return "Failed to retrieve a list of all chats."


@dataclass(frozen=True, eq=False)
class ListenerListRequestError(ApplicationException):
    status_code: int
    response_content: str

    @property
    def message(self):
        return "Failed to retrieve all listeners from this chat."
