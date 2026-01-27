"""Единственная ошибка на границе Web API (карточка №2).

Вместо зоопарка классов на каждый эндпоинт — один тип с контекстом:
что просили (method + endpoint), что пришло (status_code + detail).
Модуль сам разбирает ответ; наружу уходит только эта ошибка.
"""

from dataclasses import dataclass

from exceptions.base import ApplicationException


@dataclass(eq=False)
class WebApiError(ApplicationException):
    method: str
    endpoint: str
    status_code: int  # 0 — сетевая ошибка: ответа не было вовсе
    detail: str

    @property
    def message(self) -> str:
        if not self.status_code:
            return f'Web API {self.method} {self.endpoint} is unreachable'
        return f'Web API {self.method} {self.endpoint} failed with {self.status_code}'
